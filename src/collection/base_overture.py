import time

from pyspark.sql.functions import col, expr, to_json
from pyspark.sql.types import TimestampType
from sedona.spark import SedonaContext

from src.collection.overture_collection_base import OvertureCollection
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import get_region_bbox_coords, print_error, print_info, timing, create_table_dump, restore_table_dump

class OvertureBaseCollection(OvertureCollection):
    def __init__(self, db, db_rd, region, collection_type):
        super().__init__(db, db_rd, region, collection_type)
        self.output_schema = "public"

    def initialize_data_source(self, sedona: SedonaContext):
        """Initialize Overture geoparquet file source and data frames for admins data."""

        # Load Overture geoparquet data into Spark DataFrames
        self.land_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=land"
        )
        self.land_use_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=land_use"
        )
        self.water_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=water"
        )
        print_info("Initialized data source.")

    def initialize_tables(self):
        """Initialize PostgreSQL tables for base data."""

        sql_create_table_land = f"""
            DROP TABLE IF EXISTS {self.output_schema}.land_{self.region};
            CREATE TABLE {self.output_schema}.land_{self.region}(
                id TEXT PRIMARY KEY,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                subtype TEXT,
                wikidata TEXT,
                names TEXT,
                class TEXT,
                elevation INTEGER,
                source_tags TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_land)
        print_info(f"Created land_{self.region}")

        sql_create_table_land_use = f"""
            DROP TABLE IF EXISTS {self.output_schema}.land_use_{self.region};
            CREATE TABLE {self.output_schema}.land_use_{self.region}(
                id TEXT PRIMARY KEY,
                subtype TEXT,
                class TEXT,
                surface TEXT,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                wikidata TEXT,
                names TEXT,
                source_tags TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_land_use)
        print_info(f"Created land_use_{self.region}")

        sql_create_table_water = f"""
            DROP TABLE IF EXISTS {self.output_schema}.water_{self.region};
            CREATE TABLE {self.output_schema}.water_{self.region}(
                id TEXT PRIMARY KEY,
                subtype TEXT,
                class TEXT,
                is_salt BOOLEAN,
                is_intermittent BOOLEAN,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                wikidata TEXT,
                names TEXT,
                source_tags TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_water)
        print_info(f"Created water_{self.region}")

    def filter_region_land(self, bbox_coords: dict):
        # select necessary columns
        land = self.land_df.selectExpr(
            "id",
            "version",
            "update_time",
            "sources",
            "subtype",
            "wikidata",
            "names",
            "class",
            "elevation",
            "source_tags",
            "geometry",
            "bbox"
        )

        land = land.filter(
            (land.bbox.minx > bbox_coords["xmin"]) &
            (land.bbox.miny > bbox_coords["ymin"]) &
            (land.bbox.maxx < bbox_coords["xmax"]) &
            (land.bbox.maxy < bbox_coords["ymax"])
        )
        land = land.drop("bbox")

        # Convert the complex types to JSON strings
        complex_columns = ["update_time", "sources", "names", "source_tags"]

        for column in complex_columns:
            if column == "update_time":
                land = land.withColumn(column, col(column).cast(TimestampType()))
            else:
                land = land.withColumn(column, to_json(column))

        land = land.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        # Remove duplicate rows based on the id column
        land = land.dropDuplicates(["id"])


        return land

    def filter_region_land_use(self, bbox_coords: dict):
        # select necessary columns
        land_use = self.land_use_df.selectExpr(
            "id",
            "subtype",
            "class",
            "surface",
            "version",
            "update_time",
            "sources",
            "wikidata",
            "names",
            "source_tags",
            "geometry",
            "bbox"
        )

        land_use = land_use.filter(
            (land_use.bbox.minx > bbox_coords["xmin"]) &
            (land_use.bbox.miny > bbox_coords["ymin"]) &
            (land_use.bbox.maxx < bbox_coords["xmax"]) &
            (land_use.bbox.maxy < bbox_coords["ymax"])
        )
        land_use = land_use.drop("bbox")

        # Convert the complex types to JSON strings
        complex_columns = ["update_time", "sources", "names", "source_tags"]

        for column in complex_columns:
            if column == "update_time":
                land_use = land_use.withColumn(column, col(column).cast(TimestampType()))
            else:
                land_use = land_use.withColumn(column, to_json(column))

        # Convert binary geometry to Geometry type and then to text
        land_use = land_use.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        return land_use

    def filter_region_water(self, bbox_coords: dict):
        # select necessary columns
        water = self.water_df.selectExpr(
            "id",
            "subtype",
            "class",
            "is_salt",
            "is_intermittent",
            "version",
            "update_time",
            "sources",
            "wikidata",
            "names",
            "source_tags",
            "geometry",
            "bbox"
        )

        water = water.filter(
            (water.bbox.minx > bbox_coords["xmin"]) &
            (water.bbox.miny > bbox_coords["ymin"]) &
            (water.bbox.maxx < bbox_coords["xmax"]) &
            (water.bbox.maxy < bbox_coords["ymax"])
        )
        water = water.drop("bbox")

        # Convert the complex types to JSON strings
        complex_columns = ["update_time", "sources", "names", "source_tags"]

        for column in complex_columns:
            if column == "update_time":
                water = water.withColumn(column, col(column).cast(TimestampType()))
            else:
                water = water.withColumn(column, to_json(column))

        # Convert binary geometry to Geometry type and then to text
        water = water.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        # Remove duplicate rows based on the id column
        water = water.dropDuplicates(["id"])

        return water

    def alter_tables(self):

        sql_alter_table_land = f"""
            ALTER TABLE {self.output_schema}.land_{self.region}
            ALTER COLUMN geometry TYPE geometry;
            CREATE INDEX ON {self.output_schema}.land_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_land)
        print_info(f"Altered table: land_{self.region}")

        sql_alter_table_land_use = f"""
            ALTER TABLE {self.output_schema}.land_use_{self.region}
            ALTER COLUMN geometry TYPE geometry;
            CREATE INDEX ON {self.output_schema}.land_use_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_land_use)
        print_info(f"Altered table: land_use_{self.region}")

        sql_alter_table_water = f"""
            ALTER TABLE {self.output_schema}.water_{self.region}
            ALTER COLUMN geometry TYPE geometry;
            CREATE INDEX ON {self.output_schema}.water_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_water)
        print_info(f"Altered table: water_{self.region}")

    def run(self):
        sedona = self.initialize_sedona_context()
        self.initialize_jdbc_properties()
        self.initialize_data_source(sedona)
        self.initialize_tables()

        bbox_coords = get_region_bbox_coords(
        geom_query=self.data_config_collection["region"],
        db=self.db_rd
        )

        region_land = self.filter_region_land(bbox_coords)
        region_land_use = self.filter_region_land_use(bbox_coords)
        region_water = self.filter_region_water(bbox_coords)

        self.fetch_data(
            data_frame=region_land,
            output_schema=self.output_schema,
            output_table=f"land_{self.region}"
        )

        self.fetch_data(
            data_frame=region_land_use,
            output_schema=self.output_schema,
            output_table=f"land_use_{self.region}"
        )

        self.fetch_data(
            data_frame=region_water,
            output_schema=self.output_schema,
            output_table=f"water_{self.region}"
        )

        self.alter_tables()

@timing
def collect_base_overture(region:str):
    print_info(f"Collect Overture base data for region: {region}.")
    db = Database(settings.LOCAL_DATABASE_URI)
    db_rd = Database(settings.RAW_DATABASE_URI)

    try:
        OvertureBaseCollection(
            db=db,
            db_rd=db_rd,
            region=region,
            collection_type="base_overture"
        ).run()
    except Exception as e:
        print_error(f"Failed to collect Overture base data for region: {region}.")
        print_error(e)
        raise e
    finally:
        db.conn.close()
        db_rd.conn.close()
        print_info(f"Finished collecting Overture base data for region: {region}.")
