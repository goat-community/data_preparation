import time

from pyspark.sql.functions import col, expr, to_json
from pyspark.sql.types import TimestampType
from sedona.spark import SedonaContext

from src.collection.overture_collection_base import OvertureCollection
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import get_region_bbox_coords, print_error, print_info, timing, create_table_dump, restore_table_dump

class OvertureBuildingCollection(OvertureCollection):

    def __init__(self, db, db_rd, region, collection_type):
        super().__init__(db, db_rd, region, collection_type)
        self.output_schema = "public"

    def initialize_data_source(self, sedona: SedonaContext):
        """Initialize Overture geoparquet file source and data frames for building data."""

        # Load Overture geoparquet data into Spark DataFrames
        self.building_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=building"
        )
        self.part_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=part"
        )
        print_info("Initialized data source.")

    def initialize_tables(self):
        """Initialize PostgreSQL tables for building data."""

        sql_create_table_building = f"""
            DROP TABLE IF EXISTS {self.output_schema}.building_overture_{self.region};
            CREATE TABLE {self.output_schema}.building_overture_{self.region}(
                id TEXT PRIMARY KEY,
                class TEXT,
                level INTEGER,
                has_parts BOOLEAN,
                height FLOAT,
                num_floors INTEGER,
                facade_color TEXT,
                facade_material TEXT,
                roof_material TEXT,
                roof_shape TEXT,
                roof_direction FLOAT,
                roof_orientation TEXT,
                roof_color TEXT,
                eave_height FLOAT,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                names TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_building)
        print_info(f"Created building_overture_{self.region}")

        sql_create_table_part = f"""
            DROP TABLE IF EXISTS {self.output_schema}.part_{self.region};
            CREATE TABLE {self.output_schema}.part_{self.region}(
                id TEXT PRIMARY KEY,
                level INTEGER,
                height FLOAT,
                num_floors INTEGER,
                min_height FLOAT,
                facade_color TEXT,
                facade_material TEXT,
                roof_material TEXT,
                roof_shape TEXT,
                roof_direction FLOAT,
                roof_orientation TEXT,
                roof_color TEXT,
                building_id TEXT,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                names TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_part)
        print_info(f"Created part_{self.region}")

    def filter_region_buildings(self, bbox_coords: dict):
        # select necessary columns
        building = self.building_df.selectExpr(
            "id",
            "class",
            "level",
            "has_parts",
            "height",
            "num_floors",
            "facade_color",
            "facade_material",
            "roof_material",
            "roof_shape",
            "roof_direction",
            "roof_orientation",
            "roof_color",
            "eave_height",
            "version",
            "update_time",
            "sources",
            "names",
            "geometry",
            "bbox"
        )

        building = building.filter(
            (building.bbox.minx > bbox_coords["xmin"]) &
            (building.bbox.miny > bbox_coords["ymin"]) &
            (building.bbox.maxx < bbox_coords["xmax"]) &
            (building.bbox.maxy < bbox_coords["ymax"])
        )
        building = building.drop("bbox")

        # Convert the complex types to JSON strings
        complex_columns = ["update_time", "sources", "names"]

        for column in complex_columns:
            if column == "update_time":
                building = building.withColumn(column, col(column).cast(TimestampType()))
            else:
                building = building.withColumn(column, to_json(column))

        # Convert binary geometry to Geometry type and then to text
        building = building.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        return building

    def filter_region_parts(self, bbox_coords: dict):
        # select necessary columns
        part = self.part_df.selectExpr(
            "id",
            "level",
            "height",
            "num_floors",
            "min_height",
            "facade_color",
            "facade_material",
            "roof_material",
            "roof_shape",
            "roof_direction",
            "roof_orientation",
            "roof_color",
            "building_id",
            "version",
            "update_time",
            "sources",
            "names",
            "geometry",
            "bbox"
        )

        # filter by bounding box
        part = part.filter(
            (part.bbox.minx > bbox_coords["xmin"]) &
            (part.bbox.miny > bbox_coords["ymin"]) &
            (part.bbox.maxx < bbox_coords["xmax"]) &
            (part.bbox.maxy < bbox_coords["ymax"])
        )
        part = part.drop("bbox")

        # Convert the complex types to JSON strings
        complex_columns = ["update_time", "sources", "names"]

        for column in complex_columns:
            if column == "update_time":
                part = part.withColumn(column, col(column).cast(TimestampType()))
            else:
                part = part.withColumn(column, to_json(column))

        # Convert binary geometry to Geometry type and then to text
        part = part.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        return part

    def alter_tables(self):

        sql_alter_table_building = f"""
            UPDATE {self.output_schema}.building_overture_{self.region}
            SET geometry = ST_Multi(geometry);

            ALTER TABLE {self.output_schema}.building_overture_{self.region}
            ALTER COLUMN geometry SET DATA TYPE GEOMETRY(MULTIPOLYGON, 4326);

            CREATE INDEX ON {self.output_schema}.building_overture_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_building)
        print_info(f"Altered table: building_overture_{self.region}")

        sql_alter_table_part = f"""
            UPDATE {self.output_schema}.part_{self.region}
            SET geometry = ST_Multi(geometry);

            ALTER TABLE {self.output_schema}.part_{self.region}
            ALTER COLUMN geometry SET DATA TYPE GEOMETRY(MULTIPOLYGON, 4326);

            CREATE INDEX ON {self.output_schema}.part_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_part)
        print_info(f"Altered table: part_{self.region}")

    def run(self):
        sedona = self.initialize_sedona_context()
        self.initialize_jdbc_properties()
        self.initialize_data_source(sedona)
        self.initialize_tables()

        bbox_coords = get_region_bbox_coords(
            geom_query=self.data_config_collection["region"],
            db=self.db_rd
        )

        region_building = self.filter_region_buildings(bbox_coords)
        region_parts = self.filter_region_parts(bbox_coords)

        self.fetch_data(
            data_frame=region_building,
            output_schema=self.output_schema,
            output_table=f"building_overture_{self.region}"
        )

        self.fetch_data(
            data_frame=region_parts,
            output_schema=self.output_schema,
            output_table=f"part_{self.region}"
        )

        self.alter_tables()

@timing
def collect_building_overture(region: str):
    print_info(f"Collect Overture building data for region: {region}.")
    db = Database(settings.LOCAL_DATABASE_URI)
    db_rd = Database(settings.RAW_DATABASE_URI)

    try:
        OvertureBuildingCollection(
            db=db,
            db_rd=db_rd,
            region=region,
            collection_type="building_overture"
        ).run()
    except Exception as e:
        print_error(f"Failed to collect Overture building data for region: {region}.")
        print_error(e)
        raise e
    finally:
        db.close()
        db_rd.close()
        print_info(f"Finished collecting Overture building data for region: {region}.")
