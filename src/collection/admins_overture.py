import time

from pyspark.sql.functions import col, expr, to_json
from pyspark.sql.types import TimestampType
from sedona.spark import SedonaContext

from src.collection.overture_collection_base import OvertureCollection
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import get_region_bbox_coords, print_error, print_info, timing, create_table_dump, restore_table_dump

class OvertureAdminsCollection(OvertureCollection):

    def __init__(self, db, db_rd, region, collection_type):
        super().__init__(db, db_rd, region, collection_type)
        self.output_schema = "public"

    def initialize_data_source(self, sedona: SedonaContext):
        """Initialize Overture geoparquet file source and data frames for admins data."""

        # Load Overture geoparquet data into Spark DataFrames
        self.administrative_boundary_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=administrative_boundary"
        )
        self.locality_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=locality"
        )
        self.locality_area_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=locality_area"
        )
        print_info("Initialized data source.")

    def initialize_tables(self):
        """Initialize PostgreSQL tables for admins data."""

        sql_create_table_administrative_boundary = f"""
            DROP TABLE IF EXISTS {self.output_schema}.administrative_boundary_{self.region};
            CREATE TABLE {self.output_schema}.administrative_boundary_{self.region}(
                id TEXT PRIMARY KEY,
                admin_level INTEGER,
                is_maritime BOOLEAN,
                geopol_display TEXT,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                subtype TEXT,
                locality_type TEXT,
                wikidata TEXT,
                context_id TEXT,
                population INTEGER,
                iso_country_code_alpha_2 TEXT,
                iso_sub_country_code TEXT,
                default_language TEXT,
                driving_side TEXT,
                names TEXT,
                locality_id TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_administrative_boundary)
        print_info(f"Created administrative_boundary_{self.region}")

        sql_create_table_locality = f"""
            DROP TABLE IF EXISTS {self.output_schema}.locality_{self.region};
            CREATE TABLE {self.output_schema}.locality_{self.region}(
                id TEXT PRIMARY KEY,
                admin_level INTEGER,
                is_maritime BOOLEAN,
                geopol_display TEXT,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                subtype TEXT,
                locality_type TEXT,
                wikidata TEXT,
                context_id TEXT,
                population INTEGER,
                iso_country_code_alpha_2 TEXT,
                iso_sub_country_code TEXT,
                default_language TEXT,
                driving_side TEXT,
                names TEXT,
                locality_id TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_locality)
        print_info(f"Created locality_{self.region}")

        sql_create_table_locality_area = f"""
            DROP TABLE IF EXISTS {self.output_schema}.locality_area_{self.region};
            CREATE TABLE {self.output_schema}.locality_area_{self.region}(
                id TEXT PRIMARY KEY,
                admin_level INTEGER,
                is_maritime BOOLEAN,
                geopol_display TEXT,
                version INTEGER,
                update_time TIMESTAMPTZ,
                sources TEXT,
                subtype TEXT,
                locality_type TEXT,
                wikidata TEXT,
                context_id TEXT,
                population INTEGER,
                iso_country_code_alpha_2 TEXT,
                iso_sub_country_code TEXT,
                default_language TEXT,
                driving_side TEXT,
                names TEXT,
                locality_id TEXT,
                geometry TEXT
            );
        """
        self.db.perform(sql_create_table_locality_area)
        print_info(f"Created locality_area_{self.region}")

    def filter_region_administrative_boundary(self, bbox_cords: dict):

        # select necessary columns (check self.administrative_boundary_df.printSchema())
        administrative_boundary = self.administrative_boundary_df.selectExpr(
            "id",
            "admin_level",
            "is_maritime",
            "geopol_display",
            "version",
            "update_time",
            "sources",
            "subtype",
            "locality_type",
            "wikidata",
            "context_id",
            "population",
            "iso_country_code_alpha_2",
            "iso_sub_country_code",
            "default_language",
            "driving_side",
            "names",
            "locality_id",
            "geometry",
            "bbox"
        )

        administrative_boundary = self.administrative_boundary_df.filter(
            (administrative_boundary.bbox.minx > bbox_cords["xmin"]) &
            (administrative_boundary.bbox.miny > bbox_cords["ymin"]) &
            (administrative_boundary.bbox.maxx < bbox_cords["xmax"]) &
            (administrative_boundary.bbox.maxy < bbox_cords["ymax"])
        )
        administrative_boundary = administrative_boundary.drop(administrative_boundary.bbox)

        # convert complex columns to json
        complex_columns = ["update_time", "sources", "names"]

        for column in complex_columns:
            if column == "update_time":
                administrative_boundary = administrative_boundary.withColumn(column, col(column).cast(TimestampType()))
            else:
                administrative_boundary = administrative_boundary.withColumn(column, to_json(column))

        administrative_boundary = administrative_boundary.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        return administrative_boundary

    def filter_region_locality(self, bbox_cords: dict):

        # select necessary columns
        locality = self.locality_df.selectExpr(
            "id",
            "admin_level",
            "is_maritime",
            "geopol_display",
            "version",
            "update_time",
            "sources",
            "subtype",
            "locality_type",
            "wikidata",
            "context_id",
            "population",
            "iso_country_code_alpha_2",
            "iso_sub_country_code",
            "default_language",
            "driving_side",
            "names",
            "locality_id",
            "geometry",
            "bbox"
        )

        locality = self.locality_df.filter(
            (locality.bbox.minx > bbox_cords["xmin"]) &
            (locality.bbox.miny > bbox_cords["ymin"]) &
            (locality.bbox.maxx < bbox_cords["xmax"]) &
            (locality.bbox.maxy < bbox_cords["ymax"])
        )
        locality = locality.drop(locality.bbox)

        complex_columns = ["update_time", "sources", "names"]

        for column in complex_columns:
            if column == "update_time":
                locality = locality.withColumn(column, col(column).cast(TimestampType()))
            else:
                locality = locality.withColumn(column, to_json(column))

        locality = locality.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        return locality

    def filter_region_locality_area(self, bbox_cords: dict):

        # select necessary columns
        locality_area = self.locality_area_df.selectExpr(
            "id",
            "admin_level",
            "is_maritime",
            "geopol_display",
            "version",
            "update_time",
            "sources",
            "subtype",
            "locality_type",
            "wikidata",
            "context_id",
            "population",
            "iso_country_code_alpha_2",
            "iso_sub_country_code",
            "default_language",
            "driving_side",
            "names",
            "locality_id",
            "geometry",
            "bbox"
        )

        locality_area = self.locality_area_df.filter(
            (locality_area.bbox.minx > bbox_cords["xmin"]) &
            (locality_area.bbox.miny > bbox_cords["ymin"]) &
            (locality_area.bbox.maxx < bbox_cords["xmax"]) &
            (locality_area.bbox.maxy < bbox_cords["ymax"])
        )
        locality_area = locality_area.drop(locality_area.bbox)

        # Convert the complex types to JSON strings
        complex_columns = ["update_time", "sources", "names"]

        for column in complex_columns:
            if column == "update_time":
                locality_area = locality_area.withColumn(column, col(column).cast(TimestampType()))
            else:
                locality_area = locality_area.withColumn(column, to_json(column))

        locality_area = locality_area.withColumn("geometry", expr("ST_AsText(ST_GeomFromWKB(geometry))"))

        return locality_area

    def alter_tables(self):

        sql_alter_table_administrative_boundary = f"""
            ALTER TABLE {self.output_schema}.administrative_boundary_{self.region}
            ALTER COLUMN geometry SET DATA TYPE GEOMETRY(LINESTRING, 4326);
            CREATE INDEX ON {self.output_schema}.administrative_boundary_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_administrative_boundary)
        print_info(f"Altered table: administrative_boundary_{self.region}")

        sql_alter_table_locality = f"""
            ALTER TABLE {self.output_schema}.locality_{self.region}
            ALTER COLUMN geometry SET DATA TYPE GEOMETRY(POINT, 4326);
            CREATE INDEX ON {self.output_schema}.locality_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_locality)
        print_info(f"Altered table: locality_{self.region}")

        sql_alter_table_locality_area = f"""
            UPDATE {self.output_schema}.locality_area_{self.region}
            SET geometry = ST_Multi(geometry);

            ALTER TABLE {self.output_schema}.locality_area_{self.region}
            ALTER COLUMN geometry SET DATA TYPE GEOMETRY(MULTIPOLYGON, 4326);

            CREATE INDEX ON {self.output_schema}.locality_area_{self.region} USING GIST (geometry);
        """
        self.db.perform(sql_alter_table_locality_area)
        print_info(f"Altered table: locality_area_{self.region}")

    def run(self):
        sedona = self.initialize_sedona_context()
        self.initialize_jdbc_properties()
        self.initialize_data_source(sedona)
        self.initialize_tables()

        bbox_coords = get_region_bbox_coords(
            geom_query=self.data_config_collection["region"],
            db=self.db_rd
        )

        region_administrative_boundary = self.filter_region_administrative_boundary(bbox_coords)
        region_locality = self.filter_region_locality(bbox_coords)
        region_locality_area = self.filter_region_locality_area(bbox_coords)

        self.fetch_data(
            data_frame=region_administrative_boundary,
            output_schema=self.output_schema,
            output_table=f"administrative_boundary_{self.region}"
        )

        self.fetch_data(
            data_frame=region_locality,
            output_schema=self.output_schema,
            output_table=f"locality_{self.region}"
        )

        self.fetch_data(
            data_frame=region_locality_area,
            output_schema=self.output_schema,
            output_table=f"locality_area_{self.region}"
        )

        self.alter_tables()

@timing
def collect_admins_overture(region: str):
    print_info(f"Collect Overture admins data for region: {region}.")
    db = Database(settings.LOCAL_DATABASE_URI)
    db_rd = Database(settings.RAW_DATABASE_URI)

    try:
        OvertureAdminsCollection(
            db=db,
            db_rd=db_rd,
            region=region,
            collection_type="admins_overture"
        ).run()
    except Exception as e:
        print_error(f"Failed to collect Overture admins data for region: {region}.")
        print_error(e)
        raise e
    finally:
        db.close()
        db_rd.close()
        print_info(f"Finished collecting Overture admins data for region: {region}.")
