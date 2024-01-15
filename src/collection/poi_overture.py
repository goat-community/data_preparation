from pyspark.sql.functions import col, expr, to_json
from pyspark.sql.types import TimestampType
from sedona.spark import SedonaContext

from src.collection.overture_collection_base import OvertureBaseCollection
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import get_region_bbox_coords, print_error, print_info, timing


class OverturePOICollection(OvertureBaseCollection):
    def __init__(self, db_local, db_remote, region, collection_type):
            super().__init__(db_local, db_remote, region, collection_type)

    def initialize_data_source(self, sedona: SedonaContext):
        """Initialize Overture parquet file source and data frames for places data."""

        # Load Overture parquet data into Spark DataFrames
        self.places_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=*/*"
        )

    def initialize_tables(self):
        """Create table in PostgreSQL database for places."""

        #TODO: check fix data types
        sql_create_table_places = f"""
            DROP TABLE IF EXISTS temporal.places_{self.region}_raw;
            CREATE TABLE temporal.places_{self.region}_raw (
                id TEXT PRIMARY KEY,
                categories TEXT,
                updatetime TIMESTAMPTZ,
                version INT,
                names TEXT,
                confidence DOUBLE PRECISION,
                websites TEXT,
                socials TEXT,
                emails TEXT,
                phones TEXT,
                brand TEXT,
                addresses TEXT,
                sources TEXT,
                geometry TEXT,
                bbox TEXT
            );
        """
        self.db_local.perform(sql_create_table_places)
        print_info(f"Created table: temporal.places_{self.region}_raw.")

    def filter_region_places(self, bbox_coords: dict):
        """Initialize the places dataframe and apply relevant filters."""

        # Select the necessary columns
        places = self.places_df.selectExpr(
            "id",
            "updatetime",
            "version",
            "names",
            "categories",
            "confidence",
            "websites",
            "socials",
            "emails",
            "phones",
            "brand",
            "addresses",
            "sources",
            "geometry",
            "bbox"
        )

        # Create a polygon WKT from the bounding box coordinates
        polygon_wkt = f"POLYGON(({bbox_coords['xmin']} {bbox_coords['ymin']}, {bbox_coords['xmax']} {bbox_coords['ymin']}, {bbox_coords['xmax']} {bbox_coords['ymax']}, {bbox_coords['xmin']} {bbox_coords['ymax']}, {bbox_coords['xmin']} {bbox_coords['ymin']}))"

        # Filter the DataFrame to only include rows where the geometry is within the polygon
        places = self.places_df.filter(
            f"ST_Contains(ST_GeomFromWKT('{polygon_wkt}'), geometry) = true"
        )

        # Convert the complex types to JSON strings
        complex_columns = [
            "updatetime",
            "names",
            "categories",
            "brand",
            "addresses",
            "sources",
            "websites", 
            "socials", 
            "emails", 
            "phones",
            "bbox"
        ]

        for column in complex_columns:
            if column == "updatetime":
                places = places.withColumn(column, col(column).cast(TimestampType()))
            else:
                places = places.withColumn(column, to_json(column))
                
        places = places.withColumn("geometry", expr("ST_AsText(geometry)"))             

        return places

    @timing
    def alter_tables(self):
        """Alter table in PostgreSQL database for places.""" 
        
        # get the geometires of the study area based on the query defined in the config
        region_geoms = self.db_local.select(self.data_config_collection['region'])

        # create table for the Overture places
        drop_create_table_sql = f"""
            DROP TABLE IF EXISTS temporal.places_{self.region};
            CREATE TABLE temporal.places_{self.region} AS (
                SELECT *
                FROM temporal.places_{self.region}_raw
                WHERE 1=0
            );
        """
        self.db_local.perform(drop_create_table_sql)        
        
        # convert geom column to geometry type
        alter_geom_column_sql = f"""
            ALTER TABLE temporal.places_{self.region}
            ALTER COLUMN geometry TYPE GEOMETRY(POINT, 4326) USING ST_GeomFromText(geometry, 4326);
            CREATE INDEX ON temporal.places_{self.region} USING GIST (geometry);
        """
        self.db_local.perform(alter_geom_column_sql)
        
        # clip data to study area
        # TODO: do i create duplicates?
        for geom in region_geoms:
            clip_poi_overture = f"""
                INSERT INTO temporal.places_{self.region}
                WITH region AS (
                    SELECT ST_SetSRID(ST_GeomFromText(ST_AsText('{geom[0]}')), 4326) AS geom
                )
                SELECT p.*
                FROM temporal.places_{self.region}_raw p, region r
                WHERE ST_Intersects(ST_SetSRID(p.geometry, 4326), r.geom)
                AND ST_SetSRID(p.geometry, 4326) && r.geom;
                ;
            """
            self.db_local.perform(clip_poi_overture)

        # Adjust names column
        sql_adjust_names_column = f"""
            UPDATE temporal.places_{self.region}
            SET names = TRIM(BOTH '"' FROM (names::jsonb->'common'->0->'value')::text);
        """
        self.db_local.perform(sql_adjust_names_column)

        # Adjust categories column
        sql_adjust_categories_column = f"""
            ALTER TABLE temporal.places_{self.region}
            ADD COLUMN other_categories varchar[];

            UPDATE temporal.places_{self.region}
            SET other_categories = (
                CASE
                    WHEN (categories::jsonb->'alternate'->>0) IS NOT NULL OR (categories::jsonb->'alternate'->>1) IS NOT NULL THEN
                        ARRAY_REMOVE(ARRAY_REMOVE(ARRAY[(categories::jsonb->'alternate'->>0)::varchar, (categories::jsonb->'alternate'->>1)::varchar], NULL), '')
                    ELSE
                        ARRAY[]::varchar[]
                END
            );

            UPDATE temporal.places_{self.region}
            SET categories = TRIM(BOTH '"' FROM (categories::jsonb->>'main'));
        """
        self.db_local.perform(sql_adjust_categories_column)

        # Adjust addresses column
        sql_adjust_addresses_column = f"""
            ALTER TABLE temporal.places_{self.region}
            ADD COLUMN street varchar,
            ADD COLUMN housenumber varchar,
            ADD COLUMN zipcode varchar;

            UPDATE temporal.places_{self.region}
            SET
                street = TRIM(substring((addresses::jsonb->0->>'freeform')::varchar from '^(.*)(?=\s\d)')),
                housenumber = TRIM(substring((addresses::jsonb->0->>'freeform')::varchar from '(\s\d.*)$')),
                zipcode = (addresses::jsonb->0->>'postcode')::varchar,
                brand = brand::jsonb->'names'->'common'->0->>'value';
        """
        self.db_local.perform(sql_adjust_addresses_column)
  

    def run(self):
        """Run Overture places collection."""

        sedona = self.initialize_sedona_context()
        self.initialize_jdbc_properties()
        self.initialize_data_source(sedona)
        self.initialize_tables()

        bbox_coords = get_region_bbox_coords(
            geom_query=self.data_config_collection["region"],
            db=self.db_local
        )
        region_places = self.filter_region_places(bbox_coords)

        self.fetch_data(
            data_frame=region_places,
            output_schema="temporal",
            output_table=f"places_{self.region}_raw"
        )

        self.alter_tables()

        print_info(f"Finished Overture places collection for: {self.region}.")

def collect_poi_overture(region: str):
    print_info(f"Collect Overture places data for region: {region}.")
    db_local = Database(settings.LOCAL_DATABASE_URI)
    db_remote = Database(settings.RAW_DATABASE_URI)

    try:
        OverturePOICollection(
            db_local=db_local,
            db_remote=db_remote,
            region=region,
            collection_type="poi_overture"
        ).run()
        db_local.close()
        db_remote.close()
        print_info("Finished Overture places collection.")
    except Exception as e:
        print_error(e)
        raise e
    finally:
        db_local.close()
        db_remote.close()
