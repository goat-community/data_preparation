import os
import subprocess
import time
import duckdb
from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import print_hashtags

class OverturePOICollection:
    """Collection of the places data set from the Overture Maps Foundation"""
    def __init__(self, db: Database, region: str = "de"):
        self.region = region
        self.db = db
        self.db_config = db.db_config
        self.dbname = self.db_config.path.replace("/", "")
        self.host = self.db_config.host
        self.user = self.db_config.user
        self.port = self.db_config.port
        self.password = self.db_config.password

        self.data_config = Config('poi_overture', region)
        self.data_config_collection = self.data_config.collection
        self.dataset_dir = self.data_config.dataset_dir
        self.duckdb_cursor = duckdb.connect()

    def initialize_duckdb(self):
        initialize_duckdb = """
            INSTALL spatial;
            INSTALL parquet;
            INSTALL httpfs;
            LOAD spatial;
            LOAD parquet;
            LOAD httpfs;
            SET s3_region='us-west-2';
            """ #TODO: at least parts could be imported from config?

        self.duckdb_cursor.execute(initialize_duckdb)

    def run(self):

        start_time = time.time()

        file_path_raw_data = os.path.join(self.dataset_dir, f"places_{self.region}.geojsonseq")

        # Create the directory if it doesn't exist
        if not os.path.exists(self.dataset_dir):
            os.makedirs(self.dataset_dir)

        get_bounding_box = f"""
            WITH region AS (
                {self.data_config_collection['region']}
            )
            SELECT
                MIN(ST_XMin(ST_Envelope(geom))) AS min_minx,
                MAX(ST_XMax(ST_Envelope(geom))) AS min_maxx,
                MIN(ST_YMin(ST_Envelope(geom))) AS min_miny,
                MAX(ST_YMax(ST_Envelope(geom))) AS min_maxy
            FROM region;
        """
        bounding_box = self.db.select(get_bounding_box)

        #TODO: check if download speed can be improved using https://github.com/wherobots/OvertureMaps
        download_overture_places =f"""
            LOAD httpfs;
            LOAD spatial;

            COPY (
                SELECT
                    id,
                    updatetime,
                    version,
                    CAST(names AS JSON) AS names,
                    CAST(categories AS JSON) AS categories,
                    confidence,
                    CAST(websites AS JSON) AS websites,
                    CAST(socials AS JSON) AS socials,
                    CAST(emails AS JSON) AS emails,
                    CAST(phones AS JSON) AS phones,
                    CAST(brand AS JSON) AS brand,
                    CAST(addresses AS JSON) AS addresses,
                    CAST(sources AS JSON) AS sources,
                    ST_GeomFromWKB(geometry)
                FROM
                    read_parquet('{self.data_config_collection['source']}', hive_partitioning=1)
                WHERE
                    bbox.minx > {bounding_box[0][0]}
                    AND bbox.maxx < {bounding_box[0][1]}
                    AND bbox.miny > {bounding_box[0][2]}
                    AND bbox.maxy < {bounding_box[0][3]}
            ) TO '{file_path_raw_data}'
            WITH (FORMAT GDAL, DRIVER 'GeoJSONSeq');
        """

        self.duckdb_cursor.execute(download_overture_places)

        # drop table if exists first
        self.db.perform(f"DROP TABLE IF EXISTS temporal.places_{self.region}_raw;")

        # import the data into the database
        subprocess.run(
            f"""ogr2ogr -f "PostgreSQL" PG:"host={self.host} user={self.user} dbname={self.dbname} password={self.password} port={self.port}" -nln temporal.places_{self.region}_raw {file_path_raw_data} """,
            shell=True,
            check=True,
        )

        # get the geometires of the study area based on the query defined in the config
        region_geoms = self.db.select(self.data_config_collection['region'])

        # create table for the Overture places
        drop_create_table_sql = f"""
            DROP TABLE IF EXISTS temporal.places_{self.region};
            CREATE TABLE temporal.places_{self.region} AS (
                SELECT *
                FROM temporal.places_{self.region}_raw
                WHERE 1=0
            );
        """
        self.db.perform(drop_create_table_sql)

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
                WHERE ST_Intersects(p.wkb_geometry, r.geom)
                AND p.wkb_geometry && r.geom;
                ;
            """
            self.db.perform(clip_poi_overture)

        # adjust names column
        adjust_names_column = f"""
            UPDATE temporal.places_{self.region}
            SET names = TRIM(BOTH '"' FROM (names::jsonb->'common'->0->'value')::text);
        """
        self.db.perform(adjust_names_column)

        # adjust categories column
        adjust_categories_column = f"""
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
        self.db.perform(adjust_categories_column)

        # addresses -> street, housenumber, zipcode

        adjust_columns =f"""
            ALTER TABLE temporal.places_{self.region}
            ADD COLUMN street varchar,
            ADD COLUMN housenumber varchar,
            ADD COLUMN zipcode varchar;

            UPDATE temporal.places_{self.region}
            SET
                street = (SELECT substring(addresses::jsonb->0->>'freeform', '^(.*?)([0-9])')),
                housenumber = (SELECT substring(addresses::jsonb->0->>'freeform', '([0-9].*)$')),
                zipcode = (SELECT (addresses::jsonb->0->>'postcode')::varchar),
                brand = (SELECT brand::jsonb->'names'->'common'->0->>'value');
        """
        self.db.perform(adjust_columns)

        print_hashtags()
        print(f"Calculation took {time.time() - start_time} seconds ---")
        print_hashtags()

def collect_poi_overture(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    overture_poi_collection = OverturePOICollection(db=db, region=region)
    overture_poi_collection.initialize_duckdb()
    overture_poi_collection.run()
    db.conn.close()



