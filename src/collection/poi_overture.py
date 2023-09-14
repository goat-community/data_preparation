import duckdb
import os
import subprocess
from src.config.config import Config
from src.db.db import Database
from src.core.config import settings
from src.db.tables.poi import create_poi_table

class OverturePOICollection:
    """Collection of POIs from Overture Maps"""
    def __init__(self, db_rd: Database, region: str = "de"):
        self.region = region
        self.db_rd = db_rd
        self.db_config = db_rd.db_config
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

        file_path_raw_data = os.path.join(self.dataset_dir, f"places_{self.region}.geojsonseq")

        # Create the directory if it doesn't exist
        if not os.path.exists(self.dataset_dir):
            os.makedirs(self.dataset_dir)

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
                    bbox.minx > {self.data_config_collection['bounding_box']['bbox.minx']}
                    AND bbox.maxx < {self.data_config_collection['bounding_box']['bbox.maxx']}
                    AND bbox.miny > {self.data_config_collection['bounding_box']['bbox.miny']}
                    AND bbox.maxy < {self.data_config_collection['bounding_box']['bbox.maxy']}
            ) TO '{file_path_raw_data}'
            WITH (FORMAT GDAL, DRIVER 'GeoJSONSeq');
        """

        self.duckdb_cursor.execute(download_overture_places)

        # # drop table if exists first
        self.db_rd.perform(f"DROP TABLE IF EXISTS temporal.places_{self.region};")

        subprocess.run(
            f"""ogr2ogr -f "PostgreSQL" PG:"host={self.host} user={self.user} dbname={self.dbname} password={self.password} port={self.port}" -nln temporal.places_{self.region} {file_path_raw_data} """,
            shell=True,
            check=True,
        )

        # clip data
        clip_poi_overture = f"""
            DELETE FROM temporal.places_{self.region} p
            WHERE NOT EXISTS (
                SELECT 1
                FROM {self.data_config_collection['boundary']['table']} a
                WHERE ST_Intersects(p.wkb_geometry, a.{self.data_config_collection['boundary']['geom_column']})
                AND p.wkb_geometry && a.{self.data_config_collection['boundary']['geom_column']}
            );
        """
        self.db_rd.perform(clip_poi_overture)

        # adjust names column
        adjust_names_column = f"""
            UPDATE temporal.places_{self.region}
            SET names = TRIM(BOTH '"' FROM (names::jsonb->'common'->0->'value')::text);
        """
        self.db_rd.perform(adjust_names_column)

        # adjust categories column -> category_1, category_2 etc.

        adjust_categories_column = f"""

            ALTER TABLE temporal.places_{self.region}
            ADD COLUMN category_2 varchar,
            ADD COLUMN category_3 varchar;

            UPDATE temporal.places_{self.region}
            SET category_2 = (categories::jsonb->'alternate'->>0)::varchar,
                category_3 = (categories::jsonb->'alternate'->>1)::varchar;


            UPDATE temporal.places_{self.region}
            SET categories = TRIM(BOTH '"' FROM (categories::jsonb->>'main'));
        """
        self.db_rd.perform(adjust_categories_column)

        # addresses -> street, housenumber, zipcode

        adjust_addresses_column =f"""
            ALTER TABLE temporal.places_{self.region}
            ADD COLUMN street varchar,
            ADD COLUMN housenumber varchar,
            ADD COLUMN zipcode varchar;

            UPDATE temporal.places_{self.region}
            SET
                street = substring(addresses::jsonb->0->>'freeform', '^(.*?)([0-9])'),
                housenumber = substring(addresses::jsonb->0->>'freeform', '([0-9].*)$'),
                zipcode = (addresses::jsonb->0->>'postcode')::varchar;

        """
        self.db_rd.perform(adjust_addresses_column)

        # tags jsonb NULL, -> confidence, websites, socials, emails, phones
        create_tags_column = f"""
        ALTER TABLE temporal.places_{self.region}
        ADD COLUMN tags jsonb;

        UPDATE temporal.places_{self.region}
        SET tags = jsonb_build_object(
            'confidence', confidence,
            'website', CASE WHEN cardinality(websites) > 0 THEN websites[1] ELSE NULL END,
            'social_media', CASE WHEN cardinality(socials) > 0 THEN socials[1] ELSE NULL END,
            'phone', CASE WHEN cardinality(phones) > 0 THEN phones[1] ELSE NULL END
        );
        """
        self.db_rd.perform(create_tags_column)

        self.db_rd.perform(create_poi_table(data_set_type="poi", schema_name="temporal", data_set="overture"))

        insert_into_poi_table = f"""
            INSERT INTO temporal.poi_overture(category_1, category_2, category_3, name, street, housenumber, zipcode, tags, geom)
            SELECT
                categories,
                category_2,
                category_3,
                names,
                street,
                housenumber,
                zipcode,
                tags,
                wkb_geometry
            FROM temporal.places_{self.region};
        """

        self.db_rd.perform(insert_into_poi_table)

        #TODO: data cleaning
        # check for NULL values? exclude something? confidence? -> maybe in the end
        # drop categories = NULL? probably yes
        # drop confidence < 0.6? probably yes

def collect_poi_overture(region: str):
    db_rd = Database(settings.RAW_DATABASE_URI)
    overture_poi_collection = OverturePOICollection(db_rd=db_rd, region=region)
    overture_poi_collection.initialize_duckdb()
    overture_poi_collection.run()



