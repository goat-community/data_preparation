import time
from typing import Tuple

from src.config.config import Config
from src.db.db import Database
from src.core.config import settings
from src.db.tables.poi import POITable
from src.preparation.subscription import Subscription
from src.utils.utils import print_info, timing

class OverturePOIPreparation:
    """Preparation of the places data set from the Overture Maps Foundation"""
    def __init__(self, db: Database, region: str = "de"):
        self.region = region
        self.db = db

        self.data_config = Config('poi_overture', region)
        self.data_config_preparation = self.data_config.preparation

    def create_temp_table_query(self, last_id: str, batch_size: int) -> str:
        return f"""
            DROP TABLE IF EXISTS temp_table;
            CREATE TEMPORARY TABLE temp_table AS
            SELECT
                TRIM(categories) AS categories,
                other_categories,
                TRIM(names) AS names,
                TRIM(street) AS street,
                TRIM(housenumber) AS housenumber,
                TRIM(zipcode) AS zipcode,
                TRIM((phones::json->0)::text, '"') AS phone,
                TRIM((websites::json->0)::text, '"') AS website,
                TRIM((socials::json->0)::text, '"') AS social_media,
                TRIM(brand) AS brand,
                TRIM(id) AS id,
                confidence,
                geometry
            FROM temporal.places_{self.region}
            WHERE id::bytea > '{last_id}'::bytea
            ORDER BY id
            LIMIT {batch_size};
            ALTER TABLE temp_table ADD PRIMARY KEY (id);
        """

    def insert_into_poi_table_query(self) -> str:
        return f"""
            INSERT INTO temporal.poi_overture_{self.region}_raw(category, other_categories, name, street, housenumber, zipcode, phone, website, source, tags, geom)
            SELECT
                CASE WHEN categories = '' THEN NULL ELSE categories END,
                other_categories,
                CASE WHEN names = '' THEN NULL ELSE names END,
                CASE WHEN street = '' THEN NULL ELSE street END,
                CASE WHEN housenumber = '' THEN NULL ELSE housenumber END,
                CASE WHEN zipcode = '' THEN NULL ELSE zipcode END,
                CASE WHEN phone != '""' THEN phone ELSE NULL END,
                CASE WHEN website != '""' THEN website ELSE NULL END,
                'Overture' AS source,
                (JSONB_STRIP_NULLS(
                    JSONB_BUILD_OBJECT(
                        'confidence', confidence,
                        'social_media', CASE WHEN social_media != '""' THEN social_media ELSE NULL END,
                        'brand', brand
                    ) ||
                    JSONB_BUILD_OBJECT('extended_source', JSONB_BUILD_OBJECT('ogc_fid', id))
                )) AS TAGS,
                geometry
            FROM temp_table;
        """

    def run(self):
        self.db.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=f"overture_{self.region}_raw").create_poi_table(table_type='standard'))

        batch_size = 1000000
        last_id = ''
        batch_count = 0

        total_records = self.db.select(f"SELECT COUNT(*) FROM temporal.places_{self.region};")[0][0]
        total_batches = (total_records + batch_size - 1) // batch_size  # Round up division

        while True:
            start_time = time.time()

            self.db.perform(self.create_temp_table_query(last_id, batch_size))
            result = self.db.select("SELECT MAX(id) FROM temp_table;")

            if not result or not result[0][0]:
                break

            last_id = result[0][0]
            self.db.perform(self.insert_into_poi_table_query())

            end_time = time.time()
            batch_count += 1

            print_info(f"Processed batch {batch_count} out of {total_batches}. This batch took {end_time - start_time:.2f} seconds.")

def prepare_poi_overture(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    overture_poi_preparation = OverturePOIPreparation(db, region)
    overture_poi_preparation.run()
    db.conn.close()

def export_poi(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    db_rd = Database(settings.RAW_DATABASE_URI)

    subscription = Subscription(db=db, region=region)
    subscription.subscribe_overture()

    db.conn.close()
    db_rd.conn.close()

if __name__ == "__main__":
    export_poi()