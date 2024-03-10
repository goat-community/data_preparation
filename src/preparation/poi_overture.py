import time

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

    @timing
    def run(self):
        self.db.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=f"overture_{self.region}_raw").create_poi_table(table_type='standard', create_index=False))

        # Add loop_id column + drop indices
        sql_adjust_table = f"""
            DROP INDEX IF EXISTS temporal.places_{self.region}_geometry_idx;
            ALTER TABLE temporal.places_{self.region} DROP CONSTRAINT IF EXISTS places_{self.region}_pkey;
            ALTER TABLE temporal.places_{self.region} ADD COLUMN IF NOT EXISTS loop_id SERIAL;
            CREATE INDEX ON temporal.places_{self.region} (loop_id);
        """
        self.db.perform(sql_adjust_table)

        print_info(f"created table overture_{self.region}_raw + created loop id and dropped indices")

        batch_size = 100000

        cur = self.db.conn.cursor()

        max_loop_id = self.db.select(f"SELECT MAX(loop_id) FROM temporal.places_{self.region};")[0][0]
        total_batches = max_loop_id // batch_size + (max_loop_id % batch_size > 0)

        for i, offset in enumerate(range(0, max_loop_id, batch_size)):
            start_time = time.time()

            # Create temp table
            create_temp_table_query = f"""
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
                WHERE loop_id >= {offset} AND loop_id < {offset + batch_size};
            """

            try:
                cur.execute(create_temp_table_query)
            except Exception as e:
                print(f"An error occurred: {e}")
                self.db.conn.rollback()

            # Insert into POI table
            insert_into_poi_table_query = f"""
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

            try:
                cur.execute(insert_into_poi_table_query)
                self.db.conn.commit()
            except Exception as e:
                print(f"An error occurred: {e}")
                self.db.conn.rollback()

            end_time = time.time()
            print_info(f"Batch {i+1} out of {total_batches} processed. This batch took {end_time - start_time:.2f} seconds.")

        cur.close()

        # Remove loop_id column
        sql_adjust_table = f"""
            ALTER TABLE temporal.places_{self.region} DROP COLUMN IF EXISTS loop_id;
            CREATE INDEX places_{self.region}_geometry_idx ON temporal.places_{self.region} USING gist(geometry);
            ALTER TABLE temporal.places_{self.region} ADD PRIMARY KEY (id);
        """
        self.db.perform(sql_adjust_table)

        # create poi schema
        self.db.perform("""CREATE SCHEMA IF NOT EXISTS poi;""")

        # Clean data
        clean_data = f"""
            DROP TABLE IF EXISTS poi.poi_overture_{self.region};
            CREATE TABLE poi.poi_overture_{self.region} AS (
                SELECT *
                FROM temporal.poi_overture_{self.region}_raw
                WHERE category IS NOT NULL
                AND (tags ->> 'confidence')::numeric > 0.6
            );
            ALTER TABLE poi.poi_overture_{self.region} ADD PRIMARY KEY (id);
            CREATE INDEX ON poi.poi_overture_{self.region} USING gist(geom);
        """
        self.db.perform(clean_data)

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
