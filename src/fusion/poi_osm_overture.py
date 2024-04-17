import time

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.db.tables.poi import POITable
from src.utils.utils import print_info, timing

#TODO: create index on matching_column? or matching_key?

class OSMOverturePOIFusion:
    """Fusion of OSM POIs and the places data set from the Overture Maps Foundation"""
    def __init__(self, db: Database, region: str = "de"):
        self.region = region
        self.db = db

        self.data_config = Config('poi_osm_overture', region)
        self.data_config_preparation = self.data_config.preparation

    @timing
    def run(self):
        # define poi_table_type
        poi_table_type = self.data_config_preparation['fusion']['poi_table_type']

        # create poi schema
        self.db.perform("""CREATE SCHEMA IF NOT EXISTS poi;""")

        # Create standard POI table for fusion result
        result_table_name = f"osm_overture_{self.region}_fusion_result"
        self.db.perform(POITable(data_set_type="poi", schema_name="poi", data_set_name=result_table_name).create_poi_table(table_type=poi_table_type, create_index=False))

        cur = self.db.conn.cursor()

        total_top_level_categories = len(self.data_config_preparation['fusion']['categories'])

        print_info("POI fusion started")

        for i, (top_level_category, categories) in enumerate(self.data_config_preparation['fusion']['categories'].items(), start=1):
            for category, config in categories.items():
                start_time = time.time()

                # Create temp tables for input_1 and input_2 data needed for fusion
                input_1_table_name = f"input_1_{self.region}_fusion"
                input_2_table_name = f"input_2_{self.region}_fusion"

                # Create input_1 and input_2 tables with the matching_key column
                for input in ['input_1', 'input_2']:
                    table_name = f"{input}_{self.region}_fusion"
                    query = POITable(data_set_type="poi", schema_name="", data_set_name=table_name).create_poi_table(table_type=poi_table_type, temporary=True)
                    try:
                        cur.execute(query)
                        self.db.conn.commit()
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        self.db.conn.rollback()

                    # Insert data into input_1 and input_2 tables
                    try:
                        cur.execute(config[input])
                        self.db.conn.commit()
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        self.db.conn.rollback()

                    # Add matching_key column to the input_1 and input_2 tables
                    sql_add_matching_key = f"""
                        ALTER TABLE poi_{table_name}
                        ADD COLUMN matching_key_{input} jsonb NULL;

                        UPDATE poi_{table_name}
                        SET matching_key_{input} = JSONB_BUILD_OBJECT('source', source, 'extended_source', tags->>'extended_source')
                        WHERE tags->>'extended_source' IS NOT NULL
                        AND source IS NOT NULL;
                        ;
                    """
                    try:
                        cur.execute(sql_add_matching_key)
                        self.db.conn.commit()
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        self.db.conn.rollback()

                # Execute POI fusion
                sql_poi_fusion = f"""
                    SELECT fusion_points('poi_{input_1_table_name}', 'poi_{input_2_table_name}',
                    {config['radius']}, {config['threshold']},
                    '{config['matching_column_1']}', '{config['matching_column_2']}',
                    '{config['decision_table_1']}', '{config['decision_fusion']}',
                    '{config['decision_table_2']}')
                """
                try:
                    cur.execute(sql_poi_fusion)
                    self.db.conn.commit()
                except Exception as e:
                    print(f"An error occurred: {e}")
                    self.db.conn.rollback()

                # Update top-level category
                sql_top_level_category = f"""
                    UPDATE temporal.comparison_poi
                    SET
                        other_categories = array_append(other_categories, category),
                        category = '{top_level_category}'
                    WHERE category != '{top_level_category}';
                """
                try:
                    cur.execute(sql_top_level_category)
                    self.db.conn.commit()
                except Exception as e:
                    print(f"An error occurred: {e}")
                    self.db.conn.rollback()

                # insert data into the final table
                sql_concat_resulting_tables = f"""
                    INSERT INTO poi.poi_{result_table_name}(
                        category, other_categories, name, street, housenumber, zipcode, phone, email, website, capacity, opening_hours,
                        wheelchair, source, tags, geom
                        )
                    SELECT category, other_categories, name, street, housenumber, zipcode, phone, email, website, capacity, opening_hours, wheelchair, source, tags, geom
                    FROM temporal.comparison_poi;
                """
                try:
                    cur.execute(sql_concat_resulting_tables)
                    self.db.conn.commit()
                except Exception as e:
                    print(f"An error occurred: {e}")
                    self.db.conn.rollback()

                end_time = time.time()
                print_info(f"Processed category {top_level_category} {i} of {total_top_level_categories}. This category took {end_time - start_time:.2f} seconds.")

        cur.close()

        create_indices_result_table = f"""
            CREATE INDEX ON poi.poi_{result_table_name} USING gist(geom);
        """
        self.db.perform(create_indices_result_table)

def fusion_poi_osm_overture(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    osm_overture_poi_fusion_preparation = OSMOverturePOIFusion(db, region)
    osm_overture_poi_fusion_preparation.run()
    db.conn.close()
