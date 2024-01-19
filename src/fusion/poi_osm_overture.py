import time

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.db.tables.poi import POITable
from src.utils.utils import print_info, timing


class OSMOverturePOIFusion:
    """Fusion of OSM POIs and the places data set from the Overture Maps Foundation"""
    def __init__(self, db: Database, region: str = "de"):
        self.region = region
        self.db = db

        self.data_config = Config('poi_osm_overture', region)
        self.data_config_preparation = self.data_config.preparation

    def run(self):
        # define poi_table_type
        poi_table_type = self.data_config_preparation['fusion']['poi_table_type']

        # Create standard POI table for fusion result
        result_table_name = f"{self.region}_fusion_result"
        self.db.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=result_table_name).create_poi_table(table_type=poi_table_type))

        total_top_level_categories = len(self.data_config_preparation['fusion']['categories'])

        for i, (top_level_category, categories) in enumerate(self.data_config_preparation['fusion']['categories'].items(), start=1):
            for category, config in categories.items():
                start_time = time.time()

                 # Create temp tables for input_1 and input_2 data needed for fusion
                input_1_table_name = f"input_1_{self.region}_fusion"
                input_2_table_name = f"input_2_{self.region}_fusion"

                # Create input_1 and input_2 tables with the matching_key column
                for input in ['input_1', 'input_2']:
                    table_name = f"{input}_{self.region}_fusion" #TODO: created twice -> more elgeant solution needed
                    self.db.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=table_name).create_poi_table(table_type=poi_table_type))

                    # Insert data into input_1 and input_2 tables (if the insertion process is the same)
                    self.db.perform(config[input])

                    # Add matching_key column to the input_1 and input_2 tables
                    sql_add_matching_key = f"""
                        ALTER TABLE temporal.poi_{table_name}
                        ADD COLUMN matching_key_{input} jsonb NULL;

                        UPDATE temporal.poi_{table_name}
                        SET matching_key_{input} = JSONB_BUILD_OBJECT('source', source, 'extended_source', tags->>'extended_source')
                        WHERE tags->>'extended_source' IS NOT NULL
                        AND source IS NOT NULL;
                        ;
                    """
                    self.db.perform(sql_add_matching_key)

                # Execute POI fusion
                sql_poi_fusion = f"""
                    SELECT fusion_points('temporal.poi_{input_1_table_name}', 'temporal.poi_{input_2_table_name}',
                    {config['radius']}, {config['threshold']},
                    '{config['matching_column_1']}', '{config['matching_column_2']}',
                    '{config['decision_table_1']}', '{config['decision_fusion']}',
                    '{config['decision_table_2']}')
                """
                self.db.perform(sql_poi_fusion)

                # if category != toplevel category -> put category into other_categories and replace category with top_level_category

                # Update top-level category
                # TODO: adjust for all categories (probably only for standard needed)
                sql_top_level_category = f"""
                    UPDATE temporal.comparison_poi
                    SET
                        other_categories = array_append(other_categories, category),
                        category = '{top_level_category}'
                    WHERE category != '{top_level_category}';
                """
                self.db.perform(sql_top_level_category)

                # insert data into the final table
                sql_concat_resulting_tables = f"""
                    INSERT INTO temporal.poi_osm_overture_{self.region}_fusion_result(
                        category, other_categories ,name, street, housenumber, zipcode, phone, email, website, capacity, opening_hours,
                        wheelchair, source, tags, geom
                        )
                    SELECT category, other_categories, name, street, housenumber, zipcode, phone, email, website, capacity, opening_hours, wheelchair, source, tags, geom
                    FROM temporal.comparison_poi;
                """
                self.db.perform(sql_concat_resulting_tables)

            end_time = time.time()
            print_info(f"Processed category {top_level_category} {i} of {total_top_level_categories}. This category took {end_time - start_time:.2f} seconds.")

def fusion_poi_osm_overture(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    osm_overture_poi_fusion_preparation = OSMOverturePOIFusion(db, region)
    osm_overture_poi_fusion_preparation.run()
    db.conn.close()
