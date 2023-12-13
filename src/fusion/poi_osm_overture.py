from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.db.tables.poi import POITable


class OSMOverturePOIFusion:
    """Fusion of OSM POIs and the places data set from the Overture Maps Foundation"""
    def __init__(self, db: Database, region: str = "de"):
        self.region = region
        self.db = db

        self.data_config = Config('poi_osm_overture_fusion', region)
        self.data_config_preparation = self.data_config.preparation

    def run(self):
        # define poi_table_type
        poi_table_type = self.data_config_preparation['fusion']['poi_table_type']

        # Create standard POI table for fusion result
        result_table_name = f"{self.region}_fusion_result"
        self.db.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=result_table_name).create_poi_table(table_type=poi_table_type))

        # Add matching_key column to the fusion result table
        sql_add_matching_key = f"""
            ALTER TABLE temporal.poi_{result_table_name}
            ADD COLUMN matching_key_input_1 int8 NULL,
            ADD COLUMN matching_key_input_2 int8 NULL;
        """
        self.db.perform(sql_add_matching_key)

        for top_level_category, categories in self.data_config_preparation['fusion']['categories'].items():
            for category, config in categories.items():

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
                        ADD COLUMN matching_key_{input} int8;

                        UPDATE temporal.poi_{table_name}
                        SET matching_key_{input} = (tags->>'matching_key')::int8
                        WHERE tags->>'matching_key' IS NOT NULL
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
                sql_top_level_category = f"""
                    UPDATE temporal.comparison_poi
                    SET
                        other_categories = array_append(other_categories, category),
                        category = '{top_level_category}'
                    WHERE category != '{top_level_category}';
                """
                self.db.perform(sql_top_level_category)

                #TODO: do we want to track the source? if yes, just use an config input?

                # add tag source:
                # if confidence in tags -> overture
                # if similarity >= threshold -> both
                # else -> osm

                # sql_add_source = f"""
                #     UPDATE temporal.comparison_poi
                #     SET tags =
                #         CASE
                #             WHEN decision = 'add' THEN jsonb_insert(tags, '{{source}}', '"overture"'::jsonb, true)
                #             WHEN similarity >= {self.data_config_preparation['fusion'][top_level_category][category]['threshold']} THEN jsonb_insert(tags, '{{source}}', '"both"'::jsonb, true)
                #             ELSE jsonb_insert(tags, '{{source}}', '"osm"'::jsonb, true)
                #         END
                # """
                # self.db.perform(sql_add_source)


                # insert data into the final table
                sql_concat_resulting_tables = f"""
                    INSERT INTO temporal.poi_{self.region}_fusion_result(
                        category, other_categories ,name, street, housenumber, zipcode, opening_hours,
                        wheelchair, tags, geom
                        )
                    SELECT category, other_categories, name, street, housenumber, zipcode, opening_hours, wheelchair, jsonb_set(tags, '{{original_id}}', to_jsonb(id)) AS extended_tags, geom
                    FROM temporal.comparison_poi;
                """
                self.db.perform(sql_concat_resulting_tables)

def fusion_poi_osm_overture(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    osm_overture_poi_fusion_preparation = OSMOverturePOIFusion(db, region)
    osm_overture_poi_fusion_preparation.run()
    db.conn.close()
