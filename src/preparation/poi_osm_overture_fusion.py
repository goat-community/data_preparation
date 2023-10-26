from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.db.tables.poi import POITable


class OSMOverturePOIFusion:
    """Fusion of OSM POIs and the places data set from the Overture Maps Foundation"""
    def __init__(self, db_rd: Database, region: str = "de"):
        self.region = region
        self.db_rd = db_rd

        self.data_config = Config('poi_osm_overture_fusion', region)
        self.data_config_preparation = self.data_config.preparation

    def run(self):
        # adds a serial key
        self.db_rd.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=f"{self.region}_fusion_result").create_poi_table())
        self.db_rd.perform(f"""
                           ALTER TABLE temporal.poi_{self.region}_fusion_result
                           DROP COLUMN IF EXISTS id,
                           ADD COLUMN id SERIAL PRIMARY KEY;
                           """)

        for top_level_category in self.data_config_preparation['fusion'].keys():
            for category in self.data_config_preparation['fusion'][top_level_category].keys():
                # create temp table of the input_1 data needed for the fusion
                self.db_rd.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=f"input_1_{self.region}_fusion").create_poi_table())

                # create temp table of the input_2 data needed for the fusion
                self.db_rd.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=f"input_2_{self.region}_fusion").create_poi_table())

                sql_insert_poi_input_1 = self.data_config_preparation['fusion'][top_level_category][category]['input_1']
                self.db_rd.perform(sql_insert_poi_input_1)

                sql_insert_poi_input_2 = self.data_config_preparation['fusion'][top_level_category][category]['input_2']
                self.db_rd.perform(sql_insert_poi_input_2)

                sql_poi_fusion = f"""
                    SELECT fusion_points('temporal.poi_input_1_{self.region}_fusion', 'temporal.poi_input_2_{self.region}_fusion',
                    {self.data_config_preparation['fusion'][top_level_category][category]['radius']}, {self.data_config_preparation['fusion'][top_level_category][category]['threshold']},
                    '{self.data_config_preparation['fusion'][top_level_category][category]['matching_column_1']}', '{self.data_config_preparation['fusion'][top_level_category][category]['matching_column_2']}',
                    '{self.data_config_preparation['fusion'][top_level_category][category]['decision_table_1']}', '{self.data_config_preparation['fusion'][top_level_category][category]['decision_fusion']}',
                    '{self.data_config_preparation['fusion'][top_level_category][category]['decision_table_2']}')
                """
                self.db_rd.perform(sql_poi_fusion)

                # add top level category
                sql_top_level_category = f"""
                    UPDATE temporal.comparison_poi
                    SET
                        category = '{top_level_category}',
                        category_2 = CASE WHEN category_2 = '{top_level_category}' THEN NULL ELSE category_2 END
                    ;
                """
                self.db_rd.perform(sql_top_level_category)

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
                # self.db_rd.perform(sql_add_source)


                # insert data into the final table
                sql_concat_resulting_tables = f"""
                    INSERT INTO temporal.poi_{self.region}_fusion_result(
                        category_1, category_2, category_3, category_4, category_5, name, street, housenumber, zipcode, opening_hours,
                        wheelchair, tags, geom
                        )
                    SELECT category_1, category_2, category_3, category_4, category_5, name, street, housenumber, zipcode, opening_hours, wheelchair, jsonb_set(tags, '{{original_id}}', to_jsonb(id)) AS extended_tags, geom
                    FROM temporal.comparison_poi;
                """
                self.db_rd.perform(sql_concat_resulting_tables)

def prepare_poi_osm_overture_fusion(region: str):
    db_rd = Database(settings.RAW_DATABASE_URI)
    osm_overture_poi_fusion_preparation = OSMOverturePOIFusion(db_rd, region)
    osm_overture_poi_fusion_preparation.run()
    db_rd.conn.close()
