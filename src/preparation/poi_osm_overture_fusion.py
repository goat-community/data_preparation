from src.config.config import Config
from src.db.db import Database
from src.core.config import settings
from src.db.tables.poi import create_poi_table

class OSMOverturePOIFusion:
    """Fusion of OSM POIs and the places data set from the Overture Maps Foundation"""
    def __init__(self, db_rd: Database, region: str = "de"):
        self.region = region
        self.db_rd = db_rd

        self.data_config = Config('poi_osm_overture_fusion', region)
        self.data_config_preparation = self.data_config.preparation

    def run(self):
        # create temp table for osm data needed for the fusion
        self.db_rd.perform(create_poi_table(data_set_type="poi", schema_name="temporal", data_set=f"osm_{self.region}_fusion"))
        sql_insert_poi_osm = f"""
            INSERT INTO temporal.poi_osm_{self.region}_fusion(category_1, name, street, housenumber, zipcode, opening_hours, wheelchair, tags, geom)
            SELECT category, name, street, housenumber, zipcode, opening_hours, tags ->> 'wheelchair',
            tags::jsonb || JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT('osm_id', osm_id, 'osm_type', 'osm_type', 'phone',
            phone, 'email', tags ->> 'email', 'capacity', tags ->> 'capacity', 'website', website)),
            geom
            FROM public.poi_osm_{self.region}
            WHERE category IN ({', '.join(["'{}'".format(cat.replace("'", "''")) for cat in self.data_config_preparation['fusion']['category_osm']])});
        """
        #TODO: completes sql in config

        # table schema and table name of public.poi_osm_{self.region} might change
        self.db_rd.perform(sql_insert_poi_osm)

        # create temp table for overture data needed for the fusion
        self.db_rd.perform(create_poi_table(data_set_type="poi", schema_name="temporal", data_set=f"overture_{self.region}_fusion"))
        sql_insert_poi_overture = f"""
            INSERT INTO temporal.poi_overture_{self.region}_fusion(category_1, name, street, housenumber, zipcode, opening_hours, wheelchair, tags, geom, id)
            SELECT category_1, name, street, housenumber, zipcode, opening_hours, wheelchair, tags, geom, id
            FROM temporal.poi_overture_{self.region}
            WHERE category_1 IN ({', '.join(["'{}'".format(cat.replace("'", "''")) for cat in self.data_config_preparation['fusion']['category_overture']])});
        """
        self.db_rd.perform(sql_insert_poi_overture)

        # additional input id column?
        self.db_rd.perform(f"""SELECT fusion_points('temporal.poi_osm_{self.region}_fusion', 'temporal.poi_overture_{self.region}_fusion', {self.data_config_preparation['fusion']['radius']}, {self.data_config_preparation['fusion']['threshold']}, 'name', 'name')""")

def prepare_poi_osm_overture_fusion(region: str):
    db_rd = Database(settings.RAW_DATABASE_URI)
    osm_overture_poi_fusion_preparation = OSMOverturePOIFusion(db_rd, region)
    osm_overture_poi_fusion_preparation.run()
    db_rd.conn.close()