import json
from src.config.config import Config
from src.db.db import Database
from src.core.config import settings
from src.db.tables.poi import create_poi_table

class PublicTransportStopPreparation:
    """Class to prepare/ classify public transport stops of the GTFS dataset. It processes the stops in batches and adds the route types (e.g. 3 = bus) to classify them."""

    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region

        # Get variables for the classification
        self.config_public_transport_stop = Config("public_transport_stop", region).config['classification'] 
            
    def run(self):
        """Run the public transport stop preparation."""
                   
        unique_study_area_ids = self.db.select("""SELECT DISTINCT id FROM basic.study_area""")
        
        # Create table for public transport stops
        self.db.perform(create_poi_table(data_set_type="poi", schema_name="basic", data_set="public_transport_stop"))  
              
        category_columns = ", ".join([f"category_{i}" for i in range(1, len(self.config_public_transport_stop['gtfs_route_types']) + 1)])
        category_aliases = ', '.join([f"route_types[{i}] AS category{i}" for i in range(1, len(self.config_public_transport_stop['gtfs_route_types']) + 1)])

        for id in unique_study_area_ids:
            classify_gtfs_stop_sql = f"""
                INSERT INTO basic.poi_public_transport_stop(
                    {category_columns},
                    name,
                    geom,
                    tags                
                )
                WITH clipped_gfts_stops AS (
                    SELECT s.stop_name AS name, s.stop_loc AS geom, json_build_object('stop_id', s.stop_id) AS tags
                    FROM gtfs.stops s, basic.study_area a
                    WHERE ST_Intersects(s.stop_loc, a.geom)
                    AND a.id = {id[0]}
                    AND parent_station IS NOT NULL
                )
                SELECT {category_aliases}, c.*
                FROM clipped_gfts_stops c 
                CROSS JOIN LATERAL 
                (
                    SELECT ARRAY_AGG(route_type) AS route_types
                    FROM 
                    (
                        SELECT  '{json.dumps(self.config_public_transport_stop['gtfs_route_types'])}'::jsonb ->> r.route_type::TEXT AS route_type 
                        FROM 
                        (
                            SELECT DISTINCT o.route_type
                            FROM gtfs.stop_times_optimized o
                            WHERE o.stop_id = tags ->> 'stop_id'
                        ) r
                        WHERE route_type IS NOT NULL 
                        ORDER BY r.route_type
                    ) x 
                ) j;
            """

            self.db.perform(classify_gtfs_stop_sql)
        
def prepare_public_transport_stop(region: str):

    db_goat = Database(settings.GOAT_DATABASE_URI)
    public_transport_stop_preparation = PublicTransportStopPreparation(db=db_goat, region=region)
    public_transport_stop_preparation.run()


# if __name__ == "__main__":
#     prepare_public_transport_stop()