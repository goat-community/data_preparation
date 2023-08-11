import pandas as pd
import geopandas as gpd

from src.config.config import Config # TODO: route types
from src.db.db import Database
from src.core.config import settings

# class PublicTransportStopsPreparation:
#     """Class to prepare/ classify public transport stops of the GTFS dataset. It processes the stops in batches and adds the route types (e.g. 3 = bus) to classify them."""

#     def __init__(self, db_goat: Database, config: Config):
#         self.db_goat = db_goat
#         self.config_public_transport_stops = config.preparation #TODO: needs to be added
            
def prepare_public_transport_stops(region: str):
        
    # region is needed as input to get correct yaml from src/config/data_variables, but probably not here
    
    db_goat = Database(settings.GOAT_DATABASE_URI)
    db_conn_goat = db_goat.return_sqlalchemy_engine().connect()
    
    
    unique_study_area_ids = pd.read_sql("""SELECT DISTINCT id FROM basic.study_area""", db_conn_goat)
    
    # create table for final result
    #TODO: read from config output stop types
    
    create_table_sql = """
        DROP TABLE IF EXISTS temporal.gtfs_stops_classified;
        CREATE TABLE temporal.gtfs_stops_classified (
            stop_id TEXT,
            stop_code TEXT,
            stop_name TEXT,
            stop_desc TEXT,
            stop_loc GEOMETRY(Point),
            zone_id TEXT,
            stop_url TEXT,
            location_type TEXT,
            parent_station TEXT,
            stop_timezone TEXT,
            wheelchair_boarding TEXT,
            level_id TEXT,
            platform_code TEXT,
            bus TEXT,
            tram TEXT,
            metro TEXT,
            rail TEXT
        );
    """

    db_conn_goat.execute(create_table_sql)
    
    for id in unique_study_area_ids:
        classify_gtfs_stops_sql = f"""
            INSERT INTO temporal.gtfs_stops_classified (
                stop_id,
                stop_code,
                stop_name,
                stop_desc,
                stop_loc,
                zone_id,
                stop_url,
                location_type,
                parent_station,
                stop_timezone,
                wheelchair_boarding,
                level_id,
                platform_code,
                bus,
                tram,
                metro,
                rail
            )
            WITH clipped_gfts_stops AS (
                SELECT s.*
                FROM gtfs.stops s, basic.study_area a
                WHERE ST_Intersects(s.stop_loc, a.geom)
                AND a.id = {id}
                AND parent_station IS NOT NULL
            )
            SELECT c.*,
                route_types ->> 'bus' AS bus,
                route_types ->> 'tram' AS tram,
                route_types ->> 'metro' AS metro,
                route_types ->> 'rail' AS rail
            FROM clipped_gfts_stops c 
                CROSS JOIN LATERAL 
                (
                    SELECT jsonb_object_agg(KEY, value) AS route_types
                    FROM 
                    (
                        SELECT DISTINCT o.route_type
                        FROM gtfs.stop_times_optimized o
                        WHERE o.stop_id = c.stop_id
                    ) r, LATERAL jsonb_each_text(('{{"0": {{"tram": "yes"}}, "1": {{"metro": "yes"}}, "2": {{"rail": "yes"}}, "3": {{"bus": "yes"}}}}'::jsonb ->> route_type::text)::jsonb) t
                ) j;
        """


        db_conn_goat.execute(classify_gtfs_stops_sql)

    #TODO: drop duplicates (stop_id) and add index + primary key
    # db_conn_goat.execute("""CREATE INDEX ON temporal.gtfs_stops_classified USING GIST(stop_loc)""")
    # db_conn_goat.execute("""ALTER TABLE temporal.gtfs_stops_classified ADD PRIMARY KEY(stop_id)""")       
    
    