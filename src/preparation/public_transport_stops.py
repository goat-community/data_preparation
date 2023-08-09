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

    unique_stop_ids = pd.read_sql("""SELECT DISTINCT stop_id FROM gtfs.stop_times_optimized""", db_conn_goat)

    # Set the batch size and row count
    batch_size = 999
    current_row_count = 0

    station_types_df = None

    while current_row_count < unique_stop_ids.shape[0]: 
            
        batch_stops = unique_stop_ids.iloc[current_row_count:current_row_count+batch_size]
        current_row_count += batch_size+1  
                
        
        return_df = pd.read_sql(
            f"""
            SELECT stop_id, ARRAY_AGG(route_type) AS route_types
            FROM (
                SELECT DISTINCT stop_id, route_type
                FROM gtfs.stop_times_optimized sto 
                WHERE stop_id IN {tuple(value[0] for value in batch_stops.values)}
            ) AS subquery
            GROUP BY stop_id                  
            """, db_conn_goat)
        
        
        if station_types_df is None:
            station_types_df = return_df
        else:
            station_types_df = pd.concat([station_types_df, return_df])   


    gtfs_stops = gpd.GeoDataFrame.from_postgis("""SELECT * FROM gtfs.stops""", geom_col = 'stop_loc',con=db_conn_goat)

    gtfs_stops = gtfs_stops.merge(station_types_df, how='left', on='stop_id')

    # only keep entries where parent_station is not NaN
    gtfs_stops.dropna(subset=['parent_station'], inplace=True)

    # Custom function to check if a value exists in the set
    def check_route_type(route_set, route_value):
        if isinstance(route_set, str):
            return route_value in set(route_set.strip('{}').split(','))
        return False

    # TODO: move to config
    route_values = {
        'bus': 3,
        'tram': 0,
        'metro': 1,
        'rail': 2
    }

    # Create new columns using the custom function and the input dictionary
    for mode, value in route_values.items():
        gtfs_stops[mode] = gtfs_stops['route_types'].apply(lambda x: 'yes' if pd.notna(x) and check_route_type(x, str(value)) else 'no')


    # Drop the original 'route_types' column if you don't need it anymore
    gtfs_stops.drop(columns=['route_types'], inplace=True)

    #TODO: add final schema
    gtfs_stops.to_postgis('gtfs_stops_classified', db_conn_goat, if_exists='replace', index=False, schema='temporal')
    db_conn_goat.execute("""CREATE INDEX ON temporal.gtfs_stops_classified USING GIST(stop_loc)""")
    db_conn_goat.execute("""ALTER TABLE temporal.gtfs_stops_classified ADD PRIMARY KEY(stop_id)""")

