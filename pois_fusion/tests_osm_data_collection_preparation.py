#%%
from pyrosm_collector import osm_collect_filter, gdf_conversion
from bus_stop_preparation_merge import bus_stop_conversion, join_osm_pois_n_busstops
from pois_preparation import pois_preparation

import pandas as pd
import geopandas as gpd



# Collect data 

data_set = ["Mittelfranken", "Niederbayern","Oberbayern","Oberfranken", "Oberpfalz","Schwaben","Unterfranken"] #["Bayern"] # Mittelfranken


# 1st way - Create separate files for each region in list
# for d in data_set:
#     pois_collection = osm_collect_filter("pois",d,update=False)
#     pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],bus_stop_conversion(osm_collect_filter("bus_stops",d)[0]),pois_collection[1])

#     pois_preparation(dataframe=pois_bus_collection[0], return_type="GeoJSON",result_name=pois_bus_collection[1])


##==================================================================================================================================================##

# 2nd way - Download it separately and merge it together in one commmon file
df_res = pd.DataFrame()

for d in data_set:
    pois_collection = osm_collect_filter("pois",d, update=True)
    pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],bus_stop_conversion(osm_collect_filter("bus_stops",d)[0]),pois_collection[1])

    temp_df = pois_preparation(dataframe=pois_bus_collection[0],result_name=pois_bus_collection[1])[0]
    if data_set.index(d) == 0:
        df_res = temp_df
    else:
        df_res = pd.concat([df_res,temp_df],sort=False).reset_index(drop=True)

gdf_conversion(df_res, "Bayern", return_type='GeoJSON')
#%%