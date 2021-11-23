#%%
from collection import Config, osm_collect_filter, gdf_conversion, bus_stop_conversion, join_osm_pois_n_busstops
from preparation import file2df, landuse_preparation, pois_preparation

import os 
import sys
import yaml
import paramiko


import pandas as pd
import geopandas as gpd
# from sqlalchemy import create_engine
from fusion import replace_data_area, geonode_connection, geonode_table2df, area_n_buffer2df, base2area
from credidentials_geonode import serv_password

# POIS Collection + Preparation 

data_set = ["Mittelfranken","Niederbayern","Oberbayern","Oberfranken","Oberpfalz","Schwaben","Unterfranken"]

##====================================================Collection POIs=============================================================================##

# # 1st way - Create separate files for each region in list

config = Config("pois")
config_buses = Config("bus_stops")

for d in data_set:
    pois_collection = osm_collect_filter(config,d,update=False, driver="GPKG")
    pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],bus_stop_conversion(osm_collect_filter(config_buses,d)[0]),pois_collection[1])

    pois_preparation(dataframe=pois_bus_collection[0], return_type="GPKG",result_name=pois_bus_collection[1])

# #=============##

# 2nd way - Download it separately and merge it together in one file k
# Return type 'GPKG' could be used

# df_res = pd.DataFrame()

# for d in data_set:
#     pois_collection = osm_collect_filter(config,d, update=False)
#     pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],bus_stop_conversion(osm_collect_filter(config_buses,d)[0]),pois_collection[1])
#     temp_df = pois_preparation(dataframe=pois_bus_collection[0],result_name=pois_bus_collection[1])[0]
#     if data_set.index(d) == 0:
#         df_res = temp_df
#     else:
#         df_res = pd.concat([df_res,temp_df],sort=False).reset_index(drop=True)

# df = gdf_conversion(df_res, "pois_bayern", return_type='GPKG')[0]

# # GeoDataFrame to Geonode directly
# engine = create_engine('postgresql://geonode_data:%s@161.97.133.234:5432/' % db_password )
# df.to_postgis(con=engine, name="pois_bayern",if_exists='replace')


#==============================================================Fusion POIs===========================================================================##
# ====== Complete replace amenity ======
# table_base = "pois_bayern"

# rs_set = ["097610000", "095640000", "096630000"]

# table_input = "doctors_bavaria"

# amenity_replace = "doctors"
# columns2drop_input = ["id", "ref", "layer", "path"]
# columns2rename = {"category": "amenity", "extras": "tags", "spider": "source"}
# columns2drop_input =[]


# return_name = "data_fused_newtest"
# return_type = "GPKG"


# my_set = [amenity_replace,columns2drop_input,columns2rename,columns2drop_input, return_name, return_type]

# print(my_set)

# df_res = pd.DataFrame()

# table_base = config.fusion["table_base"]
# rs_set = config.fusion["rs_set"]
# src_1 = "geonode"
# src_2 = "geojson"
# tables_input = config.fusion_key_set(src_1)
# files_input = config.fusion_key_set(src_2)

# iterat = 0
# con = geonode_connection()

# df_base = geonode_table2df(con, table_base,geometry_column="geom")
# df_area = area_n_buffer2df(con,rs_set,buffer=8300)
# df_base2area = base2area(df_base,df_area)

# for t_input in tables_input:
#     df_input = geonode_table2df(con, t_input)
#     fusion_conf = config.fusion_set(src_1,t_input)
#     df = replace_data_area(df_base2area, df_area, df_input, *fusion_conf)[0]
#     if iterat == 0:
#         df_res = df
#     else:
#         df_res = pd.concat([df_res,df],sort=False).reset_index(drop=True)

# for f_input in files_input:
#     filename = f_input + "." + src_2
#     df_input = file2df(filename)
#     fusion_conf = config.fusion_set(src_2,f_input)
#     df = replace_data_area(df_base2area, df_area, df_input, *fusion_conf)[0]
#     if iterat == 0:
#         df_res = df
#     else:
#         df_res = pd.concat([df_res,df],sort=False).reset_index(drop=True)


# df = gdf_conversion(df_res, "pois_study_area_merged", return_type='GPKG')[0]
=======
# return_name = "data_fused_test"
# return_type = "GeoJSON"

# con = geonode_connection()
# fusion_data_areas_rs_set(con, table_base, rs_set, table_input=table_input, amenity_replace=amenity_replace, columns2rename=columns2rename,
#                                columns2drop_input=columns2drop_input, return_name=return_name, return_type=return_type)


# ====== Complete replace brand ======
# table_base = "pois_bayern"
# file_input = "dm"
# rs_set = ["097610000", "095640000", "096630000"]
# amenity_replace = "doctors"
# amenity_brand_replace = ("chemist","dm")
# columns2drop_input = ["ref", "id"]
# columns2rename = {"name": "brand", "@spider": "name"}


# return_name = "data_fused_test_brand"
# return_type = "GPKG"

# con = geonode_connection()
# fusion_data_areas_rs_set(con, table_base, rs_set, file_input=file_input, amenity_brand_replace=amenity_brand_replace, columns2rename=columns2rename,
#                          columns2drop_input=columns2drop_input,return_name=return_name, return_type=return_type)

##==============================================================TESTS===============================================================================##

# Landuse Collection + Preparation 

df_res = pd.DataFrame()

for d in data_set:
    landuse_collection = osm_collect_filter("landuse",d, update=True)
    
    temp_df = landuse_preparation(dataframe=landuse_collection[0],result_name=landuse_collection[1])[0]
    if data_set.index(d) == 0:
        df_res = temp_df
    else:
        df_res = pd.concat([df_res,temp_df],sort=False).reset_index(drop=True)

df = gdf_conversion(df_res, "landuse_bayern", return_type=None)[0]

# GeoDataFrame to Geonode directly
# engine = create_engine('postgresql://geonode_data:%s@161.97.133.234:5432/' % db_password )
# df.to_postgis(con=engine, name="landuse_bayern",if_exists='replace')
# %%
