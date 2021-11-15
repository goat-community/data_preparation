#%%
from collection import osm_collect_filter, gdf_conversion, bus_stop_conversion, join_osm_pois_n_busstops
from preparation import pois_preparation

import paramiko
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from fusion import geonode_connection, fusion_data_areas_rs_set
from credidentials_geonode import db_password, serv_password

# POIS Collection + Preparation 

data_set = ["Mittelfranken","Niederbayern","Oberbayern","Oberfranken","Oberpfalz","Schwaben","Unterfranken"]

##====================================================Collection POIs=============================================================================##

# 1st way - Create separate files for each region in list
# for d in data_set:
#     pois_collection = osm_collect_filter("pois",d,update=False)
#     pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],bus_stop_conversion(osm_collect_filter("bus_stops",d)[0]),pois_collection[1])

#     pois_preparation(dataframe=pois_bus_collection[0], return_type="GeoJSON",result_name=pois_bus_collection[1])

#=============##

# 2nd way - Download it separately and merge it together in one file 
# Return type 'GPKG' could be used

df_res = pd.DataFrame()

for d in data_set:
    pois_collection = osm_collect_filter("pois",d, update=True)[0]
    pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],bus_stop_conversion(osm_collect_filter("bus_stops",d)[0][0]),pois_collection[1])

    temp_df = pois_preparation(dataframe=pois_bus_collection[0],result_name=pois_bus_collection[1])[0]
    if data_set.index(d) == 0:
        df_res = temp_df
    else:
        df_res = pd.concat([df_res,temp_df],sort=False).reset_index(drop=True)

df = gdf_conversion(df_res, "pois_bayern", return_type='GPKG')[0]

# # GeoDataFrame to Geonode directly
# engine = create_engine('postgresql://geonode_data:%s@161.97.133.234:5432/' % db_password )
# df.to_postgis(con=engine, name="pois_bayern",if_exists='replace')


#==============================================================Fusion POIs===========================================================================##

# table_base = "pois_bayern"
# table_input = "doctors_bavaria"
# rs_set = ["097610000", "095640000", "096630000"]
# amenity_replace = "doctors"
# columns2drop_input = ["id", "ref", "layer", "path"]
# return_name = "data_fused_test"
# return_type = "GeoJSON"


# engine = geonode_connection(db_password)
# fusion_data_areas_rs_set(engine, table_input, table_base, rs_set, amenity_replace, columns2drop_input, return_name, return_type)

##=======================================================Paramiko tests=============================================================================##
# Test paramiko
# hostname = "161.97.133.234"
# username = "geonode"
# layer = "pois_bayern"

# client = paramiko.SSHClient()
# client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# try:
#     client.connect(hostname=hostname,username=username,password=serv_password)
# except:
#     print("Cannot connect to geonode server!")
#     exit()

# update_comm = "python manage.py updatelayers -f %s" % layer

# commands = ["docker exec -i spcgeonode_django_1 /bin/bash", "pwd"]

# for command in commands:
#     stdin, stdout, stderr = client.exec_command(command)
#     print(stdout.read().decode())
#     err = stderr.read().decode()
#     if err:
#         print(err)

# client.close()


##==============================================================TESTS===============================================================================##

#%%