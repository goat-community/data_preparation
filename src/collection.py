import os
import time
import psutil
import subprocess
import pandas as pd
import numpy as np
import geopandas as gpd
from other.utility_functions import gdf_conversion, create_pgpass
from config.config import Config
from db.db import DATABASE, Database
from fusion import database_table2df

# Function for collection data from OSM dbf and conversion to Dataframe
# Could be extended with varios type of searches and filters
# name - should be referenced to set from YAML file (e.g. "pois")
# pbf_region=None - when "None" data comes from YAML file, when specified - from it
# driver=None default variables driver could be (driver from "Fiona" drivers, e.g "GeoJSON", "GPKG")
# !! Notice if driver is specified it creates GeoJSON, but function still returns DF.
# !! If it is not specified func returns only DF




def osm_collection(conf, database=None, filename=None, return_type=None):
    create_pgpass()

    if isinstance(conf, str): 
        conf = Config(conf)
    else:
        conf = conf

    if not database:
        database = DATABASE

    print(f"Collection of osm data for {conf.name} started...")
    start_time = time.time()

    memory = psutil.virtual_memory().total
    cache = round(memory/1073741824 * 1000 * 0.75)

    dbname, host, username, port = DATABASE['dbname'], DATABASE['host'], DATABASE['user'], DATABASE['port']
    region_links = conf.collection_regions()
    work_dir = os.getcwd()
    os.chdir(os.path.join('src','data','temp'))
    files = ["raw-osm.osm.pbf", "raw-merged-osm.osm.pbf", "raw-merged-osm-new.osm.pbf", "raw-merged-osm.osm", "raw-merged-osm.osm", "raw-merged-osm-reduced.osm", "osm-filtered.osm", f'{conf.name}_p4b.style']
    for f in files:
        try:
            os.remove(f)
        except:
            pass 
    for i, rl in enumerate(region_links):
        subprocess.run(f'wget --no-check-certificate --output-document="raw-osm.osm.pbf" {rl}', shell=True, check=True)
        if i == 0:
            subprocess.run('cp raw-osm.osm.pbf raw-merged-osm.osm.pbf', shell=True, check=True)
        else:
            subprocess.run('/osmosis/bin/osmosis --read-pbf raw-merged-osm.osm.pbf --read-pbf raw-osm.osm.pbf --merge --write-pbf raw-merged-osm-new.osm.pbf', shell=True, check=True)
            subprocess.run('rm raw-merged-osm.osm.pbf && mv raw-merged-osm-new.osm.pbf raw-merged-osm.osm.pbf', shell=True, check=True)
        
        subprocess.run('rm raw-osm.osm.pbf', shell=True, check=True)

    subprocess.run('/osmosis/bin/osmosis --read-pbf file="raw-merged-osm.osm.pbf" --write-xml file="raw-merged-osm.osm"', shell=True, check=True)
    subprocess.run('osmconvert raw-merged-osm.osm --drop-author --drop-version --out-osm -o=raw-merged-osm-reduced.osm', shell=True, check=True)
    subprocess.run('rm raw-merged-osm.osm && mv raw-merged-osm-reduced.osm raw-merged-osm.osm', shell=True, check=True)
    subprocess.run('rm raw-merged-osm.osm.pbf', shell=True, check=True)
    obj_filter = conf.osm_object_filter()
    subprocess.run(obj_filter, shell=True, check=True)
    os.chdir(work_dir)
    conf.osm2pgsql_create_style()
    subprocess.run(f'PGPASSFILE=~/.pgpass_{dbname} osm2pgsql -d {dbname} -H {host} -U {username} --port {port} --hstore -E 4326 -r .osm -c ' + os.path.join('src','data','temp','osm-filtered.osm') + f' -s --drop -C {cache} --style src/data/temp/{conf.name}_p4b.style --prefix osm_{conf.name}', shell=True, check=True)
    os.chdir(os.path.join('src','data','temp'))
    subprocess.run('rm raw-merged-osm.osm', shell=True, check=True)
    subprocess.run('rm osm-filtered.osm', shell=True, check=True)
    subprocess.run(f'rm {conf.name}_p4b.style', shell=True, check=True)
    os.chdir(work_dir)
    
    tables = [f'osm_{conf.name}_line', f'osm_{conf.name}_point', f'osm_{conf.name}_polygon', f'osm_{conf.name}_roads']

    db = Database()
    con = db.connect()
    df_result = gpd.GeoDataFrame()

    for tab in tables:
        query1 = f'ALTER TABLE {tab} ALTER COLUMN tags TYPE jsonb USING tags::jsonb;'
        db.perform(query1)
        df = database_table2df(con, tab, geometry_column='way')
        df_result = pd.concat([df_result,df], sort=False).reset_index(drop=True)
    
    df_result["osm_id"] = abs(df_result["osm_id"])
    df_result = df_result.replace({np.nan: None})

    print(f"Collection of osm data for {conf.name} took {time.time() - start_time} seconds ---")

    return gdf_conversion(df_result, filename, return_type)





#============================================OUTDATED=============================================#
#=================================================================================================#


# def osm_collect_filter(config, pbf_region=None, driver=None, update=False):
#     # Timer
#     print(f"Collection and filtering {config.name} started...")
#     start_time = time.time()

#     # get region name desired pois types from yaml settings
#     if pbf_region:
#         pbf_data = pbf_region
#     else:
#         pbf_data = config.pbf_data

#     # Get defined data from Geofabrik
#     fp = get_data(pbf_data, update=update)
#     osm = OSM(fp)

#     # Create filter class with given parameters and create filter with class method
#     custom_filter = config.pyrosm_filter()

#     # Get Data from OSM with given filter ###custom_filter.tags_as_columns
#     # Additional filters can be applied (e.g. keep_nodes=False, keep_ways=True,keep_relations=True)
#     df = osm.get_data_by_custom_criteria(*custom_filter)

#     print(f"Collection and filtering took {time.time() - start_time} seconds ---")

#     return_name = config.name + '_' + pbf_data

#     # Return type if driver -> 'GeoJSON' write geojson file if driver -> 'PandasDF' return PandasDF
#     return gdf_conversion(df, return_name ,driver)

#==================== Temporary functions to convert bus_stops lines to polygons==================#
# # Bug FIX (Relazted ot POIs)
# def bus_stop_conversion(df):
#     # Timer
#     start_time = time.time()
#     print("Bus stops conversion has been started...")

# # Conversion lines back to polygons
#     df.at[df['geometry'].geom_type == "MultiLineString", 'geometry'] = df['geometry'].convex_hull

#     print(f"Conversion bus stops took {time.time() - start_time} seconds ---")
#     return df

# # Function for Concatination of bus stops with pois
# # Return dataframe and dataframe name as TUPLE or record GeoJSON as void
# # INPUT: DF(pois), DF(buses), DF(pois) name, ruturn_type ("df" or "GeoJSON"), return_filename for GeoJSON if not defined in "df_pois_name"
# def join_osm_pois_n_busstops(df_pois,  df_stops, df_pois_name=None, return_type=None, return_filename = 'pois_merged'):
#     start_time = time.time()
#     print("Merging process of pois and bus_stops has been started...")
#     df = pd.concat([df_pois,df_stops],sort=False).reset_index(drop=True)
#     df = df.replace({np.nan: None})
#     print(f"Merging process of pois and bus_stops took {time.time() - start_time} seconds ---")
#     if df_pois_name:
#         return_name = df_pois_name
#     else:
#         return_name = return_filename

#     return gdf_conversion(df, return_name ,return_type)


# Function for pyrosm library
# def pois_collection(config=None,config_buses=None,update=False,filename=None,return_type=None):
#     df_res = pd.DataFrame()
#     if not config:
#         config = Config("pois")
#     if not config_buses:
#         config_buses = Config("bus_stops")
        
#     data_set = config.pbf_data

#     for d in data_set:
#         pois_collection = osm_collect_filter(config, d, update=update)
#         temp_df = join_osm_pois_n_busstops(pois_collection[0],
#                                                     bus_stop_conversion(osm_collect_filter(config_buses,d)[0]),
#                                                     pois_collection[1])
#         if data_set.index(d) == 0:
#             df_res = temp_df
#         else:
#             df_res = pd.concat([df_res,temp_df],sort=False).reset_index(drop=True)

#     return gdf_conversion(df_res, filename, return_type=return_type)
