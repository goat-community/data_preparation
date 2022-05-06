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
from src.collection.fusion import database_table2df

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

