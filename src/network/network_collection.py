import subprocess
import os 
import sys
import time
import psutil
from urllib import request
from src.other.utils import print_info, print_warning, download_link, delete_dir, print_hashtags

from src.config.config import Config
from db.config import DATABASE
from other.utility_functions import create_pgpass
from decouple import config
from functools import partial
from multiprocessing.pool import Pool
from time import time

def prepare_osm_data(link):
    full_name = link.split('/')[-1]
    only_name = full_name.split(".")[0]
    subprocess.run(f'osmconvert {full_name} --drop-author --drop-version --out-osm -o={only_name}.o5m', shell=True, check=True)
    subprocess.run(f'osmfilter {only_name}.o5m -o={only_name + "_network"}.o5m --keep="highway= cycleway= junction="', shell=True, check=True)
    subprocess.run(f'osmconvert {only_name + "_network"}.o5m -o={only_name + "_network"}.osm', shell=True, check=True)
    print_info(f"Preparing file {full_name}")

def network_collection(conf=None,database=None):
    create_pgpass()

    if not conf:
        conf = Config("ways")
    if not database:
        database = DATABASE
    
    # Get system specs
    memory = psutil.virtual_memory().total
    cache = round(memory/1073741824 * 1000 * 0.75)   
    available_cpus = os.cpu_count()

    # Get credentials and clean folder
    dbname, host, username, port= DATABASE['dbname'], DATABASE['host'], DATABASE['user'], DATABASE['port']
    region_links = conf.pbf_data
    temp_data_dir = os.path.abspath(os.curdir) + '/src/data/temp'
    work_dir = os.getcwd()
    # delete_dir(temp_data_dir)
    # os.mkdir(temp_data_dir)
    os.chdir(temp_data_dir)

    # # Download all needed files
    # download = partial(download_link, '')
    # pool = Pool(processes=available_cpus)

    # print_hashtags()
    # print_info(f"Downloading OSM files started.")
    # print_hashtags()
    # pool.map(download, region_links)
    
    # # Prepare and filter osm files
    # print_hashtags()
    # print_info(f"Preparing OSM files started.")
    # print_hashtags()
    # pool.map(prepare_osm_data, region_links)

    # pool.close()
    # pool.join()

    # # Merge all osm files
    # merge_files = ''
    # print_info("Merging files")
    # file_names = [f.split('/')[-1] for f in region_links]
    # #subprocess.run(f'osmium merge {" ".join(file_names)} -o merged.osm.pbf', shell=True, check=True)
    # #subprocess.run(f'osmconvert merged.osm.pbf -o=merged.osm', shell=True, check=True)
    

    total_cnt_links = len(region_links)
    cnt_link = 0
    
    for link in region_links:
        cnt_link += 1
        full_name = link.split('/')[-1]
        network_file_name = full_name.split(".")[0] + "_network.osm"
        print_info(f"Importing {full_name}")
        if cnt_link == 1:
            subprocess.run(f'PGPASSFILE=~/.pgpass_{dbname} osm2pgrouting --dbname {dbname} --host {host} --username {username}  --file {network_file_name} --no-index --clean --conf /app/src/config/mapconfig.xml --chunk 40000', shell=True, check=True)
        elif cnt_link != total_cnt_links:
            subprocess.run(f'PGPASSFILE=~/.pgpass_{dbname} osm2pgrouting --dbname {dbname} --host {host} --username {username}  --file {network_file_name} --no-index --conf /app/src/config/mapconfig.xml --chunk 40000', shell=True, check=True)
        else:
            subprocess.run(f'PGPASSFILE=~/.pgpass_{dbname} osm2pgrouting --dbname {dbname} --host {host} --username {username}  --file {network_file_name} --conf /app/src/config/mapconfig.xml --chunk 40000', shell=True, check=True)


    # Import all OSM data using OSM2pgsql
    # TODO: Avoid creating osm_planet_polygon table here + no index creation
    #subprocess.run(f'PGPASSFILE=~/.pgpass_{dbname} osm2pgsql -d {dbname} -H {host} -U {username} --port {port} --hstore -E 4326 -r pbf -c merged.osm.pbf -s --drop -C {cache}', shell=True, check=True)



    # for i, rl in enumerate(region_links):
    #     subprocess.run(f'wget --no-check-certificate --output-document="raw-osm.osm.pbf" {rl}', shell=True, check=True)
    #     subprocess.run(f'/osmosis/bin/osmosis --read-pbf file="raw-osm.osm.pbf" --write-xml file="study_area.osm"', shell=True, check=True)
    #     subprocess.run('osmconvert study_area.osm --drop-author --drop-version --out-osm -o=study_area_reduced.osm', shell=True, check=True)
    #     subprocess.run('rm study_area.osm && mv study_area_reduced.osm study_area.osm', shell=True, check=True)

    #     os.chdir(work_dir)
    #     cmd_pgr = f'PGPASSFILE=~/.pgpass_{dbname} osm2pgrouting --dbname {dbname} --host {host} --username {username} --port {port} --file src/data/temp/study_area.osm --conf src/config/mapconfig.xml --chunk 40000' # 
    #     if i == 0:
    #         cmd_pgr = cmd_pgr + ' --clean'
    #     else:
    #         cmd_pgr = cmd_pgr + ' --no-index'
    #     subprocess.run(cmd_pgr, shell=True, check=True)

    #     os.chdir('src/data/temp')
    #     if i == 0:
    #         subprocess.run('cp raw-osm.osm.pbf merged-osm.osm.pbf', shell=True, check=True)
    #     else:
    #         subprocess.run('/osmosis/bin/osmosis --read-pbf merged-osm.osm.pbf --read-pbf raw-osm.osm.pbf --merge --write-pbf merged-osm-new.osm.pbf', shell=True, check=True)
    #         subprocess.run('rm merged-osm.osm.pbf && mv merged-osm-new.osm.pbf merged-osm.osm.pbf', shell=True, check=True)
        
    #     subprocess.run('rm raw-osm.osm.pbf', shell=True, check=True)
    #     subprocess.run('rm study_area.osm', shell=True, check=True)

    # subprocess.run(f'/osmosis/bin/osmosis --read-pbf file="merged-osm.osm.pbf" --write-xml file="merged_osm.osm"', shell=True, check=True)
    # subprocess.run('osmconvert merged_osm.osm --drop-author --drop-version --out-osm -o=merged_osm_reduced.osm', shell=True, check=True)
    # subprocess.run('rm merged_osm.osm && mv merged_osm_reduced.osm merged_osm.osm', shell=True, check=True)
    # subprocess.run('rm merged-osm.osm.pbf', shell=True, check=True)
    # os.chdir(work_dir)
    # subprocess.run(f'PGPASSFILE=~/.pgpass_{dbname} osm2pgsql -d {dbname} -H {host} -U {username} --port {port} --hstore -E 4326 -r .osm -c src/data/temp/merged_osm.osm -s --drop -C {cache}', shell=True, check=True)
    # os.chdir('src/data/temp')
    # subprocess.run('rm merged_osm.osm', shell=True, check=True)
    # os.chdir(work_dir)

network_collection()