import subprocess
import os 
import sys
import time
import psutil
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from config.config import Config
from db.config import DATABASE
from other.utility_functions import create_pgpass
from decouple import config

def network_collection(conf=None,database=None):
    create_pgpass()

    if not conf:
        conf = Config("ways")
    if not database:
        database = DATABASE

    memory = psutil.virtual_memory().total
    cache = round(memory/1073741824 * 1000 * 0.75)   

    dbname, host, username, port= DATABASE['dbname'], DATABASE['host'], DATABASE['user'], DATABASE['port']
    region_links = conf.collection_regions()
    work_dir = os.getcwd()
    os.chdir('src/data/temp')
    files = ["raw-osm.osm.pbf", "study_area.osm", "study_area_reduced", "merged-osm.osm.pbf", "merged-osm-new.osm.pbf", "merged_osm.osm", "merged_osm_reduced.osm"]
    for f in files:
        try:
            os.remove(f)
        except:
            pass

    for i, rl in enumerate(region_links):
        subprocess.run(f'wget --no-check-certificate --output-document="raw-osm.osm.pbf" {rl}', shell=True, check=True)
        subprocess.run(f'/osmosis/bin/osmosis --read-pbf file="raw-osm.osm.pbf" --write-xml file="study_area.osm"', shell=True, check=True)
        subprocess.run('osmconvert study_area.osm --drop-author --drop-version --out-osm -o=study_area_reduced.osm', shell=True, check=True)
        subprocess.run('rm study_area.osm && mv study_area_reduced.osm study_area.osm', shell=True, check=True)

        os.chdir(work_dir)
        cmd_pgr = f'PGPASSFILE=~/.pgpass_{dbname} osm2pgrouting --dbname {dbname} --host {host} --username {username} --port {port} --file src/data/temp/study_area.osm --conf src/config/mapconfig.xml --chunk 40000' # 
        if i == 0:
            cmd_pgr = cmd_pgr + ' --clean'
        subprocess.run(cmd_pgr, shell=True, check=True)

        os.chdir('src/data/temp')
        if i == 0:
            subprocess.run('cp raw-osm.osm.pbf merged-osm.osm.pbf', shell=True, check=True)
        else:
            subprocess.run('/osmosis/bin/osmosis --read-pbf merged-osm.osm.pbf --read-pbf raw-osm.osm.pbf --merge --write-pbf merged-osm-new.osm.pbf', shell=True, check=True)
            subprocess.run('rm merged-osm.osm.pbf && mv merged-osm-new.osm.pbf merged-osm.osm.pbf', shell=True, check=True)
        
        subprocess.run('rm raw-osm.osm.pbf', shell=True, check=True)
        subprocess.run('rm study_area.osm', shell=True, check=True)

    subprocess.run(f'/osmosis/bin/osmosis --read-pbf file="merged-osm.osm.pbf" --write-xml file="merged_osm.osm"', shell=True, check=True)
    subprocess.run('osmconvert merged_osm.osm --drop-author --drop-version --out-osm -o=merged_osm_reduced.osm', shell=True, check=True)
    subprocess.run('rm merged_osm.osm && mv merged_osm_reduced.osm merged_osm.osm', shell=True, check=True)
    subprocess.run('rm merged-osm.osm.pbf', shell=True, check=True)
    os.chdir(work_dir)
    subprocess.run(f'PGPASSFILE=~/.pgpass_{dbname} osm2pgsql -d {dbname} -H {host} -U {username} --port {port} --hstore -E 4326 -r .osm -c src/data/temp/merged_osm.osm -s --drop -C {cache}', shell=True, check=True)
    os.chdir('src/data/temp')
    subprocess.run('rm merged_osm.osm', shell=True, check=True)
    os.chdir(work_dir)
