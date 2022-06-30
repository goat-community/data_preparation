import argparse, sys, os
from argparse import RawTextHelpFormatter
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from src.other.utility_functions import database_table2df, df2database, drop_table
from src.collection.collection import osm_collection
from src.collection.preparation import pois_preparation, landuse_preparation, buildings_preparation
from src.collection.fusion import pois_fusion
from src.collection.update import pois_update, poi_geonode_update
from src.network.network_collection import network_collection
from src.network.ways import PrepareLayers
from src.network.network_islands import NetworkIslands
from src.export.export_tables2basic import sql_queries_goat
from src.population.process_population_buildings import process_population_buildings
from src.network.conversion_dem import conversion_dem

from src.db.db import Database
from src.db.prepare import PrepareDB


#Define command line options

layers_collect = ['network', 'pois', 'population']
layers_fuse = ['pois', 'population', 'network']
layers_update = ['pois']
layers_transfer = ['pois']

db = Database()
con = db.connect()

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('-db',help='Create neccesary extensions and functions for fresh database', action='store_true')
parser.add_argument('-c','--collect',help='Please specify layer name for data collection from osm (e.g. pois, network)')
parser.add_argument('-f', '--fuse',help='Please specify layer name for data fusion from osm (e.g. pois, network)')
parser.add_argument('-u', '--update',help='Please specify layer name for data update from osm')
parser.add_argument('-t', '--transfer',help='Please specify layer name for data transfer from local to remote database')


args = parser.parse_args()
collect = args.collect
fuse = args.fuse
update = args.update
transfer = args.transfer

if args.db == True:
    prepare_db = PrepareDB(Database)
    prepare_db.create_db_functions()
    prepare_db.create_db_extensions()
    prepare_db.create_db_tables()
    prepare_db.create_db_schemas()

# if args.i == True:

if collect or collect in(layers_collect):
    if collect == 'network':
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Network collection started.')
        network_collection()
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Network collection has been finished.')
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Starting network preparation..')
        prep_layers = PrepareLayers('ways')
        prep_layers.ways()
        NetworkIslands().find_network_islands()
    elif collect == 'pois':
        pois = osm_collection('pois')[0]
        pois = pois_preparation(pois)[0]
        drop_table(con,'pois')
        df2database(pois, 'pois')
    elif collect == 'landuse':
        landuse = osm_collection('landuse')[0]
        landuse = landuse_preparation(landuse)[0]
        drop_table(con,'landuse_osm')
        df2database(landuse, 'landuse_osm')
    elif collect == 'buildings':
        buildings = osm_collection('buildings')[0]
        buildings = buildings_preparation(buildings)[0]   
        drop_table(con,'buildings_osm')
        df2database(buildings, 'buildings_osm') 
    elif collect == 'population':
        process_population_buildings()            
    else:
        print('Please specify a valid preparation type.')

if fuse or fuse in(layers_fuse):
    if fuse == 'pois':
        pois = database_table2df(con, 'pois', geometry_column='geom')
        pois = pois_fusion(pois)[0]
        drop_table(con,'pois_goat')
        df2database(pois, 'pois_goat')
        db.perform(query = 'ALTER TABLE pois_goat DROP COLUMN IF EXISTS id;')  
        db.perform(query = 'ALTER TABLE pois_goat ADD COLUMN id serial;')
        db.perform(query = 'ALTER TABLE pois_goat ADD PRIMARY KEY (id);')        
        db.perform(query = 'CREATE INDEX ON pois_goat(poi_goat_id);')
        db.perform(query = 'ALTER TABLE pois_goat RENAME COLUMN geometry TO geom;')
        db.perform(query = 'ALTER TABLE pois_goat ALTER COLUMN osm_id TYPE float USING osm_id::double precision')        
        db.perform(query = 'ALTER TABLE pois_goat ALTER COLUMN osm_id TYPE bigint USING osm_id::bigint')
        db.perform(sql_queries_goat['pois_old'])
    else:
        print('Error ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Please specify a valid fusion type.')

if update or update in(layers_update):
    if update == 'pois':
        pois_update(db,con)
    else:
        print('Error ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Please specify a valid update type.')

if transfer or transfer in(layers_transfer):
    if transfer == 'pois':
        print("WARNING! This action overwrites 'poi' table in remote database.")
        answer = input("Do you still want to replace existed 'poi' table? [yes/no]  ") 
        if answer in ["yes", "Yes", "YES"] : 
            poi_geonode_update()
        elif answer in ["no", "NO", "No"]:
            exit()
        else: 
            print("Please enter yes/no.") 
                