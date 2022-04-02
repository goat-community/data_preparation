import argparse, sys, os
from argparse import RawTextHelpFormatter
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from src.other.utility_functions import database_table2df, df2database, drop_table
from src.collection import osm_collection
from src.preparation import pois_preparation, landuse_preparation, buildings_preparation
from src.fusion import pois_fusion
from src.network.network_collection import network_collection
from src.network.ways import PrepareLayers, Profiles
from src.network.conversion_dem import conversion_dem

from src.db.db import Database
from src.db.prepare import PrepareDB


#Define command line options

layers_collect = ['network', 'pois']
layers_fuse = ['pois', 'population', 'network']

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('-db',help='Create neccesary extensions and functions for fresh database', action='store_true')
parser.add_argument('-c','--collect',help='Please specify layer name for data collection from osm (e.g. pois, network)')
parser.add_argument('-f', '--fuse',help='Please specify layer name for data fusion from osm (e.g. pois, network)')

args = parser.parse_args()
collect = args.collect
fuse = args.fuse

if args.db == True:
    prepare_db = PrepareDB(Database)
    prepare_db.create_db_functions()
    prepare_db.create_db_extensions()
    prepare_db.create_db_tables()

# if args.i == True:

if collect or collect in(layers_collect):
    if collect == 'network':
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Network collection started.')
        network_collection()
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Network collection has been finished.')
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Starting network preparation..')
        prep_layers = PrepareLayers('ways')
        prep_layers.ways()
    elif collect == 'pois':
        pois = osm_collection('pois')[0]
        pois = pois_preparation(pois)[0]
        drop_table('pois')
        df2database(pois, 'pois')
    elif collect == 'landuse':
        landuse = osm_collection('landuse')[0]
        landuse = landuse_preparation(landuse)[0]
        drop_table('landuse_osm')
        df2database(landuse, 'landuse_osm')
    elif collect == 'buildings':
        buildings = osm_collection('buildings')[0]
        buildings = buildings_preparation(buildings)[0]   
        drop_table('buildings_osm')
        df2database(buildings, 'buildings_osm')             
    else:
        print('Please specify a valid preparation type.')

if fuse or fuse in(layers_fuse):
    if fuse == 'pois':
        db = Database()
        con = db.connect()
        pois = database_table2df(con, 'pois', geometry_column='geom')
        pois = pois_fusion(pois)[0]
        drop_table('pois_fused')
        df2database(pois, 'pois_fused')
        db.perform(query = 'ALTER TABLE pois_fused DROP COLUMN IF EXISTS id;')  
        db.perform(query = 'ALTER TABLE pois_fused ADD COLUMN id serial;')
        db.perform(query = 'ALTER TABLE pois_fused ADD PRIMARY KEY (id);')        
        db.perform(query = 'CREATE INDEX ON pois_fused(poi_goat_id);')
        db.perform(query = 'ALTER TABLE pois_fused RENAME COLUMN geometry TO geom;')
        db.perform(query = 'ALTER TABLE pois_fused ALTER COLUMN osm_id TYPE float USING osm_id::double precision')        
        db.perform(query = 'ALTER TABLE pois_fused ALTER COLUMN osm_id TYPE bigint USING osm_id::bigint')
    else:
        print('Error ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '  Please specify a valid fusion type.')
