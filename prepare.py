import argparse, sys, os
from argparse import RawTextHelpFormatter
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from src.other.utility_functions import database_table2df, df2database, drop_table, migrate_table2localdb
# from src.collection import osm_collection
# from src.collection.preparation import pois_preparation, landuse_preparation, buildings_preparation
# from src.collection.fusion import pois_fusion
# from src.network.network_collection import network_collection
# from src.network.ways import PrepareLayers, Profiles
# from src.network.conversion_dem import conversion_dem
from src.population.population_data_preparation import population_data_preparation
from src.population.produce_population_points import Population
from src.export.export_goat import getDataFromSql
from src.export.export_tables2basic import sql_queries_goat
from src.other.create_h3_grid import H3Grid

# from src.network.network_islands_municip import network_islands_mun

from src.db.db import Database
from src.db.prepare import PrepareDB


#Define command line options

layers_prepare = ['aoi', 'dem', 'building', 'network', 'study_area', 'population', 'grid_visualization', 'grid_calculation', 'poi']

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('-p','--prepare', required=True, help='Please specify layer name for data preparation for goat (e.g. pois, network, population, buildings, etc)')
parser.add_argument("-m", "--municipality", required=True, help = "Comma Separated Codes of the municipality to be passed e.g. 091780124")

args = vars(parser.parse_args())
prepare = args['prepare']

municipalities = [municipality.strip() for municipality in args['municipality'].split(',')]

db = Database()

if prepare or prepare in(layers_prepare):
    if prepare == 'population':
        population_data_preparation(municipalities)
        population = Population(Database=Database)
        population.produce_population_points(source_population = 'census_extrapolation')
    elif prepare == 'network':
        getDataFromSql(['ways'], municipalities)
        migrate_table2localdb('study_area')
        migrate_table2localdb('ways')
        migrate_table2localdb('ways_vertices_pgr')
        db = Database()
        db.perform(sql_queries_goat['nodes_edges'])
    elif prepare == 'grids':
        getDataFromSql(['study_area'], municipalities)
        migrate_table2localdb('study_area')
        db.perform(sql_queries_goat['study_area'])
        gdf_v = []
        gdf_c = []
        for m in municipalities:
            grid = H3Grid()
            bbox_coords = grid.create_grids_study_area_table(m)
            bbox = H3Grid().create_geojson_from_bbox(*bbox_coords)
            gdf1 = H3Grid().create_grid(m, polygon=bbox, resolution=9)
            gdf2 = H3Grid().create_grid(m, polygon=bbox, resolution=10)
            gdf_v.append(gdf1)
            gdf_c.append(gdf2)
        gdf_v = pd.concat(gdf_v)
        gdf_c = pd.concat(gdf_c)
        df2database(gdf_v, 'grid_visualization')
        df2database(gdf_c, 'grid_calculation')
        db.perform(sql_queries_goat['grids'])
    elif prepare == 'study_area':
        getDataFromSql(['study_area'], municipalities)
        migrate_table2localdb('study_area')
        db.perform(sql_queries_goat['study_area'])
    elif prepare == 'pois':
        getDataFromSql(['pois'], municipalities)
        migrate_table2localdb('pois')
        db.perform(sql_queries_goat['pois'])