#%%
'''This script produces a SQL table with population points.'''
import imp
from re import I
import sys
import os
import importlib
import geopandas as gpd
# from winreg import QueryInfoKey
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from db.db import Database
from src.population.data_fusion_buildings import data_fusion_buildings as sql_data_fusion_buildings
from src.population.classify_buildings import classify_buildings as sql_classify_buildings
from src.population.classify_buildings import intersect_landuse as sql_intersection_landuse
from src.population.classify_buildings import intersect_landuse_additional as sql_intersect_landuse_additional
from src.population.classify_buildings import intersect_osm_landuse as sql_intersect_osm_landuse
from src.population.classify_buildings import finalized_classification as sql_finalized_classification
from src.population.classify_buildings import buildings_update as sql_buildings_update
from src.population.create_residential_addresses import create_residential_addresses
from src.population.prepare_census import prepare_census
from src.population.population_census import population_census
from src.population.population_extrapolated_census import population_extrapolated_census
from src.population.population_disaggregation import population_disaggregation
from src.population.building_classifier import building_prediction
from sqlalchemy import create_engine

### The following files are required to be in the data directory to run the script:

#  1. study_area.sql (input)
#  2. pois.sql (can be generated with scripts inside this repo, but needs to be clipped to study area) ????
#  3. buildings_osm.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  4. study_area.osm (can be generated manually -> see instructions below)
#  5. planet_osm_point.sql (can be generated manually -> see instructions below)
#  6. landuse.sql (input: ATKIS)
#  7. landuse_osm.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  8. landuse_additional (input: UrbanAtlas)
#  9. mapconfig.xml (input) ????
# 10. ways.sql (can be generated manually -> see instructions below)
# 11. census.sql

### How to manually generate some of the files:

# 1. study_area.osm:
# from pyrosm import get_data, OSM
# downloading lastest .osm.pbf from Geofabrik or BBBike
# fp = get_data("Mittelfranken", directory = Path(__file__).parent/'data', update = True)
# print("Data was downloaded to:", fp)
# for Nürnberg run in container (before start container using command line: docker-compose up -d and ssh into container: docker exec -it db_data_preparation /bin/bash)
# https://www.openstreetmap.org/export#map=11/49.4471/11.1525 for the bounding box
# osmosis --read-pbf file="src/data/mittelfranken-latest.osm.pbf" --bounding-box top=49.5742 left=10.8861 bottom=49.3009 right=11.3331 --write-xml file="src/data/study_area.osm"

# 2. planet_osm_point.sql:
# Create db extensions (should be added when creating the database?)
# db.perform(query = "CREATE EXTENSION IF NOT EXISTS hstore;")
# study_area.osm is required to be in the data directory
# run in container (before start container using command line: docker-compose up -d and ssh into container: docker exec -it db_data_preparation /bin/bash)
# osm2pgsql -d goat -H localhost -U goat --password --hstore -E 4326 -c src/data/study_area.osm

# 3. ways.sql
# Create db extensions (should be added when creating the database?)
# db.perform(query = "CREATE EXTENSION IF NOT EXISTS pgrouting;")
# mapconfig.xml is required to be in the data directory e.g. copy from main goat repo
# run in Container (before start container using command line: docker-compose up -d and ssh into container: docker exec -it db_data_preparation /bin/bash)
# osm2pgrouting --file "src/data/study_area.osm" --conf "src/data/mapconfig.xml" --clean --dbname goat --username goat --host localhost --password earlmanigault
# from network_temporary.ways import PrepareLayers
# preparelayers = PrepareLayers(Database = Database)
# preparelayers.ways()

#######

class Population():

    def __init__(self, Database):
        self.db = Database()     
    
    def produce_population_points(self, source_population):
        '''This function produces a SQL table with population points.'''

        # check if columns exist and the type of the sql dumps -> maybe search for the first 1000 lines
        # also check for indices and primary keys -> minimum requirements: primary key on gid or id and gist index on geom
        # give user feedback e.g. there are additional columns

        # execute scripts
        scripts = importlib.import_module("population", "src")

        print ('It was chosen to use population from: ', source_population)
        self.db.perform(query = sql_data_fusion_buildings)
        print('------ data fusion finished! ------')
        self.db.perform(query = sql_classify_buildings)
        print('------ buildings classificaton finished! ------')
        self.db.perform(query = sql_intersection_landuse)
        print ('------ landuse intersection finished! ------')
        self.db.perform(query = sql_intersect_landuse_additional)
        print ('------ landuse additional intersection finished! ------')
        self.db.perform(query = sql_intersect_osm_landuse)
        print ('------ osm landuse intersection finished! ------')
        self.db.perform(query = sql_finalized_classification)
        print ('------ finilized classification finished! ------')
        self.db.perform(query = sql_buildings_update)
        print ('------ buildings upate finished! ------')

        # further classification: select residential_status = potential_residents 
        sql_potential_residents = "SELECT gid, residential_status, geom FROM buildings b WHERE b.residential_status = 'potential_residents'"

        # didn't connect with engine, only used connect() function
        gdf_potential_residents = gpd.GeoDataFrame.from_postgis(sql_potential_residents, self.db.connect_sqlalchemy())

        #gdf_potential_residents = self.db.select(query = sql_potential_residents)
        
        # print('------ data fetching finished! ------')
        print('the number of rows: ', len(gdf_potential_residents))

        prediction_type = building_prediction(gdf_potential_residents)
        prediction_type.to_postgis('prediction_type', self.db.connect_sqlalchemy(), if_exists='replace')   # why?
        
        # data prediction: save it to database
        
        # updating database with prediction results
        
        sql_update_building_types = f'''
        UPDATE buildings b 
        SET residential_status = 'with_residents'
        FROM prediction_type pt
        WHERE b.gid = pt.gid AND pt.pred_label = 1;

        UPDATE buildings b
        SET residential_status = 'no_residents'
        FROM prediction_type pt
        WHERE b.gid = pt.gid AND pt.pred_label = 2;'''

        self.db.perform(query = sql_update_building_types)

        self.db.perform(query = create_residential_addresses)

        if source_population == 'census_standard':
            self.db.perform(query = prepare_census)
            self.db.perform(query = population_census)
        elif source_population == 'census_extrapolation':
            self.db.perform(query = prepare_census)
            self.db.perform(query = population_extrapolated_census)
        elif source_population == 'disaggregation':
            # census_disaggregation: population_disaggregation
            self.db.perform(query = population_disaggregation)
        else:
            print('No valid population mode was provided. Therefore the population scripts cannot be executed.') 


    
# population = Population(Database=Database)
# population.produce_population_points(source_population = 'census_extrapolation')







# def main():
#     population = Population(Database = Database)
#     population.produce_population_points(source_population = 'census_standard')

#     return 0

# if __name__ == '__main__':
#     quit(main())
