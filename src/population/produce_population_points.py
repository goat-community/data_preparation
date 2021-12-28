#%%
'''This script produces a SQL table with population points.'''

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from db.db import Database
db = Database()

### The following files are required to be in the data directory to run the script:

#  1. study_area.sql (input)
#  2. pois.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  3. buildings_osm.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  4. study_area.osm (can be generated manually -> see instructions below)
#  5. planet_osm_point.sql (can be generated manually -> see instructions below)
#  6. landuse.sql (input)
#  7. landuse_osm.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  8. landuse_additional (ATKIS)
#  9. mapconfig.xml (input)
# 10. ways.sql (can be generated manually -> see instructions below)
# 11. census.sql

### How to manually generate some of the files:

# 1. study_area.osm:
# from pyrosm import get_data, OSM
# downloading lastest .osm.pbf from Geofabrik or BBBike
# fp = get_data("Mittelfranken", directory = Path.cwd()/'data', update = True)
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

def produce_population_points(source_population):
    '''This function produces a SQL table with population points.'''
    print ('It was chosen to use population from: ', source_population)
    # data_fusion_buildings + helper function get_id_for_max_val
    from get_id_for_max_val import get_id_for_max_val
    db.perform(query = get_id_for_max_val)
    from data_fusion_buildings import data_fusion_buildings
    db.perform(query = data_fusion_buildings)

    # classify_buildings + helper functions jsonb_array_int_array, derive_dominant_class, classify_building
    from jsonb_array_int_array import jsonb_array_int_array
    db.perform(query = jsonb_array_int_array)
    from derive_dominant_class import derive_dominant_class
    db.perform(query = derive_dominant_class)
    from classify_building import classify_building
    db.perform(query = classify_building)
    #Create db extensions (should be added when creating the database?)
    db.perform(query = "CREATE EXTENSION IF NOT EXISTS intarray;")
    from classify_buildings import classify_buildings
    db.perform(query = classify_buildings)

    # create_residential_addresses + helper function meter_degree
    from meter_degree import meter_degree
    db.perform(query = meter_degree)
    from create_residential_addresses import create_residential_addresses
    db.perform(query = create_residential_addresses)

    if source_population == 'census_standard':
        from prepare_census import prepare_census
        db.perform(query = prepare_census)
        # census_standard: population_census
        from population_census import population_census
        db.perform(query = population_census)
    elif source_population == 'census_extrapolation':
        from prepare_census import prepare_census
        db.perform(query = prepare_census)        
        # census_extrapolation: population_extrapolated_census
        from population_extrapolated_census import population_extrapolated_census
        db.perform(query = population_extrapolated_census)
    elif source_population == 'disaggregation':
        # census_disaggregation: population_disaggregation
        from population_disaggregation import population_disaggregation
        db.perform(query = population_disaggregation)
    elif source_population == 'custom_population':
        #Some logic for checking custom population missing
        print('Custom population will be used.')
    else:
        print('No valid population mode was provided. Therefore the population scripts cannot be executed.')

produce_population_points(source_population = 'census_standard')
