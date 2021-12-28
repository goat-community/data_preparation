#%%
'''This script produces a SQL table with population points.'''

import sys
import os
import importlib
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from db.db import Database


### The following files are required to be in the data directory to run the script:

#  1. study_area.sql (input)
#  2. pois.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  3. buildings_osm.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  4. study_area.osm (can be generated manually -> see instructions below)
#  5. planet_osm_point.sql (can be generated manually -> see instructions below)
#  6. landuse.sql (input: ATKIS)
#  7. landuse_osm.sql (can be generated with scripts inside this repo, but needs to be clipped to study area)
#  8. landuse_additional (input: UrbanAtlas)
#  9. mapconfig.xml (input)
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

    def _database_preparation_population(self):
        """This function prepares the database to run the function produce_population_points()."""
        # helper functions
        helper_functions = importlib.import_module("helper_functions", "src")
        self.db.perform(query = helper_functions.get_id_for_max_val.get_id_for_max_val)
        self.db.perform(query = helper_functions.classify_building.classify_building)
        self.db.perform(query = helper_functions.jsonb_array_int_array.jsonb_array_int_array)
        self.db.perform(query = helper_functions.derive_dominant_class.derive_dominant_class)
        self.db.perform(query = helper_functions.meter_degree.meter_degree)

        # Create db extensions (should be added when creating the database?)
        self.db.perform(query = "CREATE EXTENSION IF NOT EXISTS intarray;")

    def produce_population_points(self, source_population):
        '''This function produces a SQL table with population points.'''

        # check if columns exist and the type of the sql dumps -> maybe search for the first 1000 lines
        # also check for indices and primary keys -> minimum requirements: primary key on gid or id and gist index on geom
        # give user feedback e.g. there are additional columns

        # prepare database
        self._database_preparation_population()

        # execute scripts
        scripts = importlib.import_module("population", "src")
        print ('It was chosen to use population from: ', source_population)
        self.db.perform(query = scripts.data_fusion_buildings.data_fusion_buildings)
        self.db.perform(query = scripts.classify_buildings.classify_buildings)
        self.db.perform(query = scripts.create_residential_addresses.create_residential_addresses)

        if source_population == 'census_standard':
            self.db.perform(query = scripts.prepare_census.prepare_census)
            self.db.perform(query = scripts.population_census.population_census)
        elif source_population == 'census_extrapolation':
            self.db.perform(query = scripts.prepare_census.prepare_census)
            self.db.perform(query = scripts.population_extrapolated_census.population_extrapolated_census)
        elif source_population == 'disaggregation':
            # census_disaggregation: population_disaggregation
            self.db.perform(query = scripts.population_disaggregation.population_disaggregation)
        else:
            print('No valid population mode was provided. Therefore the population scripts cannot be executed.') 

### tests

def main():
    population = Population(Database = Database)
    population.produce_population_points(source_population = 'census_standard')

    return 0

if __name__ == '__main__':
    quit(main())
