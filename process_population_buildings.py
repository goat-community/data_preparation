from src.other.utility_functions import database_table2df, df2database, drop_table, migrate_table2localdb, file2df
from src.population.population_data_preparation import population_data_preparation
from src.population.produce_population_points import Population
from src.export.export_goat import getDataFromSql
from src.export.export_tables2basic import sql_queries_goat
from preparation.poi import kindergarten_deaggrgation
from other.geocoding_functions import addLocationOfAdressToJson, GeoAddress

from src.db.db import Database

import json
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from src.other.utility_functions import database_table2df, df2database, drop_table, file2df, gdf_conversion
#from src.collection.collection import osm_collection
from preparation.poi import pois_preparation, landuse_preparation, buildings_preparation
from src.collection.fusion import pois_fusion
#from src.network.network_collection import network_collection
from src.network.ways import PrepareLayers, Profiles
from src.network.conversion_dem import conversion_dem

from src.db.db import Database
from src.db.prepare import PrepareDB


query_rt = '''CREATE TABLE public.buildings_collection (
                                gid serial4 NOT NULL,
                                old_gid int4 NULL,
                                osm_id int8 NULL,
                                building text NULL,
                                amenity text NULL,
                                residential_status text NULL,
                                housenumber text NULL,
                                street text NULL,
                                building_levels int2 NULL,
                                building_levels_residential int2 NULL,
                                roof_levels int2 NULL,
                                height float8 NULL,
                                area int4 NULL,
                                gross_floor_area_residential int4 NULL,
                                geom geometry NULL);

              CREATE INDEX ON public.buildings_collection USING gist (geom);

              CREATE TABLE public.population_collection (
                                geom geometry(point, 4326) NULL,
                                fixed_population float8 NULL,
                                population float8 NULL,
                                building_gid int4 NULL, 
                                gid serial4 NOT NULL);

              CREATE INDEX ON public.population_collection USING gist (geom);'''

query_b = '''INSERT INTO buildings_collection (old_gid,osm_id,building,amenity,residential_status,housenumber,street,
                building_levels,building_levels_residential,roof_levels,height,area,gross_floor_area_residential,geom)
             SELECT gid, osm_id,building,amenity,residential_status,housenumber,street,
                building_levels,building_levels_residential,roof_levels,height,area,gross_floor_area_residential,geom
             FROM buildings;'''

query_p = '''INSERT INTO population_collection (geom,fixed_population,population,building_gid)
             SELECT geom,fixed_population,population,building_gid
             FROM population;'''

query_upd_gid = '''UPDATE population_collection 
                   SET building_gid = buildings_collection.gid 
                   FROM buildings_collection
                   WHERE building_gid = old_gid;
                   
                   UPDATE buildings_collection
                   SET old_gid = NULL;'''

query_drop_gid = '''ALTER TABLE buildings_collection
                    DROP COLUMN old_gid;'''


from src.config.config import Config

conf = Config('population')

db = Database()
con = db.connect()
drop_table(con,'population_collection')
drop_table(con, 'buildings_collection')
db.perform(query=query_rt)

municipalities = conf.preparation['rs_codes']
for m in municipalities:
   drop_table(con, 'population')
   drop_table(con, 'buildings')
   population_data_preparation([m])
   population = Population(Database=Database)
   population.produce_population_points(source_population = 'census_extrapolation')

   db.perform(query = query_b)
   db.perform(query = query_p)  
   db.perform(query = query_upd_gid)

db.perform(query = query_drop_gid)


