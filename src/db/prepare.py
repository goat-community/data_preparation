import sys, os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
# print(sys.path[0])
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,parentdir)
from db.db import Database


class PrepareDB:
    def __init__(self, Database):
        self.db = Database()

    def read_as_str(self, path, file_name): # read sql file as text input
        sqltxt = open(path + file_name, 'r', encoding = 'utf8').read()

        return sqltxt

    def create_db_functions(self):
        """This function prepares the database to run the function produce_population_points()."""
        path = "src/db/functions/"

        self.db.perform(query = self.read_as_str(path,'get_id_for_max_val.sql'))
        self.db.perform(query = self.read_as_str(path,'classify_building.sql'))
        self.db.perform(query = self.read_as_str(path,'jsonb_array_int_array.sql'))
        self.db.perform(query = self.read_as_str(path,'derive_dominant_class.sql'))
        
        self.db.perform(query = self.read_as_str(path,'get_idw_values.sql'))
        self.db.perform(query = self.read_as_str(path,'get_slope_profile.sql'))
        self.db.perform(query = self.read_as_str(path,'compute_impedances.sql'))
        self.db.perform(query = self.read_as_str(path,'classify_way.sql'))
        self.db.perform(query = self.read_as_str(path,'create_equal_area_split_polygon.sql'))

        print("==== Importing functions finished ====")
        
    def create_db_extensions(self):
        # Create db extensions (should be added when creating the database?)
        self.db.perform(query = "CREATE EXTENSION IF NOT EXISTS intarray;")
        self.db.perform(query = "CREATE EXTENSION IF NOT EXISTS postgis_raster;")
    
    def create_db_schemas(self):
        # Create db extensions (should be added when creating the database?)
        self.db.perform(query = "CREATE SCHEMA IF NOT EXISTS basic;")
        self.db.perform(query = "CREATE SCHEMA IF NOT EXISTS extra;")
        self.db.perform(query = "CREATE SCHEMA IF NOT EXISTS temporal;")    

    def create_db_tables(self):
        self.db.perform(query = "CREATE TABLE IF NOT EXISTS poi_goat_id (poi_goat_id VARCHAR, index int, osm_id VARCHAR);")