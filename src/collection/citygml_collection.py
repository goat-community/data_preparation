from src.db.db import Database
from src.db.config import DATABASE, DATABASE_RD
from src.config.config import Config
from src.other.utils import create_table_schema
from src.other.utility_functions import create_pgpass
import xml.etree.ElementTree as ET
import pprint


class CityGMLCollection:
    def __init__(self, db_config):
         self.dbname = db_config["dbname"]
         self.host = db_config["host"]
         self.username = db_config["user"]
         self.port = db_config["port"]
         self.password = db_config["password"]
         self.root_dir = "/app"
         self.data_dir_input = self.root_dir + "/src/data/input/"
         self.temp_data_dir = self.data_dir_input + "temp/"
         create_pgpass()
         tree = ET.parse("buildings.xml")
         root = tree.getroot()
         dictentry = root.findall('{http://www.opengis.net/gml}dictionaryEntry')
         dict_building_types = {}
         for definitions in dictentry:
             for descAndNames in definitions:
                 list_name = []
                 for k in descAndNames.findall('{http://www.opengis.net/gml}name'):
                     list_name.append(k.text)
                 dict_building_types[list_name[0]] = list_name[1]
                
         self.dict_building_types = dict_building_types

    def read_buildings(self, db, geom_wkt):
         buildings = db.select(f""" WITH merged_buildings AS 
        (
            SELECT s.root_id, s.cityobject_id, b.roof_type, b.measured_height, b.function, s.geometry AS geom 
            FROM surface_geometry s, building b
            WHERE ST_Intersects(s.geometry,  ST_TRANSFORM(ST_Geomfromtext('{geom_wkt}', 4326),25832))
            AND geometry IS NOT NULL 
            AND ST_GeometryType (ST_FORCE2D(ST_MakeValid(s.geometry))) = 'ST_Polygon'
            AND cityobject_id = b.id 
        )
        SELECT m.root_id, roof_type, measured_height, 
        function, ST_AsText(ST_UNION(ST_FORCE2D(ST_makeValid(m.geom)))) AS geom 
        FROM merged_buildings m, thematic_surface t
        WHERE m.cityobject_id = t.building_id 
        GROUP BY m.root_id, roof_type, measured_height, function;""")
        
         return buildings
         
            
    
    def processing_units(self, db, db_rd):
         Config("buildings").download_db_schema()
         create_table_schema(db, DATABASE, "basic.building")
         processing_units = db.select("""SELECT ST_AsText(ST_MULTI(create_equal_area_split_polygon(hb.geom, 25)))
                                      FROM hamburg_boundries hb """)
         
         
                  
         # Read buildings from the database
         for processing_unit in processing_units:
             print('Processing unit will be computed:' + str(processing_units.index(processing_unit)))
             buildings= self.read_buildings(db_rd, processing_unit[0])
             self.insert_buildings(db, buildings)
            
     # Insert buildings into the database
    def insert_buildings (self, db, buildings):
        for building in buildings:
            if building[1] == '1000':
                roof_level = '0' 
            else:
                roof_level = '1'
            building_levels = round(building[2]/3)
            
            building_type= self.dict_building_types.get(building[3])
            db.perform(f"""INSERT INTO basic.building (id, roof_levels, building_levels, building_type, geom ) 
                       VALUES ({building[0]},{roof_level},{building_levels},'{building_type}', ST_TRANSFORM(ST_Geomfromtext('{building[4]}', 25832),4326))""")

db = Database(DATABASE)
db_rd= Database(DATABASE_RD)

city_gml_collection = CityGMLCollection(DATABASE)
city_gml_collection.processing_units(db,db_rd)
city_gml_collection.insert_buildings(db)




