import pprint
import xml.etree.ElementTree as ET

from src.config.config import Config
from src.db.config import DATABASE, DATABASE_RD
from src.db.db import Database
from src.utils.utils import create_pgpass, create_table_schema


class CityGMLCollection:
    def __init__(self, db, db_rd):
        self.db = db
        self.db_rd = db_rd       
        self.root_dir = "/app"
        self.data_dir_input = self.root_dir + "/src/data/input/"
        self.temp_data_dir = self.data_dir_input + "temp/"
        create_pgpass(self.db.db_config)
        tree = ET.parse("building_function_types_citygml.xml")
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

    def read_buildings(self, db, geom_wkt: str):
         buildings = db.select(f"""WITH merged_buildings AS 
        (
            SELECT s.root_id, s.cityobject_id, b.roof_type, b.measured_height, b.function, s.geometry AS geom 
            FROM surface_geometry s, building b
            WHERE ST_Intersects(s.geometry, ST_TRANSFORM(ST_Geomfromtext('{geom_wkt}', 4326), 25832))
            AND geometry IS NOT NULL 
            AND ST_GeometryType (ST_FORCE2D(ST_MakeValid(s.geometry))) = 'ST_Polygon'
            AND cityobject_id = b.id 
        ), 
        unioned_buildings AS 
        (
            SELECT m.root_id, roof_type, measured_height, 
            function, ST_UNION(ST_FORCE2D(ST_makeValid(m.geom))) AS geom 
            FROM merged_buildings m, thematic_surface t
            WHERE m.cityobject_id = t.building_id 
            GROUP BY m.root_id, roof_type, measured_height, function
        )
        SELECT root_id, roof_type, measured_height, function, ST_AsText((ST_DUMP(geom)).geom)
        FROM unioned_buildings;""")
        
         return buildings
         
    def processing_units(self, db, db_rd, boundary_name: str, on_exists: str = "append"):   

        if on_exists == "replace":
            print("Building table will be created and data will be inserted.")       
            Config("buildings").download_db_schema()   
            create_table_schema(db, DATABASE, "basic.building")
        elif on_exists == 'append':
            check_if_exists = db.select("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE  table_schema = 'basic'
                    AND    table_name   = 'building'
                );"""
            )
            if check_if_exists[0][0] == True: 
                print("Data will be appended to the existing building table.")    
            else: 

                raise NameError("The building table does not exist. Therefore the data cannot be appended")
        else:
            raise ValueError("Please provide a valid argument for the parameter 'on_exists'.")            
        
        processing_units = db.select(
            f"""SELECT ST_AsText(ST_MULTI(create_equal_area_split_polygon(bb.geom, 25)))
            FROM basic.building_boundaries bb 
            WHERE name = '{boundary_name}'"""
        )
        return processing_units
    
    def run_processing(self, db, db_rd, processing_units, boundary_name: str):

        self.processing_units(db, db_rd, boundary_name="Bremen", on_exists="replace")
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

city_gml_collection = CityGMLCollection(db, db_rd)
city_gml_collection.processing_units(db, db_rd, boundary_name="Bremen", on_exists="replace") # Please note the first time you need create the table