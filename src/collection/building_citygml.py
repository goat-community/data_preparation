import xml.etree.ElementTree as ET
from src.db.db import Database
from src.core.config import settings
import os 

class CityGMLCollection:
    def __init__(self, db, db_rd):
        self.db = db
        self.db_rd = db_rd       
        tree = ET.parse(os.path.join(settings.CONFIG_DIR, "data_variables", "building", "building_function.xml"))
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
        self.insert_chunk_size = 500

    def read_buildings(self):
        buildings = self.db_rd.select(f"""
        WITH merged_buildings AS 
        (
            SELECT s.root_id, s.cityobject_id, b.roof_type, b.measured_height, b.function, s.geometry AS geom 
            FROM surface_geometry s, building b
            WHERE geometry IS NOT NULL 
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
        SELECT root_id, CASE WHEN roof_type = '1000' THEN 0 ELSE 1 END as roof_level, 
        round(measured_height / 3), '{str(self.dict_building_types).replace("'", '"')}'::jsonb ->> function, ST_TRANSFORM((ST_DUMP(geom)).geom, 4326)
        FROM unioned_buildings;""")
        
        return buildings
        
    
    def insert_buildings(self, buildings: list):
        
        # Insert buildings into other database in chunks of 1000
        for i in range(0, len(buildings), self.insert_chunk_size):
            buildings_to_insert = buildings[i:i+self.insert_chunk_size]
            buildings_to_insert = [(b[0], b[1], b[2], b[3], b[4]) for b in buildings_to_insert]
            buildings_to_insert = str(buildings_to_insert).replace('None', 'NULL')
            self.db.perform(f"""INSERT INTO basic.building (id, roof_levels, building_levels, building_type, geom) VALUES {buildings_to_insert[1:-1]}""")
            print(f"Inserted {i} buildings into the database.")
            
    def create_building_table(self):   
        """_summary_

        Args:
            on_exists (str, optional): _description_. Defaults to "append".

        Raises:
            NameError: _description_
            ValueError: _description_
        """        
        
        check_if_exists = self.db.select("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'basic'
                AND    table_name   = 'building'
            );"""
        )
        if check_if_exists[0][0] == True: 
            print("Data will be appended to the existing building table.")    
        else: 
            raise NameError("Building table does not exist. Please create the table first.")
            # print("Building table will be created and data will be inserted.")       
            # Config("building").download_db_schema()   
            # create_table_schema(db, "basic.building")
    
    def run(self):

        self.create_building_table()
        buildings = self.read_buildings()
        self.insert_buildings(buildings=buildings)
        
 
db = Database(settings.REMOTE_DATABASE_URI)
db_rd = Database(settings.CITYGML_DATABASE_URI)
 
city_gml_collection = CityGMLCollection(db, db_rd)
city_gml_collection.run()

