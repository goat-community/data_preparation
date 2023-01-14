import glob
import string
from tokenize import single_quoted
import pandas as pd
import geopandas as gpd
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from db.db import Database
import fiona
import logging as LOGGER
import re
from pathlib import Path
db = Database()
engine = db.connect_sqlalchemy()   

def initialize_import(dir):
    """
        Import Geopackages files to PostGIS
    """ 

    if dir.endswith('/'):
        dir = dir[:-1]
    
    bulk_import = BulkImport(dir)
    bulk_import.create_files_path('gpkg')  # 'gpkg' as defaut input data format
    bulk_import.bulk_import_files()

class BulkImport:

    def __init__(self, dir):    
        self.dir = dir
          
    def read_asGPD(self, file, layer = None):
        """
            Read file as geo pandas DataFrame
        """
        gpd_file = None
        
        gpd_file =  gpd.read_file(file, layer = layer)
       
        # check whether the srid is 4326
        # print (type(gpd_file.crs))   # print crs code class pyproj
        crs = str(gpd_file.crs).split(':')[-1]
        if crs == '4326':
            gpd_file = gpd_file.rename(columns={'geometry': 'geom'}).set_geometry('geom')
            # gpd_file = gpd_file.to_crs("EPSG:" + '4326')   # assign crs
            gpd_file.columns = gpd_file.columns.str.replace(r'\W+', '', regex=True)   
            gpd_file = gpd_file.loc[gpd_file.is_valid]    
            return gpd_file
        else:
            print(f'''{file} wrong reference system!''')
            return []

    def read_files(self):  #
        """
            Read files from path and return them as a list of geopandas frame
            Parameter files is a list of files path
        """
        files_store = []
        table = []
        print('Processing and validating files before importing') 
        for file in self.files:
            
            try:            
                if file.endswith('.gpkg'):
                    #Select gpkg layers based on filters(prefix, middle and sufix) and read them
                    # layers = self.select_gpkg_layers(fiona.listlayers(file))
                    layers = fiona.listlayers(file)                                    
                    for layer in layers:
                        single_gdf = self.read_asGPD(file, layer)
                        if len(single_gdf) != 0:   # check the value is valid
                            files_store.append(single_gdf)
                            table.append(layer)     # use layer name as table name
                            # table.append(Path(file).stem) 
                else:
                    print(f'''{file} is not in correct format''')
                
                print(f'''{file} processed''')
            except Exception as e:            
                print(f'''{file} file is discarded because file is either empty or invalid.''')
                print(e)
        return files_store, table

    def insert_gdf_to_postgis(self, data, table_name):   #
        """
            insert geodataframe created into postgres table
        """
        try:
            data.to_postgis(table_name, engine, if_exists='replace')
        except Exception as e :
            raise e

    def create_files_path(self, ext):  
        """
            Get list of files from the path recursively
        """
        self.files = glob.glob(self.dir + '/**/*.' + ext, recursive=True)
        
    def bulk_import_files(self):   #
        """
            Import bulk files to PostGIS
        """         
        #Read files using geopandas and return them as list
        files_store, table_store = self.read_files()  

        if len(files_store) == 0:
            print("No files selected to import because selected directory is either empty or contain invalid files.")
            raise "No files selected to import because selected directory is either empty or contain invalid files."          

        print ('--- finish read ---')
        print("Importing files to PostGIS")
            
        
        #Import geopandas file to PostGIS
        print('--- insert gdf to postgis ---')
        for i in range(len(files_store)):
            # print(type(files_store[i]))
            files_store[i].columns = files_store[i].columns.str.lower()       # lower case   
            self.insert_gdf_to_postgis(files_store[i],table_store[i])        

        print('All valid files imported successfully')

        # # #Create spatial index      
        # if self.insert_type == 'create':  
        #     create_spatial_index = 'CREATE INDEX ON ' + self.table_name + ' USING GIST(geom)'
        #     engine.execute(create_spatial_index)            
        #     print(f'''Spatial Index created for {self.table_name} table''')

# initialize_import('/app/src/data/input')
initialize_import('/home/p4b/Plan4Better/goat_community/data_preparation/src/data/input')
