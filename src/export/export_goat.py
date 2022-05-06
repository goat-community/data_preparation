import geopandas as gpd
from src.db.db import Database
from src.db.config import DATABASE, DATABASE_RD
import os
from src.other.create_h3_grid import H3Grid
import subprocess
import datetime
from src.export.export_sql_queries import *
from src.other.utility_functions import create_pgpass

cwd = os.getcwd()
user_rd,password_rd,host_rd,port_rd,dbname_rd = DATABASE_RD.values()
user,password,host,port,dbname = DATABASE.values()

create_pgpass()

# db_rd = Database()
# con_rd = db_rd.connect_rd()

db = Database()
#con = db.connect()

#Create folder for the export results
if os.path.isdir('export_results') == False:
    os.makedirs('export_results')

def export_layer(layer_name, municipalities, export_formats):  
    db_rd = Database()
    # If layer_name is study_area or exist in dictionary, only then it will be exported. Otherwise, export will be discarded.
    if layer_name in sql_queries or layer_name == "study_area":
        print('Data for %s is searched in the database.' % layer_name)    
        start = datetime.datetime.now()  
        if layer_name in sql_queries and layer_name != "study_area":   
            db_rd.perform_rd(sql_queries[layer_name])
        # df= gpd.GeoDataFrame.from_postgis('SELECT * FROM %s' % layer_name, con_rd, geom_col='geom' )

        # if df.empty == False:
        #     if 'shp' in export_formats:
        #         print('A shapefile for %s will be created.' % layer_name)
        #         df.to_file('export_results/%s.shp' % layer_name,  driver='ESRI Shapefile',  encoding='utf-8')

        #     if 'geojson' in export_formats:
        #         print('A geojson for %s will be created.' % layer_name)
        #         df.to_file('export_results/%s.geojson' % layer_name, driver="GeoJSON")        

        #     if 'sql' in export_formats:
        #         print('A sql dump for %s will be created.' % layer_name)
        #         cmd_call = f'''PGPASSFILE={cwd}/config/.pgpass /usr/bin/pg_dump -h {host} -t {layer_name} --no-owner -U {user} {dbname} > export_results/{layer_name}.sql'''
        #         subprocess.call(cmd_call, shell=True)
        #         pass

        end = datetime.datetime.now() 
        print('Export took: %s seconds' % (end-start).total_seconds())
    else:
        #Discarding export becaue layer_name not found in dictionary
        print(f'''Exporting failed for {layer_name} because it couldn't be found in dictionary''')

def getDataFromSql(layer_names, municipalities, export_formats=['shp','sql', 'geojson']):   
    db_rd = Database()
    con_rd = db_rd.connect_rd()
    print(municipalities)
    #Create temp table for study_area
    for i, mun in enumerate(municipalities):
        if i == 0:
            db_rd.perform_rd(f'''CREATE SCHEMA IF NOT EXISTS temporal;
            DROP TABLE IF EXISTS temporal.study_area;
            CREATE TABLE temporal.study_area AS 
            SELECT rs, name, sum_pop::integer, geom, gen, default_building_levels, default_roof_levels
            FROM public.sub_study_area
            WHERE rs = '{mun}';''') # --germany_municipalities_districts 
        else:
            db_rd.perform_rd(f'''
            INSERT INTO temporal.study_area (rs, name, sum_pop, geom, gen, default_building_levels ,default_roof_levels)
            SELECT rs, name, sum_pop::integer, geom, gen, default_building_levels ,default_roof_levels
            FROM public.sub_study_area 
            WHERE rs = '{mun}';''')

    if db_rd.select_rd('SELECT * FROM temporal.study_area LIMIT 1') == []:
        sql_municipalities = ('''CREATE TABLE study_area AS
        SELECT rs, gen AS name, ewz::integer AS sum_pop, geom, gen 
        FROM public.germany_municipalities
        WHERE rs IN(SELECT UNNEST(ARRAY%s));
        ''' % municipalities)

    db_rd.perform_rd('CREATE INDEX ON temporal.study_area USING GIST(geom);')
    
    # If layer_names is 'all', then all the layers from sql_queries will be exported including the study_area otherwise layers specified by user will be exported    
    layer_names = sql_queries if layer_names[0] == 'all' else layer_names
    
    for layer_name in layer_names:
        print(f'''Exporting {layer_name} with municipalities code {municipalities}''')
        if layer_name == 'grids':
            print('Data for %s is searched in the database.' % layer_name)
            for i, mun in enumerate(municipalities):
                if i == 0:
                    grid = H3Grid()
                    bbox_coords = grid.create_grids_study_area_table(f'{mun}')
                    bbox = H3Grid().create_geojson_from_bbox(*bbox_coords)
                    df_gv = H3Grid().create_grid(mun, polygon=bbox, resolution=9, layer_name='grid_visualization')
                    db = Database()
                    con = db.connect_rd_sqlalchemy()
                    df_gv.to_postgis(con=con, schema = 'temporal', name = 'grid_visualization', if_exists='replace',index=False)
                    df_gc = H3Grid().create_grid(mun, polygon=bbox, resolution=10, layer_name='grid_calculation')
                    df_gc.to_postgis(con=con, schema = 'temporal', name = 'grid_calculation', if_exists='replace',index=False)

                else:
                    grid = H3Grid()
                    bbox_coords = grid.create_grids_study_area_table(f'{mun}')
                    bbox = H3Grid().create_geojson_from_bbox(*bbox_coords)
                    df_gv = H3Grid().create_grid(mun, polygon=bbox, resolution=9, layer_name='grid_visualization')           
                    db = Database()
                    con = db.connect_rd_sqlalchemy()
                    df_gv.to_postgis(con=con, schema = 'temporal', name = 'grid_visualization', if_exists='append',index=False)
                    df_gc = H3Grid().create_grid(mun, polygon=bbox, resolution=10, layer_name='grid_calculation')
                    df_gc.to_postgis(con=con, schema = 'temporal', name = 'grid_calculation', if_exists='append',index=False)
        else:
            export_layer(layer_name, municipalities, export_formats)
            print('\n')    

    # for i in sql_queries.keys():
    #     if i != 'study_area':
    #         db.perform('''DROP TABLE IF EXISTS %s''' % i)

    con_rd.close()


# getDataFromSql('grids', )