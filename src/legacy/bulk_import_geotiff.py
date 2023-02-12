from osgeo import gdal
import os, glob
import subprocess
from db.config import DATABASE

from src.import.common_import_functions import BulkImport


# Creating a .pgpass for autu-authentication
cwd = os.getcwd()
user,password,host,port,dbname = DATABASE.values()
connection = f'''postgresql://{user}:{password}@{host}:{port}/{dbname}'''

subprocess.call('echo '+':'.join([str(host),str(port),dbname,user,password])+f' > {cwd}/config/.pgpass', shell=True)
subprocess.call(f'chmod 0600 {cwd}/config/.pgpass', shell=True)
#tiff_path = '/home/pankaj/Desktop/Learning/Upwork/geotiff/tif1/eu_dem_v11_E00N20/eu_dem_v11_E00N20.TIF'



def bulk_import_geotiff(args):
    """
        Import bulk geotiff files to PostGIS
    """ 
    if args['insert_type'] == 'append':
        print('Importing geotiff files to postgis does not support append mode.')
    else:
        if args['dir'].endswith('/'):
            args['dir'] = args['dir'][:-1]
            
        if args['table_name'] is None:
            args['table_name'] = args['dir'].split('/')[-1]     

        bulk_import = BulkImport(args)    
        bulk_import.create_files_path('[tT][iI][fF]')   
        bulk_import.discardfiles()
        files = bulk_import.files        
        
        #create temp directory
        print('Creating temp directory in src to store reprojected files')
        temp_dir = cwd +"/temp"
        subprocess.call(f'''mkdir {temp_dir}''', shell=True)

        #Reproject GeoTiff files
        for file in files:
            print(f'''Reprojecting {file} to {args['srid']}''')
            destination = temp_dir + '/' + file.split('/')[-1]        
            subprocess.call(f'''gdalwarp -t_srs EPSG:{args['srid']} {file} {destination}''', shell=True)
            print(f'''{file} reprojected to {args['srid']} and placed in {temp_dir}''')

        #Import geotiff files to PostGIS
        print('Importing raster files to PostGIS')
        cmd_call = f'''PGPASSFILE={cwd}/config/.pgpass raster2pgsql -c -C -s {args['srid']} -f rast -F -I -k -M -t 100x100 {temp_dir + '/*.[tT][iI][fF]'} {args['table_name']} | psql {connection}'''        
        subprocess.call(cmd_call, shell=True)
        print('Selected raster files imported successfully')
        print('Deleting temp directory')
        subprocess.call(f''' rm -r {temp_dir}''', shell=True)