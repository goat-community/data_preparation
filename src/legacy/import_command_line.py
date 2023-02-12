import argparse

from src.import.common_import_functions import initialize_import
from src.import.bulk_import_geotiff import bulk_import_geotiff

ap = argparse.ArgumentParser()

ap.add_argument("-t", "--type", required=True, help = "Specify the file type to import: tif, shp, geojson, gpkg")
ap.add_argument("-d", "--dir", required=True, help = "Directory Path of files")
ap.add_argument("-r", "--srid", required=True, help = "Target SRID")
ap.add_argument("-i", "--insert_type", required=True, help = "Specify create or append")
ap.add_argument("-c", "--discard_files", help = "Comma Separated files name to be discarded")
ap.add_argument("-p", "--prefix", help = "Match prefix with gpkg layers to import")
ap.add_argument("-m", "--middle", help = "Match middle substring with gpkg layers to import")
ap.add_argument("-s", "--sufix", help = "Match sufix with gpkg layers to import")
ap.add_argument("-n", "--table_name", help = "Specify Table name to import file")

args = vars(ap.parse_args())

if args['type'] not in ['shp', 'geojson', 'gpkg', 'tif']:
    print("file type should be shp, geojson, gpkg or tif")
else:
    if args['type'] == 'tif':
        bulk_import_geotiff(args)
    else:
        initialize_import(args)

