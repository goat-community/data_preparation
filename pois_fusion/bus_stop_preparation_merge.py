import geopandas as gp
import pandas as pd
import time
import sys
import os
import numpy as np

# Temporary function to convert bus_stops lines to polygons
# Bug FIX 
def bus_stop_conversion(df):
    # Timer
    start_time = time.time()
    print("Bus stops conversion has been started...")

# Conversion lines back to polygons
    df.at[df['geometry'].geom_type == "MultiLineString", 'geometry'] = df['geometry'].convex_hull
    
    print("Conversion bus stops took %s seconds ---" % (time.time() - start_time))
    return df

# Function for Concatination of bus stops with pois 
# Return dataframe and dataframe name as TUPLE or record GeoJSON as void
# INPUT: DF(pois), DF(buses), DF(pois) name, ruturn_type ("df" or "GeoJSON"), return_filename for GeoJSON if not defined in "df_pois_name"
def join_osm_pois_n_busstops(df_pois,  df_stops, df_pois_name=None, return_type="df", return_filename = 'pois_merged'):
    start_time = time.time()
    print("Merging process of pois and bus_stops has been started...")
    df = pd.concat([df_pois,df_stops],sort=False).reset_index(drop=True)
    df = df.replace({np.nan: None})
    print("Merging process of pois and bus_stops took %s seconds ---" % (time.time() - start_time))
    if return_type == "GeoJSON":
        print("Writing %s.geojson..." % return_filename)
        if df_pois_name:
            df.to_file(os.path.join(sys.path[0], "data" , df_pois_name + ".geojson"), driver="GeoJSON")
        else:
            df.to_file(os.path.join(sys.path[0], "data" , return_filename + ".geojson"), driver="GeoJSON")
    else:
        return df, df_pois_name

#df2geojson(bus_stop_conversion(df), "bus_stop_test")