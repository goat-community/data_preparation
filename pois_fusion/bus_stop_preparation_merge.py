import geopandas as gp
import pandas as pd
import time
import sys
import os

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

def join_osm_pois_n_busstops(df_pois, df_stops,return_type="df", return_filename = 'pois_merged'):
    start_time = time.time()
    print("Merging process of pois and bus_stops has been started...")
    df = pd.concat([df_pois,df_stops],sort=False).reset_index(drop=True)
    print("Merging process of pois and bus_stops took %s seconds ---" % (time.time() - start_time))
    if return_type == "GeoJSON":
        print("Writing %s.geojson..." % return_filename)
        df.to_file(os.path.join(sys.path[0], "data" , return_filename + ".geojson"), driver="GeoJSON")
    else:
        return df

#df2geojson(bus_stop_conversion(df), "bus_stop_test")