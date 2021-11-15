import os
import time
import sys
import yaml
import ast
import numpy as np
import pandas as pd
import geopandas as gp
from pandas.core.accessor import PandasDelegate
from collection import gdf_conversion
# Solutuion for the bug in pyrosm (functions)
# from bus_stop_preparation_merge import bus_stop_conversion, join_osm_pois_n_busstops


def poi_return_search_condition(name, var_dict):
    for key,value in var_dict.items():
        for v in value:
            if (name in v  or v in name) and name != '': 
                return key
            else:
                pass

    # Convert polygons to points and set origin geometry for all elements
def osm_obj2points(df, geom_column = "geom"):
    #df = df.set_crs(31468)

    df.at[df[geom_column].geom_type == "Point", 'origin_geometry'] = 'point'

    df.at[df[geom_column].geom_type == "MultiPolygon", 'origin_geometry'] = 'polygon'
    df.at[df[geom_column].geom_type == "MultiPolygon", 'geom'] = df['geom'].centroid
    df.at[df[geom_column].geom_type == "Polygon", 'origin_geometry'] = 'polygon'
    df.at[df[geom_column].geom_type == "Polygon", 'geom'] = df['geom'].centroid

    df.at[df[geom_column].geom_type == "LineString", 'origin_geometry'] = 'line'
    df.at[df[geom_column].geom_type == "MultiLineString", 'origin_geometry'] = 'line'

    return df

def pois_preparation(dataframe=None,filename=None, return_type=None,result_name="pois_preparation_result"):
    # (2 Options) POIs preparation from geojson imported from OSM (if you already have it)
    if dataframe is not None:
        df = dataframe
    elif filename:
        file = open(os.path.join(sys.path[0], 'data', filename + ".geojson"), encoding="utf-8")
        df = gp.read_file(file)
    else:
        print("Incorrect 'datatype' value!") 
        sys.exit()

    # Timer start
    print("Preparation started...")
    start_time = time.time()

    df["osm_id"] = df["id"]

    df = df.drop(columns={"lat", "lon", "version", "timestamp", "changeset"})

    df = df.rename(columns={"geometry": "geom", "addr:housenumber": "housenumber", "osm_type" : "origin_geometry"})

    df = df.assign(source = "osm")

    # Replace None values with empty strings in "name" column and dict in "tags" column
    # To be able to search within the values
    df["name"] = df["name"].fillna(value="")
    df["amenity"] = df["amenity"].fillna(value="")
    # Convert Null tags value to dict and str values to dict
    if dataframe is not None:
        df["tags"] = df["tags"].apply(lambda x: dict() if not x else ast.literal_eval(x))
    else:
        df["tags"] = df["tags"].apply(lambda x: dict() if not x else x)
    
    # variables for preparation
    # !!! Some columns could be not in the list 
    # REVISE it (probabaly check columns - if value from config is not there - create column)

    i_amenity = df.columns.get_loc("amenity")
    i_tourism = df.columns.get_loc("tourism")
    i_shop = df.columns.get_loc("shop")
    i_name = df.columns.get_loc("name")
    i_leisure = df.columns.get_loc("leisure")
    i_sport = df.columns.get_loc("sport")
    i_organic = df.columns.get_loc("organic")
    i_operator = df.columns.get_loc("operator")
    i_highway = df.columns.get_loc("highway")
    i_public_transport = df.columns.get_loc("public_transport")
    i_railway = df.columns.get_loc("railway")
    i_tags = df.columns.get_loc("tags")

    # Depending on zone "origin" can be not presented
    try:
        i_origin = df.columns.get_loc("origin")
    except:
        df = df.assign(origin = None)
        i_origin = df.columns.get_loc("origin")

    # Try to get location of subway column if it exists 
    try:
        i_subway = df.columns.get_loc("subway")
    except:
        df = df.assign(subway = None)
        i_subway = df.columns.get_loc("subway")


    # This section getting var from conf file (variables container)
    with open(os.path.join(sys.path[0] , 'pois_prep_conf.yaml'), encoding="utf-8") as m:
            config = yaml.safe_load(m)

    var = config['VARIABLES_SET']

    # Related to sport facilities
    sport_var_disc = var["sport"]["sport_var_disc"]
    leisure_var_add = var["sport"]["leisure_var_add"]
    leisure_var_disc = var["sport"]["leisure_var_disc"]
    # Related to Supermarkets
    health_food_var = var["health_food"]
    hypermarket_var = var["hypermarket"]
    no_end_consumer_store_var = var["no_end_consumer_store"]
    discount_supermarket_var = var["discount_supermarket"]
    # Related to Discount Gyms
    discount_gym_var = var["discount_gym"]
    # Related to Community Sport Centre
    # community_sport_centre_var = var["community_sport_centre"]

    # Convert polygons to points and set origin geometry for all elements
    df = osm_obj2points(df)

    # remove lines from 
    df = df[df.origin_geometry != 'line']

    df = df.reset_index(drop=True)

    # # Playgrounds
    df['amenity'] = np.where((df['leisure'] == 'playground') & (df['leisure'] != df['amenity']) & (df['amenity']), df['leisure'], df['amenity'])
    df['amenity'] = np.where((df['leisure'] == 'playground') & (df['amenity'] == ''), df['leisure'], df['amenity'])

    # Iterate through the rows
    for i in df.index:
        df_row = df.iloc[i]

        if df_row[i_tourism] and df_row[i_amenity] != "" and df_row[i_tourism] != df_row[i_amenity] and df_row[i_tourism] != "yes":
            df_row["amenity"] = df_row["tourism"]
            df = df.append(df_row)
        elif df_row[i_tourism] and df_row[i_amenity] == "" and df_row[i_tourism] != "yes":
            df.iat[i,i_amenity] = df.iat[i,i_tourism]
        
        # Sport pois from leisure and sport features
        if df_row[i_sport] or df_row[i_leisure] in leisure_var_add and df_row[i_leisure] not in leisure_var_disc and df_row[i_sport] not in sport_var_disc:
            df.iat[i,i_amenity] = "sport"
            if df_row[i_sport]:
                df.iat[i,i_tags]["sport"] = df_row[i_sport]
            elif df_row[i_leisure]:
                df.iat[i,i_tags]["leisure"] = df_row[i_leisure]
            continue
        elif df_row[i_leisure] not in leisure_var_add and df_row[i_leisure]:
            df.iat[i,i_tags]["leisure"] = df_row[i_leisure]

        # Gyms and discount gyms -> Fitness centers
        if (df_row[i_leisure] == "fitness_centre" or (df_row[i_leisure] == "sport_centre" and df_row[i_sport] == "fitness")) and (
            df_row[i_sport] in ["multi", "fitness"] or not df_row[i_sport]) and 'yoga' not in df_row[i_name].lower():
            operator = poi_return_search_condition(df_row[i_name].lower(), discount_gym_var)
            if operator:
                df.iat[i,i_operator] = operator
                df.iat[i,i_amenity] = "discount_gym"
            else:
                df.iat[i,i_amenity] = "gym"
            continue

        # Yoga centers check None change here
        if (df_row[i_sport] == "yoga" or "yoga" in df_row[i_name] or "Yoga" in df_row[i_name]) and not df_row[i_shop]:
            df.iat[i,i_amenity] = "yoga"
            continue    
            
        # Recclasify shops. Define convenience and clothes, others assign to amenity. If not rewrite amenity with shop value
        if df_row[i_shop] == "grocery" and df_row[i_amenity] == "":
            if df_row[i_organic] == "only":
                df.iat[i,i_amenity] = "organic"
                df.iat[i,i_tags]["organic"] = df_row[i_organic]
                continue
            elif df_row[i_origin]:
                df.iat[i,i_amenity] = "international_hypermarket"
                df.iat[i,i_tags]["origin"] = df_row[i_origin]
                continue
            else:
                df.iat[i,i_amenity] = "convenience"
                df.iat[i,i_shop] = None
                continue

        elif df_row[i_shop] == "fashion" and df_row[i_amenity] == "":
            df.iat[i,i_amenity] = "clothes"
            df.iat[i,i_shop] = None
            continue
        # Supermarkets recclassification
        elif df_row[i_shop] == "supermarket" and df_row[i_amenity] == "":
            operator = [poi_return_search_condition(df_row[i_name].lower(), health_food_var),
                        poi_return_search_condition(df_row[i_name].lower(), hypermarket_var),
                        poi_return_search_condition(df_row[i_name].lower(), no_end_consumer_store_var),
                        poi_return_search_condition(df_row[i_name].lower(), discount_supermarket_var)]
            if any(operator):
                for op in operator:
                    if op:
                        df.iat[i,i_operator] = op
                        o_ind = operator.index(op)
                        df.iat[i,i_amenity] = [cat for i, cat  in enumerate(["health_food", "hypermarket","no_end_consumer_store", "discount_supermarket"]) if i == o_ind][0]
                        continue
                    else:
                        pass
            else:
                if df_row[i_organic] == "only":
                    df.iat[i,i_amenity] = "organic"
                    df.iat[i,i_tags]["organic"] = df_row[i_organic]
                    continue
                elif df_row[i_origin]:
                    df.iat[i,i_amenity] = "international_hypermarket"
                    df.iat[i,i_tags]["origin"] = df_row[i_origin]
                    continue 
                else:
                    df.iat[i,i_amenity] = "supermarket"
                    continue
        elif df_row[i_shop] and df_row[i_shop] != "yes" and df_row[i_amenity] == "":
            df.iat[i,i_amenity] = df.iat[i,i_shop]
            df.iat[i,i_tags]["shop"] = df_row[i_shop]
            continue
         
        # Transport stops
        if df_row[i_highway] == "bus_stop" and df_row[i_name] != '':
            df.iat[i,i_amenity] = "bus_stop"
            continue
        elif df_row[i_public_transport] == "platform" and df_row[i_tags] and df_row[i_highway] != "bus_stop" and df_row[i_name] != '' and ("bus","yes") in df_row[i_tags].items():
            df.iat[i,i_amenity] = "bus_stop"
            df.iat[i,i_tags]["public_transport"] = df_row[i_public_transport]
            continue
        elif df_row[i_public_transport] == "stop_position" and isinstance(df_row[i_tags], dict) and ("tram","yes") in df_row[i_tags].items() and df_row[i_name] != '':
            df.iat[i,i_amenity] = "tram_stop"
            df.iat[i,i_tags]["public_transport"] = df_row[i_public_transport]
            continue
        elif df_row[i_railway] == "subway_entrance":
            df.iat[i,i_amenity] = "subway_entrance"
            df.iat[i,i_tags]["railway"] = df_row[i_railway]  
            continue
        elif df_row[i_railway] == "stop" and df_row[i_tags] and ("train","yes") in df_row[i_tags].items():
            df.iat[i,i_amenity] = "rail_station"
            df.iat[i,i_tags]["railway"] = df_row[i_railway]  
            continue
        elif df_row[i_highway]:
            df.iat[i,i_tags]["highway"] = df_row[i_highway]
        elif df_row[i_public_transport]:
            df.iat[i,i_tags]["public_transport"] = df_row[i_public_transport]
        elif df_row[i_railway]:
            df.iat[i,i_tags]["railway"] = df_row[i_railway]
        elif df_row[i_subway]:
            df.iat[i,i_tags]["subway"] = df_row[i_subway]        

    df = df.reset_index(drop=True)

    # # # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry='geom')
    df.crs = "EPSG:4326"

    # Filter subway entrances
    try: 
        df_sub_stations = df[(df["public_transport"] == "station") & (df["subway"] == "yes") & (df["railway"] != "proposed")]
        df_sub_stations = df_sub_stations[["name","geom", "id"]]
        df_sub_stations = df_sub_stations.to_crs(31468)
        df_sub_stations["geom"] = df_sub_stations["geom"].buffer(250)
        df_sub_stations = df_sub_stations.to_crs(4326) 

        df_sub_entrance = df[(df["amenity"] == "subway_entrance")]
        df_sub_entrance = df_sub_entrance[["name","geom", "id"]]
 
        df_snames = gp.overlay(df_sub_entrance, df_sub_stations, how='intersection') 
        df_snames = df_snames[["name_2", "id_1"]]
        df = (df_snames.set_index('id_1').rename(columns = {'name_2':'name'}).combine_first(df.set_index('id')))
    except:
        print("No subway stations for given area.")
        df = df.drop(columns={"id"})


    # Remove irrelevant columns
    df = df.drop(columns={"shop", "tourism", "leisure", "sport", "highway", "origin", "organic", "public_transport", "railway", "subway"})

    df = df.drop_duplicates(subset=['osm_id', 'amenity', 'name'], keep='first')

    # Timer finish
    print("Preparation took %s seconds ---" % (time.time() - start_time)) 
    df = gp.GeoDataFrame(df, geometry='geom')
    
    if filename and not result_name:
        result_name = filename + "prepared"

    return gdf_conversion(df,result_name,return_type)

#====================================================== Temporary functions to convert bus_stops lines to polygons=======================================#
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
def join_osm_pois_n_busstops(df_pois,  df_stops, df_pois_name=None, return_type=None, return_filename = 'pois_merged'):
    start_time = time.time()
    print("Merging process of pois and bus_stops has been started...")
    df = pd.concat([df_pois,df_stops],sort=False).reset_index(drop=True)
    df = df.replace({np.nan: None})
    print("Merging process of pois and bus_stops took %s seconds ---" % (time.time() - start_time))
    if df_pois_name:
        return_name = df_pois_name
    else:
        return_name = return_filename
    
    return gdf_conversion(df, return_name ,return_type)
