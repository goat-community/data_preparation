#%%

import os
import time
import sys
import yaml
import ast
import numpy as np
import pandas as pd
import geopandas as gp
from pandas.core.accessor import PandasDelegate
from pyrosm_collector import osm_collect_filter, gdf_conversion
# Solutuion for the bug in pyrosm (functions)
from bus_stop_preparation_merge import bus_stop_conversion, join_osm_pois_n_busstops


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

    #df = df.set_crs(4326)

    return df

def pois_preparation(dataframe=None,filename=None, return_type="PandasDF",result_name="pois_preparation_result"):
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

    df = df.drop(columns={"lat", "lon", "version", "timestamp", "changeset"})
    df = df.rename(columns={"geometry": "geom", "id ":"osm_id", "addr:housenumber": "housenumber", "osm_type" : "origin_geometry"})
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

    # remove lines from df
    df = df[df.origin_geometry != 'line']

    df = df.reset_index(drop=True)

    # # Playgrounds
    df['amenity'] = np.where((df['leisure'] == 'playground') & (df['leisure'] != df['amenity']) & (df['amenity']), df['leisure'], df['amenity'])
    df['amenity'] = np.where((df['leisure'] == 'playground') & (df['amenity'] == ''), df['leisure'], df['amenity'])

    # Iterate through the rows
    for i in df.index:
        df_row = df.iloc[i]

        if df_row[i_tourism] and df_row[i_amenity] != "" and df_row[i_tourism] != df_row[i_amenity]:
            df_row["amenity"] = df_row["tourism"]
            df = df.append(df_row)
        elif df_row[i_tourism] and df_row[i_amenity] == "":
            df.iat[i,i_amenity] = df.iat[i,i_tourism]
        
        # Sport pois from leisure and sport features
        # !!!!!!!!!!!!!Add sport and leisure into tags
        if df_row[i_sport] or df_row[i_leisure] in leisure_var_add and df_row[i_leisure] not in leisure_var_disc and df_row[i_sport] not in sport_var_disc:
            df.iat[i,i_amenity] = "sport"

            
            continue

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

        ##================================================================================================================##
            
        # Recclasify shops. Define convenience and clothes, others assign to amenity. If not rewrite amenity with shop value
        if df_row[i_shop] == "grocery":
            if df_row[i_organic] == "only":
                df.iat[i,i_amenity] = "organic"
                continue
            elif df_row[i_origin]:
                df.iat[i,i_amenity] = "international_hypermarket"
                continue
            else:
                df.iat[i,i_amenity] = "convenience"
                df.iat[i,i_shop] = None
                continue

        elif df_row[i_shop] == "fashion":
            df.iat[i,i_amenity] = "clothes"
            df.iat[i,i_shop] = None
            continue
        # Supermarkets recclassification
        elif df_row[i_shop] == "supermarket":
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
                    continue
                elif df_row[i_origin]:
                    df.iat[i,i_amenity] = "international_hypermarket"
                    continue 
                else:
                    df.iat[i,i_amenity] = "supermarket"
                    continue
        elif df_row[i_shop]:
            df.iat[i,i_amenity] = df.iat[i,i_shop]
            continue
         
        # Transport stops
        if df_row[i_highway] == "bus_stop" and df_row[i_name] != '':
            df.iat[i,i_amenity] = "bus_stop"
            continue
        elif df_row[i_public_transport] == "platform" and df_row[i_tags] and df_row[i_highway] != "bus_stop" and df_row[i_name] != '' and ("bus","yes") in df_row[i_tags].items():
            df.iat[i,i_amenity] = "bus_stop"
            continue
        elif df_row[i_public_transport] == "stop_position" and isinstance(df_row[i_tags], dict) and ("tram","yes") in df_row[i_tags].items() and df_row[i_name] != '':
            df.iat[i,i_amenity] = "tram_stop"
            continue
        elif df_row[i_railway] == "subway_entrance":
            df.iat[i,i_amenity] = "subway_entrance"
            continue
        elif df_row[i_railway] == "stop" and df_row[i_tags] and ("train","yes") in df_row[i_tags].items():
            df.iat[i,i_amenity] = "rail_station"
            continue
        
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


    
 
    # Timer finish
    print("Preparation took %s seconds ---" % (time.time() - start_time)) 
    df = gp.GeoDataFrame(df, geometry='geom')
    
    if filename and not result_name:
        result_name = filename + "prepared"
    

    return gdf_conversion(df,result_name,return_type)


# Tests
# 1 - Direct collection and preparation
# pois_collection = osm_collect_filter("pois")
# pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],bus_stop_conversion(osm_collect_filter("bus_stops")[0]))
# pois_preparation(dataframe=pois_bus_collection[0], return_type="GeoJSON",result_filename=pois_bus_collection[1])

# pois_preparation(dataframe=df, return_type="GeoJSON",result_filename='test')

# # 2 - Preparation from geoJSON
# join_osm_pois_n_busstops(osm_collect_filter("pois")[0],bus_stop_conversion(osm_collect_filter("bus_stops")[0]),return_type="GeoJSON", return_filename='pois_merged')
# pois_preparation(filename="pois_merged",return_type="GeoJSON", result_name="pois_from_geojson_test")

#%%















##=======================================================REWRITTEN=================================================================##

    #     # Convert polygons to points and set origin geometry for all elements
    #     if df_row[i_geom_idx].geom_type in ("Polygon", "MultiPolygon"):
    #         df.iat[i,i_geom_idx] = df.iat[i,i_geom_idx].centroid
    #         df.iat[i,i_orig_geom] = "polygon"
    #     elif df_row[i_geom_idx].geom_type in ("Point") or df_row[i_orig_geom] == "node":
    #         df.iat[i,i_orig_geom] = "point"
    #     else:
    #         df.iat[i,i_orig_geom] = "line"

    # Alternatives 
    
    # Tourism pois
    # temp_df = df.loc[(df['tourism'] != df['amenity']) & (df['amenity'] != '') & (df['tourism'] != None)]
    # df = pd.concat([df,temp_df],sort=False).reset_index(drop=True)
    # df['amenity'] = np.where((df['tourism'] != df['amenity']) & (df['amenity'] != '') & (df['tourism'] != None), df['tourism'], df['amenity'])
    # df['amenity'] = np.where((df['tourism'] != None) & (df['amenity'] == ''), df['tourism'], df['amenity'])

    # Sport pois
    # df['amenity'] = np.where((df['sport'] != None) | (df['leisure'].isin(leisure_var_add)) & (~df['leisure'].isin(leisure_var_disc)) & (~df['sport'].isin(sport_var_disc)), "sport", df['amenity'])

    # Gyms discounts
    # rewrite next loop


        # POIs preparation 
        # Playgrouds pois
        # if df_row[i_leisure] == "playground":
        #     df.iat[i,i_amenity] = df.iat[i,i_leisure]
        #     continue

        # # Tourism pois
        # if df_row[i_tourism] and df_row[i_amenity] and df_row[i_tourism] != df_row[i_amenity]:
        #     #collect data in separate df
        #     df = df.append(df.iloc[i])
        #     df.iat[i,i_amenity] = df.iat[i,i_tourism]
        #     continue
        # elif df_row[i_tourism] and not df_row[i_amenity]:
        #     df.iat[i,i_amenity] = df.iat[i,i_tourism]
        #     continue
        
        ##================================================================================================================##

# %%
