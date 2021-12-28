import os
import time
import sys
import ast
import yaml
from pathlib import Path
from difflib import SequenceMatcher
import numpy as np
import pandas as pd
import geopandas as gp
from pandas.core.accessor import PandasDelegate
from collection import gdf_conversion, osm_collect_filter, bus_stop_conversion, join_osm_pois_n_busstops
gp.options.use_pygeos = True

#================================== POIs preparation =============================================#
# Func asses probability of string similarity
def similar(a,b):
	return SequenceMatcher(None,a,b).ratio()

# Function search in config
def poi_return_search_condition(name, var_dict):
    for key,value in var_dict.items():
        for v in value:
            if (similar(name, v) > 0.8 or (name in v  or v in name)) and name != '': 
                return key
            else:
                pass

# Convert polygons to points and set origin geometry for all elements
def osm_obj2points(df, geom_column = "geom"):

    df.at[df[geom_column].geom_type == "Point", 'origin_geometry'] = 'point'

    df.at[df[geom_column].geom_type == "MultiPolygon", 'origin_geometry'] = 'polygon'
    df.at[df[geom_column].geom_type == "MultiPolygon", 'geom'] = df['geom'].centroid
    df.at[df[geom_column].geom_type == "Polygon", 'origin_geometry'] = 'polygon'
    df.at[df[geom_column].geom_type == "Polygon", 'geom'] = df['geom'].centroid

    df.at[df[geom_column].geom_type == "LineString", 'origin_geometry'] = 'line'
    df.at[df[geom_column].geom_type == "MultiLineString", 'origin_geometry'] = 'line'

    #df['geom'] = df['geom'].to_crs(4326)
    return df

def file2df(filename):
    name, extens = filename.split(".")
    if extens == "geojson":
        file = open(Path(__file__).parent/'data'/filename, encoding="utf-8")
        df = gp.read_file(file)
    elif extens == "gpkg":
        file =  Path(__file__).parent/'data'/filename
        df = gp.read_file(file)
    else:
        print("Extension of file %s currently doen not support with file2df() function." % filename)
        sys.exit()
    return df     

def pois_preparation(dataframe, config, return_type=None,result_name="pois_preparation_result"):

    df = dataframe

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


    # This section getting var from conf class (variables container)
    var = config.preparation
    # Related to sport facilities
    sport_var_disc = var["sport"]["sport_var_disc"]
    leisure_var_add = var["sport"]["leisure_var_add"]
    leisure_var_disc = var["sport"]["leisure_var_disc"]
    # Related to Supermarkets
    health_food_var = var["health_food"]
    hypermarket_var = var["hypermarket"]
    no_end_consumer_store_var = var["no_end_consumer_store"]
    discount_supermarket_var = var["discount_supermarket"]
    supermarket_var = var["supermarket"]
    chemist_var = var["chemist"]
    organic_var = var["organic"]
    # Banks
    bank_var = var["bank"]
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
                operator = poi_return_search_condition(df_row[i_name].lower(), organic_var)
                if operator:
                    df.iat[i,i_operator] = operator
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
                        poi_return_search_condition(df_row[i_name].lower(), discount_supermarket_var),
                        poi_return_search_condition(df_row[i_name].lower(), supermarket_var)]
            if any(operator):
                for op in operator:
                    if op:
                        df.iat[i,i_operator] = op
                        o_ind = operator.index(op)
                        df.iat[i,i_amenity] = [cat for i, cat  in enumerate(["health_food", "hypermarket","no_end_consumer_store", "discount_supermarket", "supermarket"]) if i == o_ind][0]
                        continue
                    else:
                        pass
            else:
                if df_row[i_organic] == "only":
                    df.iat[i,i_amenity] = "organic"
                    df.iat[i,i_tags]["organic"] = df_row[i_organic]
                    operator = poi_return_search_condition(df_row[i_name].lower(), organic_var)
                    if operator:
                        df.iat[i,i_operator] = operator
                    continue
                elif df_row[i_origin]:
                    df.iat[i,i_amenity] = "international_hypermarket"
                    df.iat[i,i_tags]["origin"] = df_row[i_origin]
                    continue 
                # rewrite next block - move search condition to config, write function for search
                elif "müller " in df_row[i_name].lower() or df_row[i_name].lower() == "müller":
                    df.iat[i,i_amenity] = "chemist"
                    df.iat[i,i_operator] = "müller"
                    continue
                elif "dm " in df_row[i_name].lower() or "dm-" in df_row[i_name].lower():
                    df.iat[i,i_amenity] = "chemist"
                    df.iat[i,i_operator] = "dm"
                    continue
                else:
                    df.iat[i,i_amenity] = "supermarket"
                    continue
        elif df_row[i_shop] == "chemist" and df_row[i_amenity] == "":
            operator = poi_return_search_condition(df_row[i_name].lower(), chemist_var)
            if operator:
                df.iat[i,i_operator] = operator
                df.iat[i,i_amenity] = "chemist"
                continue
            else:
                df.iat[i,i_amenity] = "chemist"
                continue
        elif df_row[i_shop] == "health_food" and df_row[i_amenity] == "":
            operator = poi_return_search_condition(df_row[i_name].lower(), health_food_var)
            if operator:
                df.iat[i,i_operator] = operator
                df.iat[i,i_amenity] = "health_food"
                continue
            else:
                df.iat[i,i_amenity] = "health_food"
                continue
        elif df_row[i_shop] and df_row[i_shop] != "yes" and df_row[i_amenity] == "":
            df.iat[i,i_amenity] = df.iat[i,i_shop]
            df.iat[i,i_tags]["shop"] = df_row[i_shop]
            continue

        # Additionaly define operator health_food amenities

        # Banks 
        if df_row[i_amenity] == "bank":
            operator = poi_return_search_condition(df_row[i_name].lower(), bank_var)
            if operator:
                df.iat[i,i_operator] = operator
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
    
    if config.pbf_data and not result_name:
        result_name = config.pbf_data + "prepared"

    return gdf_conversion(df,result_name,return_type)

# Preparation jedeschule table ->> conversion to fusable format
def school_categorization(df, config, result_name, return_type):
    var = config.preparation
    var_schools = var["schools"]

    schule = var_schools["schule"]
    grundschule = var_schools["grundschule"]
    hauptschule_mittelschule = var_schools["hauptschule_mittelschule"]
    exclude = var_schools["exclude"]

    df['name_1'] = df['name'].str.lower()
    df['name_1'] = df['name'].replace({np.nan: ""})

    for ex in exclude:
        temp_df = df.loc[df['name_1'].str.contains(ex, case=False)]
        m = ~df.id.isin(temp_df.id)
        df = df[m]
    df = df.drop(columns={"name_1"})
    df = df.reset_index()

    df.loc[df['school_t_1'].isin(schule), 'amenity'] = df['school_t_1'].str.lower()
    df_base = df[~df['amenity'].isnull()]

    df.loc[df['school_t_1'].isin(grundschule), 'amenity'] = 'grundschule'
    df_grund = df[df['school_t_1'].isin(grundschule)]

    df.loc[df['school_t_1'].isin(hauptschule_mittelschule), 'amenity'] = 'hauptschule_mittelschule'
    df_hauptmittel = df[df['school_t_1'].isin(hauptschule_mittelschule)]

    df_result = pd.concat([df_base,df_grund,df_hauptmittel],sort=False)

    # Should return 2 dataframes grundschule and mittel_haupt+schule
    return gdf_conversion(df_result,result_name,return_type)

def pois_preparation_set(config,config_buses,update=False,filename=None,return_type=None):
    df_res = pd.DataFrame()
    data_set = config.pbf_data

    for d in data_set:
        pois_collection = osm_collect_filter(config, d, update=update)
        pois_bus_collection = join_osm_pois_n_busstops(pois_collection[0],
                                                    bus_stop_conversion(osm_collect_filter(config_buses,d)[0]),
                                                    pois_collection[1])
        temp_df = pois_preparation(dataframe=pois_bus_collection[0], config=config, result_name=pois_bus_collection[1])[0]
        if data_set.index(d) == 0:
            df_res = temp_df
        else:
            df_res = pd.concat([df_res,temp_df],sort=False).reset_index(drop=True)

    return gdf_conversion(df_res, filename, return_type=return_type)


#================================ Landuse preparation ============================================#


def landuse_preparation(dataframe=None, filename=None, config=None, return_type=None, result_name="landuse_preparation_result"):
    """introduces the landuse_simplified column and classifies it according to the config input"""
    
    df = dataframe

    # Timer start
    print("Preparation started...")
    start_time = time.time()

    # Preprocessing: removing, renaming and reordering of columns
    df = df.drop(columns={"timestamp", "version", "changeset"})
    df = df.rename(columns={"geometry": "geom", "id": "osm_id", "osm_type": "origin_geometry"})
    df["landuse_simplified"] = None
    df = df[["landuse_simplified", "landuse", "tourism", "amenity", "leisure", "natural", "name",
             "tags", "osm_id", "origin_geometry", "geom"]]

    df = df.assign(source = "osm")

    # Fill landuse_simplified coulmn with values from the other columns
    custom_filter = config.pyrosm_filter()[0]

    if custom_filter is None:
        print("landuse_simplified can only be generated if the custom_filter of collection\
               is passed")
    else:
        for i in custom_filter.keys():
            df["landuse_simplified"] = df["landuse_simplified"].fillna(
                df[i].loc[df[i].isin(custom_filter[i])])

        # import landuse_simplified dict from config
        landuse_simplified_dict = config.preparation["landuse_simplified"]

        # Rename landuse_simplified by grouping
        # e.g. ["basin","reservoir","salt_pond","waters"] -> "water"
        for i in landuse_simplified_dict.keys():
            df["landuse_simplified"] = df["landuse_simplified"].replace(
                landuse_simplified_dict[i], i)

    if df.loc[~df['landuse_simplified'].isin(list(landuse_simplified_dict.keys()))].empty:
        print("All entries were classified in landuse_simplified")
    else:
        print("The following tags in the landuse_simplified column need to be added to the\
               landuse_simplified dict in config.yaml:")
        print(df.loc[~df['landuse_simplified'].isin(
            list(landuse_simplified_dict.keys()))])

    # remove lines from dataset
    df = df[df.origin_geometry != 'line']
    df = df.reset_index(drop=True)

    # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry="geom")
    df.crs = "EPSG:4326"
    df = df.reset_index(drop=True)

    # Timer finish
    print(f"Preparation took {time.time() - start_time} seconds ---")

    if filename and not result_name:
        result_name = filename + "prepared"

    return gdf_conversion(df, result_name, return_type)

    #================================ Buildings preparation ======================================#

def buildings_preparation(dataframe=None, filename=None, config=None ,return_type=None, result_name="buildings_preparation_result"):
    """introduces the landuse_simplified column and classifies it according to the config input"""

    df = dataframe

    # Timer start
    print("Preparation started...")
    start_time = time.time()
    # Preprocessing: removing, renaming, reordering and data type adjustments of columns
    df = df.drop(columns={"timestamp", "version", "changeset"})
    df = df.rename(columns={"geometry": "geom", "id": "osm_id", "osm_type": "origin_geometry",\
                            "addr:street": "street", "addr:housenumber": "housenumber",\
                            "building:levels": "building_levels", "roof:levels": "roof_levels"})
    df["residential_status"] = None
    df["area"]               = None
    df = df[["osm_id", "building", "amenity", "leisure", "residential_status", "street", "housenumber",
             "area", "building_levels", "roof_levels", "origin_geometry","geom"]]
    df["building_levels"] = pd.to_numeric(df["building_levels"], downcast="float")
    df["roof_levels"] = pd.to_numeric(df["roof_levels"], downcast="float")
    df = df.assign(source = "osm")

    # classifying residential_status in 'with_residents', 'potential_residents', 'no_residents'
    df.loc[((df.building.str.contains("yes")) & (df.amenity.isnull()) & (df.amenity.isnull())), "residential_status"] = "potential_residents"
    df.loc[df.building.isin(config.variable_container["building_types_residential"]), "residential_status"] = "with_residents"
    df.residential_status.fillna("no_residents", inplace=True)

    # remove lines from dataset
    df = df[df.origin_geometry != 'line']
    df = df.reset_index(drop=True)

    # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry="geom")
    df.crs = "EPSG:4326"
    df = df.reset_index(drop=True)

    # calculating the areas of the building outlines in m^2
    df = df.to_crs({'init': 'epsg:3857'})
    df['area'] = df['geom'].area.round(2)
    df = df[df.area != 0]
    df = df.to_crs({'init': 'epsg:4326'})

    # Timer finish
    print(f"Preparation took {time.time() - start_time} seconds ---")


    if filename and not result_name:
        result_name = filename + "prepared"

    return gdf_conversion(df, result_name, return_type)