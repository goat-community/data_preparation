#%%

import geopandas as gp
import os
import pandas
import time
import geojson
import sys
import yaml
from pyrosm_collector import osm_collect

# Fuction saves prepared DFrame to GeoJSON
# Need to add some variables in properties
def df2geojson(df):
    features = []
    insert_features = lambda X: features.append(
            geojson.Feature(geometry=X["geom"],
                            properties=dict(name=X["name"],
                                            amenity=X["amenity"],
                                            tags=X["tags"])))
    df.apply(insert_features, axis=1)
    with open('map1.geojson', 'w', encoding='utf8') as fp:
        geojson.dump(geojson.FeatureCollection(features), fp, sort_keys=True, ensure_ascii=False)


def poi_return_search_condition(name, var_dict):
    for key,value in var_dict.items():
        for v in value:
            if name in v: 
                return key
                break
            else:
                pass

# POIs preparation from geojson imported from OSM 
filename = "pois.geojson"
file = open(os.path.join("pois_fusion", filename), encoding="utf-8")
df = gp.read_file(file)

# Start with OSM data collection -- skip GeoJSON file step 
# df = osm_collect()

print("Converstion started...")
start_time = time.time()

df = df.drop(columns={"lat", "lon", "version", "timestamp", "changeset"})
df = df.rename(columns={"geometry": "geom", "id ":"osm_id", "addr:housenumber": "housenumber", "osm_type" : "origin_geometry"})
#df = df.assign(poi_type = None)

# Replace None values with empty strings in name column
df["name"] = df["name"].fillna(value="")

# variables for preparation
i_geom_idx = df.columns.get_loc("geom")
i_orig_geom = df.columns.get_loc("origin_geometry")
i_amenity = df.columns.get_loc("amenity")
i_shop = df.columns.get_loc("shop")
i_tourism = df.columns.get_loc("tourism")
i_name = df.columns.get_loc("name")
i_leisure = df.columns.get_loc("leisure")
i_sport = df.columns.get_loc("sport")
i_organic = df.columns.get_loc("organic")
i_origin = df.columns.get_loc("origin")

i_tags = df.columns.get_loc("tags")

# This section getting var from conf file (variables container)
with open(os.path.join(sys.path[0] , 'pois_prep_conf.yaml')) as m:
        config = yaml.safe_load(m)

var = config['VARIABLES_SET']

# Related to sport facilities
sport_var_disc = var["sport"]["sport_var_disc"]
leisure_var_add = var["sport"]["leisure_var_add"]
leisure_var_disc = var["sport"]["leisure_var_disc"]
# Related to Supermarkets
health_food_var = var["health_food"]

# Iterate through the rows
for i in df.index:
    # Convert polygons to points and set origin geometry
    if df.iat[i,i_geom_idx].geom_type in ["Polygon", "MultiPolygon"]:
        df.iat[i,i_geom_idx] = df.iat[i,i_geom_idx].centroid
        df.iat[i,i_orig_geom] = "polygon"
    if df.iat[i,i_orig_geom] == "node":
        df.iat[i,i_orig_geom] = "point"

    # POIs preparation 
    # Playgrouds pois
    if df.iat[i,i_leisure] == "playground" and df.iat[i,i_amenity] and df.iat[i,i_leisure] != df.iat[i,i_amenity]:
        df = df.append(df.iloc[i])
        df.iat[i,i_amenity] = df.iat[i,i_leisure]
    elif df.iat[i,i_leisure] == "playground" and not df.iat[i,i_amenity]:
        df.iat[i,i_amenity] = df.iat[i,i_leisure]

    # Tourism pois
    if df.iat[i,i_tourism] and df.iat[i,i_amenity] and df.iat[i,i_tourism] != df.iat[i,i_amenity]:
        df = df.append(df.iloc[i])
        df.iat[i,i_amenity] = df.iat[i,i_tourism]
    elif df.iat[i,i_tourism] and not df.iat[i,i_amenity]:
        df.iat[i,i_amenity] = df.iat[i,i_tourism]
    
    # Sport pois from leisure and sport features
    if df.iat[i,i_sport] or df.iat[i,i_leisure] in leisure_var_add and df.iat[i,i_leisure] not in leisure_var_disc and df.iat[i,i_sport] not in sport_var_disc:
        df.iat[i,i_amenity] = "sport"
        if isinstance(df.iat[i,i_tags], dict):
            df.iat[i,i_tags]["sport"] = df.iat[i,i_sport]
            df.iat[i,i_tags]["leisure"] = df.iat[i,i_leisure]
        else: 
            df.iat[i,i_tags] = [{"sport": df.iat[i,i_sport], "leisure": df.iat[i,i_leisure]}]
    
    # Gyms -> Fitness centers
    if (df.iat[i,i_leisure] == "fitness_centre" or (df.iat[i,i_leisure] == "sport_centre" and df.iat[i,i_sport] == "fitness")) and (
        df.iat[i,i_sport] in ["multi", "fitness"] or df.iat[i,i_sport] is None) and 'yoga' not in df.iat[i,i_name]:
        df.iat[i,i_amenity] = "gym"
        if isinstance(df.iat[i,i_tags], dict):
            df.iat[i,i_tags]["sport"] = df.iat[i,i_sport]
            df.iat[i,i_tags]["leisure"] = df.iat[i,i_leisure]
        else: 
            df.iat[i,i_tags] = [{"sport": df.iat[i,i_sport], "leisure": df.iat[i,i_leisure]}]
        
    # Yoga centers check None change here
    if (df.iat[i,i_sport] == "yoga" or "yoga" in df.iat[i,i_name] or "Yoga" in df.iat[i,i_name]) and not df.iat[i,i_shop]:
        df.iat[i,i_amenity] = "yoga"
        if isinstance(df.iat[i,i_tags], dict):
            df.iat[i,i_tags]["sport"] = df.iat[i,i_sport]
            df.iat[i,i_tags]["leisure"] = df.iat[i,i_leisure]
        if df.iat[i,i_tags] is None: 
            df.iat[i,i_tags] = [{"sport": df.iat[i,i_sport], "leisure": df.iat[i,i_leisure]}]
        
    # Recclasify shops. Define convenience and clothes, others assign to amenity
    if df.iat[i,i_shop] == "grocery":
        df.iat[i,i_amenity] = "convenience"
        df.iat[i,i_shop] = None
    elif df.iat[i,i_shop] == "fashion":
        df.iat[i,i_amenity] = "clothes"
        df.iat[i,i_shop] = None
    else:
        df.iat[i,i_amenity] = df.iat[i,i_shop]
    
    # Supermarkets recclassification


    # Refinement discount gyms


    # Organic as amenity for shops and supermarkets
    if df.iat[i,i_organic] == "only" and (df.iat[i,i_amenity] == "supermarket" or df.iat[i,i_amenity] == "convenience"):
        df.iat[i,i_amenity] = "organic"

    # International supermarkets
    if df.iat[i,i_origin] and (df.iat[i,i_amenity] == "supermarket" or df.iat[i,i_amenity] == "convenience"):
        df.iat[i,i_amenity] = "international_hypermarket"




##--------------------------------------------------------------------------------------------------------------------------------##

    # if not df.iloc[i,i_amenity] and df.iloc[i,i_leisure]:
    #     df.iloc[i,i_amenity] = df.iloc[i,i_leisure]
    


    # if not df.iloc[i,i_amenity] and df.iloc[i,i_shop]:
    #     df.iloc[i,i_amenity] = df.iloc[i,i_shop]



    # if df.iloc[i,i_amenity] != df.iloc[i,i_tourism] and df.iloc[i,i_tourism]: 
    #     count += 1
    #     print(type(df.iloc[i]))
    #     print(df.iloc[i,i_amenity], df.iloc[i,i_tourism], df.iloc[i,i_name])
    
    # if df.iloc[i,i_amenity] and not df.iloc[i,i_shop]:
    #     df.iloc[i,i_elem_type] = df.iloc[i,i_amenity]
    # elif not df.iloc[i,i_amenity] and df.iloc[i,i_shop]:
    #     df.iloc[i,i_elem_type] = df.iloc[i,i_shop]

##-------------------------------------------------------------------------------------------------------------------------------##
  
print(df)
print(df.columns)
print("Preparation took %s seconds ---" % (time.time() - start_time))
df2geojson(df)
