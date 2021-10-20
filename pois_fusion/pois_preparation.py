import geopandas as gp
import os
import pandas
import time
import geojson
import sys
from pandas.core.accessor import PandasDelegate
import yaml
#from pyrosm_collector import osm_collect

# Fuction saves prepared DFrame to GeoJSON
# Need to add some variables in properties
def df2geojson(df, name):
    features = []
    insert_features = lambda X: features.append(
            geojson.Feature(geometry=X["geom"],
                            properties=dict(name=X["name"],
                                            amenity=X["amenity"],
                                            tags=X["tags"])))
    df.apply(insert_features, axis=1)
    with open(name +'.geojson', 'w', encoding='utf8') as fp:
        geojson.dump(geojson.FeatureCollection(features), fp, sort_keys=True, ensure_ascii=False)


def poi_return_search_condition(name, var_dict):
    for key,value in var_dict.items():
        for v in value:
            if name in v and name != '': 
                return key
            else:
                pass

# POIs preparation from geojson imported from OSM 
filename = "pois.geojson"
file = open(os.path.join("pois_fusion", filename), encoding="utf-8")
df = gp.read_file(file)

# Start with OSM data collection -- skip GeoJSON file step 
# df = osm_collect()

# Timer start
print("Converstion started...")
start_time = time.time()

df = df.drop(columns={"lat", "lon", "version", "timestamp", "changeset"})
df = df.rename(columns={"geometry": "geom", "id ":"osm_id", "addr:housenumber": "housenumber", "osm_type" : "origin_geometry"})
#df = df.assign(poi_type = None)

# Replace None values with empty strings in name column
df["name"] = df["name"].fillna(value="")

# variables for preparation
# !!! Some columns could be not in the list 
# REVISE it (probabaly check columns - if value from config is not there - create column)
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
i_operator = df.columns.get_loc("operator")
i_highway = df.columns.get_loc("highway")
i_public_transport = df.columns.get_loc("public_transport")
i_railway = df.columns.get_loc("railway")
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
hypermarket_var = var["hypermarket"]
no_end_consumer_store_var = var["no_end_consumer_store"]
discount_supermarket_var = var["discount_supermarket"]
# Related to Discount Gyms
discount_gym_var = var["discount_gym"]
# Related to Community Sport Centre
community_sport_centre_var = var["community_sport_centre"]

# Iterate through the rows
for i in df.index:
    # Convert polygons to points and set origin geometry
    if df.iat[i,i_geom_idx].geom_type in ["Polygon", "MultiPolygon"]:
        df.iat[i,i_geom_idx] = df.iat[i,i_geom_idx].centroid
        df.iat[i,i_orig_geom] = "polygon"
    if df.iat[i,i_orig_geom] == "node":
        df.iat[i,i_orig_geom] = "point"

    # Remove lines as not necessary?
    if df.iat[i,i_geom_idx].geom_type in ["LineString"]:
        df = df.drop(df.iloc[i])

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
    
    # Gyms anddiscount gyms -> Fitness centers
    if (df.iat[i,i_leisure] == "fitness_centre" or (df.iat[i,i_leisure] == "sport_centre" and df.iat[i,i_sport] == "fitness")) and (
        df.iat[i,i_sport] in ["multi", "fitness"] or not df.iat[i,i_sport]) and 'yoga' not in df.iat[i,i_name].lower():
        operator = poi_return_search_condition(df.iat[i,i_name].lower(), discount_gym_var)
        if operator:
            df.iat[i,i_operator] = operator
            df.iat[i,i_amenity] = "discount_gym"
        else:
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
        
    # Recclasify shops. Define convenience and clothes, others assign to amenity. If not rewrite amenity with shop value
    if df.iat[i,i_shop] == "grocery":
        df.iat[i,i_amenity] = "convenience"
        df.iat[i,i_shop] = None
    elif df.iat[i,i_shop] == "fashion":
        df.iat[i,i_amenity] = "clothes"
        df.iat[i,i_shop] = None
    elif df.iat[i,i_shop]:
        df.iat[i,i_amenity] = df.iat[i,i_shop]
    
    # Supermarkets recclassification
    if df.iat[i,i_amenity] == "supermarket":
        operator = [poi_return_search_condition(df.iat[i,i_name].lower(), health_food_var),
                    poi_return_search_condition(df.iat[i,i_name].lower(), hypermarket_var),
                    poi_return_search_condition(df.iat[i,i_name].lower(), no_end_consumer_store_var),
                    poi_return_search_condition(df.iat[i,i_name].lower(), discount_supermarket_var)]
        for op in operator:
            if op:
                df.iat[i,i_operator] = op
                o_ind = operator.index(op)
                df.iat[i,i_amenity] = [cat for i, cat  in enumerate(["health_food", "hypermarket","no_end_consumer_store", "discount_supermarket"]) if i == o_ind][0]
            else:
                pass
    
    # Organic as amenity for shops and supermarkets
    if df.iat[i,i_organic] == "only" and (df.iat[i,i_amenity] == "supermarket" or df.iat[i,i_amenity] == "convenience"):
        df.iat[i,i_amenity] = "organic"

    # International supermarkets
    if df.iat[i,i_origin] and (df.iat[i,i_amenity] == "supermarket" or df.iat[i,i_amenity] == "convenience"):
        df.iat[i,i_amenity] = "international_hypermarket"

    # Bycicle rental ??? not relevant for whole Bayern

    # Transport stops
    if df.iat[i,i_highway] == "bus_stop" and df.iat[i,i_name] != '':
        df.iat[i,i_amenity] = "bus_stop"
    if df.iat[i,i_public_transport] == "platform" and df.iat[i,i_tags] and df.iat[i,i_highway] != "bus_stop" and df.iat[i,i_name] != '' and ("bus","yes") in df.iat[i,i_tags].items():
        df.iat[i,i_amenity] = "bus_stop"
    if df.iat[i,i_public_transport] == "stop_position" and df.iat[i,i_tags] and ("tram","yes") in df.iat[i,i_tags].items() and df.iat[i,i_name] != '':
        df.iat[i,i_amenity] = "tram_stop"
    if df.iat[i,i_railway] == "subway_entrance":
        df.iat[i,i_amenity] = "subway_entrance"
    if df.iat[i,i_railway] == "stop" and df.iat[i,i_tags] and ("train","yes") in df.iat[i,i_tags].items():
        df.iat[i,i_amenity] = "rail_station"
    
    ##!!!!!#### SUBWAY_ENTRANCE - name ?????

    

    


##--------------------------------------------------------------------------------------------------------------------------------##
    # Sport_centers and Waterparks

    # if not df.iat[i,i_amenity] and df.iat[i,i_sport] and ((df.iat[i,i_leisure] == "sport_centre" and poi_return_search_condition(df.iat[i,i_name].lower(), community_sport_centre_var) or
    # (df.iat[i,i_leisure] == "recreation_ground" and poi_return_search_condition(df.iat[i,i_name].lower(), community_sport_centre_var)))):
    #     print(df.iloc[i])


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
df2geojson(df, "result_test")