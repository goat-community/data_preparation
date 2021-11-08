

# %%

import geopandas as gp
import os
import pandas as pd
import time
import sys
from pandas.core.accessor import PandasDelegate
import yaml
import ast
import numpy as np
from pyrosm_collector import osm_collect_filter, gdf_conversion
gp.options.use_pygeos = True

def landuse_return_search_condition(name, var_dict):
    for key,value in var_dict.items():
        for v in value:
            if name in v and name != '': 
                return key
            else:
                pass

def landuse_preparation(dataframe=None,filename=None, return_type="df",result_filename="landuse_preparation_result"):

    # (2 Options) landuse preparation from geojson imported from OSM (if you already have it)
    if dataframe is not None:
        df = dataframe
    elif filename:
        file = open(os.path.join(os.path.abspath(os.getcwd()), 'data', filename + ".geojson"), encoding="utf-8")
        df   = gp.read_file(file)
    else:
        print("Incorrect 'datatype' value!") 
        sys.exit()

    # Timer start
    print("Preparation started...")
    start_time = time.time()

    # Preprocessing: removing, renaming and reordering of columns
    df = df.drop(columns={"timestamp", "version", "changeset"})
    df = df.rename(columns={"geometry": "geom", "id":"osm_id", "osm_type" : "origin_geometry"})
    df["landuse_simplified"] = None
    df = df[["landuse_simplified", "landuse", "tourism", "amenity", "leisure", "natural", "name", "tags", 
            "osm_id", "origin_geometry", "geom"]]




    # Fill landuse_simplified coulmn with values from the other columns 

    df["landuse_simplified"] = df["landuse_simplified"].fillna(df['landuse'].loc[df["landuse"].isin(["basin", "reservoir", "salt_pond", "water", "waters", 
                                                                                        "allotments", "aquaculture", "farmland", "farmyard", 
                                                                                        "greenhouse_horticulture", "plant_nursery", "vineyard", 
                                                                                        "forest", "grass", "meadow", "village_green", 
                                                                                        "recreation_ground", "cemetery", "residential", "garages",
                                                                                        "commercial", "retail", "religious", "industrial", "landfill",
                                                                                        "quarry", "railway", "highway", "military", "garden", 
                                                                                        "national_park", "nature_reserve", "park", "grave_yard", 
                                                                                        "orchard", "fallow", "plantation"])])
    df["landuse_simplified"] = df["landuse_simplified"].fillna(df['natural'].loc[df["natural"].isin(["forest","grass","meadow","green_area", 
                                                                                                    "wetland", "scrub", "wood", "grassland", 
                                                                                                    "heath", "water"])])
    df["landuse_simplified"] = df["landuse_simplified"].fillna(df['amenity'].loc[df["amenity"].isin(["parking", "school", "hospital", "grave_yard"])])
    df["landuse_simplified"] = df["landuse_simplified"].fillna(df['leisure'].loc[df["leisure"].isin(["adult_gaming_centre", "amusement_arcade", "beach_resort", 
                                                                                                    "bandstand", "dance", "dog_park", "escape_game", "fitness_centre",
                                                                                                    "garden", "horse_riding", "marina", "miniature_golf", 
                                                                                                    "nature_reserve", "park", "pitch", "playground", "sports_centre", 
                                                                                                    "stadium", "swimming_pool", "track", "water_park", "leisure"])])

    # Rename landuse_simplified by grouping e.g. ["basin","reservoir","salt_pond","waters"] -> "water"

    df["landuse_simplified"] = df["landuse_simplified"].replace(["basin","reservoir","salt_pond","waters", "swimming_pool"], "water")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["allotments", "aquaculture", "fallow", "farmland", 
                                                                "farmyard", "greenhouse_horticulture", "orchard", 
                                                                "pasture", "plant_nursery", "plantation", "vineyard"], "agriculture")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["forest","grass","meadow","green_area", "wetland", "scrub", "wood",
                                                                "grassland", "heath"], "nature")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["adult_gaming_centre", "amusement_arcade", "beach_resort", "bandstand", 
                                                                "dance", "dog_park", "escape_game", "fitness_centre", "garden", 
                                                                "horse_riding", "marina", "miniature_golf", "nature_reserve", "park", 
                                                                "pitch", "playground", "sports_centre", "stadium",  "track", 
                                                                "water_park", "national_park", "village_green", "recreation_ground"], 
                                                                "leisure")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["grave_yard"], "cemetery")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["garages"], "residential")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["retail"], "commercial")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["school","university","hospital","college","churchyard","religious"], 
                                                                "community")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["landfill","quarry"], "industrial")
    df["landuse_simplified"] = df["landuse_simplified"].replace(["highway","parking","railway", "parking"], "transportation")


    # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry="geom")

    df = df.reset_index(drop=True)

    # Timer finish
    print("Preparation took %s seconds ---" % (time.time() - start_time))  
    print(df)
    print(df.columns)

    return gdf_conversion(df,result_filename,return_type)

#tests 




#osm_collect_filter('landuse', 'PandasDF', update = True).to_file(os.path.join(os.path.join(os.path.abspath(os.getcwd()), "data") , "landuse.geojson"), driver="GeoJSON")

# dataframe = None
# filename = 'landuse'
# dataframe = osm_collect_filter('landuse', 'PandasDF', update = True)


#landuse_preparation(filename="landuse", return_type="PandasDF",result_filename='landuse_preparation_result')

#landuse_preparation(filename=landuse.geojson)

# %%

# %%

# checking data to analyse wrong classifications
# df.loc[(df['landuse_simplified']=='water') & (df['natural'].isin(['basin','reservoir','salt_pond','waters', 'swimming_pool', 'water']))]
# df.loc[~df['landuse_simplified'].isin(['water', 'agriculture', 'nature', 'leisure', 'cemetery', 'residential', 'community','commercial', 'industrial', 'transportation', 'military'])]

# pd.DataFrame(df)
