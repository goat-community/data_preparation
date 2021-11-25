"""This module contains all classes and functions for the OSM data collection
with the Pyrosm package."""
#%%
import os
import sys
import time
import yaml

#from pygeos import GEOSException
from pyrosm import get_data, OSM
import geopandas as gpd
import pandas as pd
import numpy as np
from osm_dict import OSM_tags


class Config:
    def __init__(self,name):
        config = yaml.safe_load(open(os.path.join(sys.path[0], 'config.yaml'),\
                 encoding="utf-8"))
        var = config['VARIABLES_SET']
        self.name = name
        self.pbf_data = var['region_pbf']
        self.collection = var[name]['collection']
        self.preparation = var[name]['preparation']
        self.fusion = var[name]['fusion']

    def pyrosm_filter(self):
        """creates a filter based on user input in the config to filter the OSM import"""
        coll = self.collection

        # check if input osm_tags, osm_features are valid and print non valid ones
        for i in coll["osm_tags"].keys():
            if i not in OSM_tags.keys():
                print(f"{i} is not a valid osm_feature")
        for i in [item for sublist in coll["osm_tags"].values() for item in sublist]:
            if i not in [item for sublist in OSM_tags.values() for item in sublist] + ['all']:
                print(f"{i} is not a valid osm_feature")

        # loop collects all tags of a feature from osm_feature_tags_dict.py if "all" in config file
        temp = {}
        for key, values in coll["osm_tags"].items():
            for value in values:
                if value == 'all':
                    temp = temp | {key:OSM_tags[key]}

        po_filter = coll["osm_tags"] | temp,None,"keep",list(coll["osm_tags"].keys())+\
                    coll["additional_columns"],coll["points"], coll["lines"],coll["polygons"], None

        return po_filter

    def fusion_key_set(self, typ):
        fus = self.fusion
        key_set = fus["fusion_data"]['source'][typ].keys()
        return key_set

    def fusion_set(self,typ,key):
        fus = self.fusion["fusion_data"]["source"][typ][key]
        fus_set = fus["amenity_replace"],fus["amenity_brand_replace"],fus["columns2rename"],\
                  fus["columns2drop"]
        return fus_set

def classify_osm_tags(name):
    """helper function to help assign osm tags to their corresponding feature"""
    # import dict from conf_yaml
    config = yaml.safe_load(open(os.path.join(sys.path[0], 'config.yaml'),\
             encoding="utf-8"))
    var = config['VARIABLES_SET']
    temp = {}
    for key in var[name]['collection']['osm_tags'].keys():
        if key == 'not_sure':
            for i in var[name]['collection']['osm_tags'][key]:
                for keys, values in OSM_tags.items():
                    for value in values:
                        if i == value:
                            if keys in temp.keys():
                                if isinstance(temp[keys], str) is True:
                                    temp[keys] = [temp[keys], i]
                                else:
                                    temp[keys].append(i)
                            else:
                                temp = temp | {keys:i}
                        elif "no_valid_osm_tag" not in temp.keys() and i not in temp.values():
                            temp = temp | {"no_valid_osm_tag":i}
                        elif "no_valid_osm_tag" in temp.keys() and i not in temp["no_valid_osm_tag"]:
                            if isinstance(temp["no_valid_osm_tag"], str) is True:
                                temp["no_valid_osm_tag"] = [temp["no_valid_osm_tag"], i]
                            else:
                                temp["no_valid_osm_tag"].append(i)
            print(temp)
            sys.exit()

# Function for creation backupfiles
# Use it as return result for data preparation functions
def gdf_conversion(gdf, name=None, return_type=None):
    """can convert a GeoGataFrame(gdf) into GeoJSON or GPKG, but always returns the gdf and name"""
    if return_type == "GeoJSON":
        print(f"Writing down the geojson file {name + '.geojson'} ...")
        start_time = time.time()
        gdf.to_file(os.path.join(sys.path[0],"data", name + ".geojson"), driver=return_type)
        print(f"Writing file {time.time() - start_time} seconds ---")
        print(f"GeoJSON {name + '.geojson'} was written.")
        return gdf, name
    elif return_type == "GPKG":
        print(f"Writing down the geojson file {name + '.gpkg'} ...")
        start_time = time.time()
        gdf.to_file(os.path.join(sys.path[0],"data", name + ".gpkg"), driver=return_type)
        print(f"Writing file {time.time() - start_time} seconds ---")
        print(f"GeoPackage {name + '.gpkg'} was written.")
        return gdf, name
    else:
        return gdf, name

# Function for collection data from OSM dbf and conversion to GeoJSON
# Could be extended with varios type of searches and filters
# name - should be referenced to set from YAML file (e.g. "pois")
# pbf_region=None - when "None" data comes from YAML file, when specified - from it
# driver=None default variables driver could be (driver from "Fiona" drivers, e.g "GeoJSON", "GPKG")
# !! Notice if driver is specified it creates GeoJSON, but function still returns DF.
# !! If it is not specified func returns only DF
# update=True parameter to download fresh data from OSM

def osm_collect_filter(config, pbf_region=None, driver=None, update=False):
    # Timer
    print(f"Collection and filtering {config.name} started...")
    start_time = time.time()

    # get region name desired pois types from yaml settings
    if pbf_region:
        pbf_data = pbf_region
    else:
        pbf_data = config.pbf_data

    # Get defined data from Geofabrik
    fp = get_data(pbf_data, update=update)
    osm = OSM(fp)

    # Create filter class with given parameters and create filter with class method
    custom_filter = config.pyrosm_filter()

    # Get Data from OSM with given filter ###custom_filter.tags_as_columns
    # Additional filters can be applied (e.g. keep_nodes=False, keep_ways=True,keep_relations=True)
    df = osm.get_data_by_custom_criteria(*custom_filter)

    print(f"Collection and filtering took {time.time() - start_time} seconds ---")

    return_name = config.name + '_' + pbf_data

    # Return type if driver -> 'GeoJSON' write geojson file if driver -> 'PandasDF' return PandasDF
    return gdf_conversion(df, return_name ,driver)

#==================== Temporary functions to convert bus_stops lines to polygons==================#
# Bug FIX (Relazted ot POIs)
def bus_stop_conversion(df):
    # Timer
    start_time = time.time()
    print("Bus stops conversion has been started...")

# Conversion lines back to polygons
    df.at[df['geometry'].geom_type == "MultiLineString", 'geometry'] = df['geometry'].convex_hull

    print(f"Conversion bus stops took {time.time() - start_time} seconds ---")
    return df

# Function for Concatination of bus stops with pois
# Return dataframe and dataframe name as TUPLE or record GeoJSON as void
# INPUT: DF(pois), DF(buses), DF(pois) name, ruturn_type ("df" or "GeoJSON"), return_filename for GeoJSON if not defined in "df_pois_name"
def join_osm_pois_n_busstops(df_pois,  df_stops, df_pois_name=None, return_type=None, return_filename = 'pois_merged'):
    start_time = time.time()
    print("Merging process of pois and bus_stops has been started...")
    df = pd.concat([df_pois,df_stops],sort=False).reset_index(drop=True)
    df = df.replace({np.nan: None})
    print(f"Merging process of pois and bus_stops took {time.time() - start_time} seconds ---")
    if df_pois_name:
        return_name = df_pois_name
    else:
        return_name = return_filename

    return gdf_conversion(df, return_name ,return_type)

#======================================== Buildings collection ===================================#

def osm_collect_buildings(name='buildings', driver=None):
    #import data from pois_coll_conf.yaml
    with open(os.path.join(sys.path[0] , 'config.yaml'), encoding="utf-8") as m:
        config = yaml.safe_load(m)

    var = config['VARIABLES_SET']

    # get region name desired pois types from yaml settings
    pbf_data = var['region_pbf']
    # query_values = var[name]['query_values']
    # filter = var[name]['tags']['filter']
    # additional = var[name]['tags']['additional']
    # point = var[name]['points']
    # polygon = var[name]['polygons']
    # line = var[name]['lines']

    # Get defined data from Geofabrik
    fp = get_data(pbf_data)
    osm = OSM(fp)

    buildings = osm.get_buildings()

    return gdf_conversion(buildings, name, return_type=driver)

# %%
