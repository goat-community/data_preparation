#%%

import yaml
import os
import sys
import time
from pygeos import GEOSException
from pyrosm import get_data, OSM
import geopandas as gpd
import pandas as pd
from osm_feature_tags_dict import OSM_tags

# pyrosm filter class. Input for it: type of search 'pois'(currently only pois and landuse), tags from config , dictionary of osm tags
# variables of class are 'filter' - list of feature:tags which are relevant to scrap and 'tags_as_columns'- list of tags which will be 
# converted to columns in geopandas DataFrame
class PyrOSM_Filter:
    def __init__(self, name):
        # import dict from conf_yaml
        with open(os.path.join(sys.path[0], 'pyrosm_coll_conf.yaml'), encoding="utf-8") as m:config = yaml.safe_load(m) 
        var = config['VARIABLES_SET']
        # check if input osm_tags, osm_features are valid and print non valid ones
        for i in var[name]['collection']['osm_tags'].keys():
            if i not in OSM_tags.keys():
                print("%s is not a valid osm_feature" % i)
        for i in [item for sublist in var['bus_stops']['collection']['osm_tags'].values() for item in sublist]:
            if i not in [item for sublist in OSM_tags.values() for item in sublist]:
                print("%s is not a valid osm_tag" % i)

        # the loop prints a dict with all the tags in "not_sure" and their associated feature 
        # if the tag is not in osm_featutre_tags_dict.py, it will be assigned to the key "no_valid_osm_tag"
        temp = {}
        for key in var[name]['collection']['osm_tags'].keys():
            if key == 'not_sure':
                for i in var[name]['collection']['osm_tags'][key]:
                    for keys, values in OSM_tags.items():
                        for value in values:
                            if i == value:
                                if keys in temp.keys():
                                    if type(temp[keys]) == str:
                                        temp[keys] = [temp[keys], i]
                                    else:
                                        temp[keys].append(i)
                                else:
                                    temp = temp | {keys:i}
                            elif "no_valid_osm_tag" not in temp.keys() and i not in temp.values():
                                temp = temp | {"no_valid_osm_tag":i}
                            elif "no_valid_osm_tag" in temp.keys() and i not in temp["no_valid_osm_tag"]:
                                if type(temp["no_valid_osm_tag"]) == str:
                                    temp["no_valid_osm_tag"] = [temp["no_valid_osm_tag"], i]
                                else: 
                                    temp["no_valid_osm_tag"].append(i)
                print(temp)
                sys.exit()
        # the loop collects all tags of a feature from osm_feature_tags_dict.py if "all" in config file
        temp = {}
        for key, values in var[name]['collection']['osm_tags'].items():
            for value in values:
                if value == 'all':
                    temp = temp | {key:OSM_tags[key]}
        # collects user input from config file
        self.filter    = var[name]['collection']['osm_tags'] | temp
        self.columns   = list(var[name]['collection']['osm_tags'].keys()) + var[name]['collection']['additional_columns']
        self.f_point   = var[name]['collection']['points']
        self.f_polygon = var[name]['collection']['polygons']
        self.f_line    = var[name]['collection']['lines']

def gdf_conversion(gdf, name, return_type=None):
    if return_type=="GeoJSON":
        print("Writing down the geojson file %s ..." % (name + ".geojson"))
        start_time = time.time()
        gdf.to_file(os.path.join(sys.path[0],"data", name + ".geojson"), driver=return_type)
        print("Writing file %s seconds ---" % (time.time() - start_time))
        print("GeoJSON was written.")
        return gdf, name
    else:
        return gdf, name

# Function for collection data from OSM dbf and conversion to GeoJSON
# Could be extended with varios type of searches and filters 
# name='pois', driver='PandasDF' default variables driver could be 
# use 'update' parameter to download fresh data from OSM 
def osm_collect_filter(name, pbf_region=None, driver=None, update=False):
    # Timer
    print("Collection and filtering %s started..." % name)
    start_time = time.time()
    #import data from pois_coll_conf.yaml
    with open(os.path.join(sys.path[0] , 'pyrosm_coll_conf.yaml'), encoding="utf-8") as m:
        config = yaml.safe_load(m)

    var = config['VARIABLES_SET']

    # get region name desired pois types from yaml settings
    if pbf_region:
        pbf_data = pbf_region
    else:
        pbf_data = var['region_pbf']

    # Get defined data from Geofabrik
    fp = get_data(pbf_data, update=update)
    osm = OSM(fp)

    # Create filter class with given parameters and create filter with class method          
    custom_filter = PyrOSM_Filter(name)
    
    # Get Data from OSM with given filter ###custom_filter.tags_as_columns 
    # Additional filters can be applied (exmpl.  keep_nodes=False, keep_ways=True,keep_relations=True)
    df = osm.get_data_by_custom_criteria(custom_filter=custom_filter.filter, tags_as_columns=custom_filter.columns, keep_ways=custom_filter.f_line, 
    keep_relations=custom_filter.f_polygon, keep_nodes=custom_filter.f_point)

    print("Collection and filtering took %s seconds ---" % (time.time() - start_time))

    return_name = name + '_' + pbf_data

    # Return type if driver -> 'GeoJSON' write geojson file if driver -> 'PandasDF' return PandasDF
    return gdf_conversion(df, return_name ,driver)


def osm_collect_buildings(name='buildings', driver='PandasDF'):
    #import data from pois_coll_conf.yaml
    with open(os.path.join(sys.path[0] , 'pyrosm_coll_conf.yaml'), encoding="utf-8") as m:
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



# # Tests
# osm_collect_filter(name='pois')
# osm_collect_filter(name='bus_stops', driver='GeoJSON')

# osm_collect_buildings(driver='GeoJSON')
 
# %%