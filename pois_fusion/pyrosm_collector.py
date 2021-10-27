#%%
from pygeos import GEOSException
from pyrosm import get_data, OSM
from osm_feature_tags_dict import OSM_tags
import yaml
import geopandas
import os
import pandas
import sys
import time

# pyrosm filter class. Input for it: type of search 'pois'(currently only pois), tags from config , dictionary of osm tags
# variables of class are 'filter' - list of feature:tags which are relevant to scrap and 'tags_as_columns'- list of tags which will be 
# converted to columns in geopandas DataFrame
class PyrOSM_Filter:
    def __init__(self, filter, query_values, feat_dict, additional, point, polygon, line):
        new_osm_dict = {k: feat_dict[k] for k in filter}
        if not query_values:
            new_osm_dict = dict([(key, True) for key, val in new_osm_dict.items()])
        else:
            for t in query_values:
                if t not in [x for v in new_osm_dict.values() for x in v]:
                    print(t + ' is not in osm_feature_tag_dict.py for ''pois'' type of filter! Check OSM documentation and dict file.'+ '\n')
            for key, val in new_osm_dict.items():
                val = [t for t in query_values if t in val]
                new_osm_dict[key] = val
            new_osm_dict = dict([(key, val) for key, val in new_osm_dict.items() if len(val) > 0])
        self.filter = new_osm_dict 
        self.columns = additional + filter
        self.f_point = point
        self.f_polygon = polygon
        self.f_line = line      

def gdf_conversion(gdf, name, return_type='PandasDF'):
    if return_type == 'PandasDF':
        return gdf
    else:
        print("Writing down the geojson file %s ......" % (name + ".geojson"))
        start_time = time.time()
        gdf.to_file(os.path.join(sys.path[0],"data", name + ".geojson"), driver=return_type)
        print("Writing file %s seconds ---" % (time.time() - start_time))
        return print("GeoJSON was written.") 


# Function for collection data from OSM dbf and conversion to GeoJSON
# Could be extended with varios type of searches and filters 
# type='pois', driver='PandasDF' default variables driver could be 
def osm_collect_filter(type, driver):
    #import data from pois_coll_conf.yaml
    with open(os.path.join(sys.path[0] , 'pyrosm_coll_conf.yaml')) as m:
        config = yaml.safe_load(m)

    var = config['VARIABLES_SET']

    # get region name desired pois types from yaml settings
    pbf_data = var['region_pbf']
    query_values = var[type]['query_values']
    filter = var[type]['tags']['filter']
    additional = var[type]['tags']['additional']
    point = var[type]['points']
    polygon = var[type]['polygons']
    line = var[type]['lines']

    # Get defined data from Geofabrik
    fp = get_data(pbf_data)
    osm = OSM(fp)

    # Create filter class with given parameters and create filter with class method          
    custom_filter = PyrOSM_Filter(filter, query_values, OSM_tags, additional, point, polygon, line)

    # Get Data from OSM with given filter ###custom_filter.tags_as_columns 
    # Additional filters can be applied (exmpl.  keep_nodes=False, keep_ways=True,keep_relations=True)
    df = osm.get_data_by_custom_criteria(custom_filter=custom_filter.filter, tags_as_columns=custom_filter.columns, keep_ways=custom_filter.f_line, 
    keep_relations=custom_filter.f_polygon, keep_nodes=custom_filter.f_point)

    # Return type if driver -> 'GeoJSON' write geojson file if driver -> 'PandasDF' return PandasDF
    if driver == 'PandasDF':
        return df
    else:
        df.to_file(os.path.join(sys.path[0] , type + ".geojson"), driver=driver)


#%%
# tests

#%%

#buildings
def osm_collect_buildings(name='buildings', driver='PandasDF'):
    #import data from pois_coll_conf.yaml
    with open(os.path.join(sys.path[0] , 'pyrosm_coll_conf.yaml')) as m:
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
# osm_collect_filter(type='pois', driver='GeoJSON')
# osm_collect_filter(name='bus_stops', driver='GeoJSON')

# osm_collect_buildings(driver='GeoJSON')
 
# %%
