

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


def landuse_preparation(dataframe=None, filename=None, return_type=None, result_filename="landuse_preparation_result", custom_filter=None):
    """Beschreibung für eine Funktion kdhwekjhdkj"""
    # (2 Options) landuse preparation from geojson imported from OSM (if you already have it)
    if dataframe is not None:
        df = dataframe
    elif filename:
        file = open(os.path.join(os.path.abspath(os.getcwd()),
                                 'data', filename + ".geojson"), encoding="utf-8")
        df = gp.read_file(file)
    else:
        print("Incorrect 'datatype' value!")
        sys.exit()

    # Timer start
    print("Preparation started...")
    start_time = time.time()

    # Preprocessing: removing, renaming and reordering of columns
    df = df.drop(columns={"timestamp", "version", "changeset"})
    df = df.rename(columns={"geometry": "geom",
                            "id": "osm_id", "osm_type": "origin_geometry"})
    df["landuse_simplified"] = None
    df = df[["landuse_simplified", "landuse", "tourism", "amenity", "leisure", "natural", "name", "tags",
             "osm_id", "origin_geometry", "geom"]]

    # Fill landuse_simplified coulmn with values from the other columns

    if custom_filter == None:
        print("landuse_simplified can only be generated if the custom_filter of osm_collect_filter is passed")
    else:
        for i in custom_filter.keys():
            df["landuse_simplified"] = df["landuse_simplified"].fillna(
                df[i].loc[df[i].isin(custom_filter[i])])

        # import landuse_simplified dict from pyrosm_collector.py
        with open(os.path.join(sys.path[0], 'pyrosm_coll_conf.yaml'), encoding="utf-8") as m:
            config = yaml.safe_load(m)
        var = config['VARIABLES_SET']
        landuse_simplified_dict = var['landuse']['preparation']['landuse_simplified']

        # Rename landuse_simplified by grouping e.g. ["basin","reservoir","salt_pond","waters"] -> "water"
        for i in landuse_simplified_dict.keys():
            df["landuse_simplified"] = df["landuse_simplified"].replace(
                landuse_simplified_dict[i], i)

    # hier wörter durch dict.keys() ebenfalls ersetzen
    if df.loc[~df['landuse_simplified'].isin(list(landuse_simplified_dict.keys()))].empty:
        print("All entries were classified in landuse_simplified")
    else:
        print("The following tags in the landuse_simplified column need to be added to the landuse_simplified dict in pyrosm_coll_conf.yaml:")
        print(df.loc[~df['landuse_simplified'].isin(
            list(landuse_simplified_dict.keys()))])

    # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry="geom")

    df = df.reset_index(drop=True)

    # Timer finish
    print("Preparation took %s seconds ---" % (time.time() - start_time))
    print(df)
    print(df.columns)

    return gdf_conversion(df, result_filename, return_type)


# tests

# dataframe        = osm_collect_filter(name='landuse' ,driver=None, update=False)[0][0]
# name             = osm_collect_filter(name='landuse' ,driver=None, update=False)[0][1]
# custom_filter    = osm_collect_filter(name='landuse' ,driver=None, update=False)[1]
# landuse_prepared = landuse_preparation(dataframe = osm_collect_filter(name='landuse' ,driver=None, update=False)[0][0], return_type=None,result_filename="landuse_preparation_result", custom_filter=osm_collect_filter(name='landuse' ,driver=None, update=False)[1])

collection = osm_collect_filter(name='landuse', driver=None, update=False)
#landuse_prepared = landuse_preparation(
#    dataframe=collection[0][0], return_type=None, result_filename="landuse_preparation_result", custom_filter=collection[1])
#geodataframe = landuse_prepared[0]
#name = landuse_prepared[1]

# geodataframe.to_file(os.path.join(os.path.join(os.path.abspath(
#    os.getcwd()), "data"), "landuse_Bavaria_prepared.geojson"), driver="GeoJSON")

#geodataframe.to_file(os.path.join(os.path.join(os.path.abspath(
#   os.getcwd()), "data"), "landuse_Bavaria_prepared.gpkg"), driver="GPKG")

# Überarbeiten

# osm_collect_filter(name = 'landuse', driver = 'PandasDF', update = True)[0].to_file(os.path.join(os.path.join(os.path.abspath(os.getcwd()), "data") , "landuse.geojson"), driver="GeoJSON")

# checking data to analyse wrong classifications
# df.loc[(df['landuse_simplified']=='water') & (df['natural'].isin(['basin','reservoir','salt_pond','waters', 'swimming_pool', 'water']))]
# df.loc[~df['landuse_simplified'].isin(['water', 'agriculture', 'nature', 'leisure', 'cemetery', 'residential', 'community','commercial', 'industrial', 'transportation', 'military'])]

# pd.DataFrame(df)

# %%
