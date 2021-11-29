import os
import sys
import geopandas as gpd
import pandas as pd
import numpy as np
from collection import gdf_conversion
from db.db import Database
from db.config import DATABASE
#from sqlalchemy import create_engine

# Create connection to geonode db
def geonode_connection():
    db = Database()
    con = db.connect()
    return con

# Returns geonode table as a dataframe 
def geonode_table2df(con, table_name, geometry_column='geometry'):
    query = "SELECT * FROM %s" % table_name
    df = gpd.read_postgis(con=con,sql=query, geom_col=geometry_column)
    return df

# Returns study area as df from geonode db (germany_municipalities) according to rs code 
def study_area2df(con,rs):
    query = "SELECT * FROM germany_municipalities WHERE rs = '%s'" % rs
    df_area = gpd.read_postgis(con=con,sql=query, geom_col='geom')
    df_area = df_area.filter(['geom'], axis=1)
    return df_area

# Creates DataFrame with buffer (default 8300 meters) of geometries, provided as a list of dataframes
def area_n_buffer2df(con, rs_set, buffer=8300):
    list_areas = []
    for rs in rs_set:
        df_area = study_area2df(con,rs)
        list_areas.append(df_area)
    df_area_union = pd.concat(list_areas,sort=False).reset_index(drop=True)
    df_area_union["dis_field"] = 1
    df_area_union = df_area_union.dissolve(by="dis_field")
    area_union_buffer = df_area_union
    area_union_buffer = area_union_buffer.to_crs(31468)
    area_union_buffer["geom"] = area_union_buffer["geom"].buffer(buffer)
    area_union_buffer = area_union_buffer.to_crs(4326)
    buffer_serie = area_union_buffer.difference(df_area_union)
    df_buffer_area = gpd.GeoDataFrame(geometry=buffer_serie)
    df_buffer_area = df_buffer_area.set_crs('epsg:4326')
    df_buffer_area = df_buffer_area.reset_index(drop=True)
    df_buffer_area = df_buffer_area.rename(columns={"geometry":"geom"})    
    df = pd.concat([df_area_union,df_buffer_area], sort=False).reset_index(drop=True)
    df["dis_field"] = 1
    df = df.dissolve(by="dis_field").reset_index(drop=True)
    return df

    # Fuction deaggregates address (street+housenumber) to separate strings returns tuple (street,number)
def addr_deaggregate(addr_street):
    street_l = []
    number_l = []
    addr_split = addr_street.split()
    for a in addr_split:
        if len(a) < 2 or any(map(str.isdigit, a)):
            number_l.append(a)
        else:
            street_l.append(a)

    street = ' '.join(street_l)
    number = ' '.join(number_l)
       
    return street, number

def base2area(df_base,df_area):
    df_base2area = gpd.overlay(df_base, df_area, how='intersection')
    return df_base2area

# POIs fusion function for fusion custom pois data with osm data, referenced to table "germany_municipalities" with regions 
# RETURNS tuple(df,name) and saves data as file if is specified
# - con - sqlalchemy con with connection to geonode database
# - table_input - df with data to fuse with osm 
# - table_base - df of base osm data
# - df_area - df with polygon of study area 
# - amenity_replace - str name of amenity which objects will be removed from base osm df
# - amenity_update - str name of amenity to update (works together with brand_replace)
# - brand_replace - str brand to replace (works)
# - columns2drop_input - list(str) with column names which are not merging to base os df 
# - return_name - str name of data, can be used as filename
# - return_type - str ("GeoJSON", "GPKG") file type for storage return


def replace_data_area(df_base2area, df_area, df_input, osm_location = False , amenity_replace=None, amenity_brand_replace=None, columns2rename=None, columns2drop=None, return_name = None, return_type=None):
    # Cut data to given area 
    df_input2area = gpd.overlay(df_input, df_area, how='intersection')

    # Remove amenity class from base dataframe
    if amenity_replace:
        df_base2area = df_base2area[df_base2area.amenity != amenity_replace]
        if osm_location:
            df_base_amenity = df_base2area[df_base2area.amenity == amenity_replace]
    elif amenity_brand_replace:
        amenity_brand_replace = eval(amenity_brand_replace)
        df_base2area = df_base2area[(df_base2area.amenity != amenity_brand_replace[0]) & (df_base2area.brand != amenity_brand_replace[1])]
        if osm_location:
            df_base_amenity_brand = df_base2area[(df_base2area.amenity == amenity_brand_replace[0]) & (df_base2area.brand == amenity_brand_replace[1])]
    else:
        print("Amenity (and brand) were not specified.. ")

    # Prepare input data for concatination
    df_input2area = df_input2area.rename(columns=columns2rename)
    df_input2area = df_input2area.drop(columns={*columns2drop})
    if amenity_brand_replace:
        df_input2area['amenity'] = amenity_brand_replace[0]
        df_input2area['brand'] = amenity_brand_replace[1]
    df_input2area['amenity'] = df_input2area['amenity'].str.lower()

    # Set tags and geom from osm data to input

    # Deaggregate street and number
    for i in df_input2area.index:
        df_row = df_input2area.iloc[i]
        address = addr_deaggregate(df_row["addr:street"])
        df_input2area.at[i,"addr:street"] = address[0]
        df_input2area.at[i,"housenumber"] = address[1]

    # Concatination of dataframes
    df_result = pd.concat([df_base2area,df_input2area],sort=False).reset_index(drop=True)
    df_result = df_result.replace({np.nan: None})

    # Writedown result of fusion
    return gdf_conversion(df_result, return_name, return_type=return_type)


def fuse_data_area(df_base2area, df_area, df_input, amenity_fuse=None, amenity_brand_fuse=None, columns2rename=None, columns2drop=None, return_name = None, return_type=None):
    # Cut data to given area 
    df_input2area = gpd.overlay(df_input, df_area, how='intersection')
    
    return 