import geopandas as gpd
import pandas as pd
import numpy as np
from collection import gdf_conversion
from sqlalchemy import create_engine


def geonode_connection(password):
    engine = create_engine('postgresql://geonode_data:%s@161.97.133.234:5432/' % password)
    return engine

def geonode_table2df(engine, table_name, geometry_column='geometry'):
    query = "SELECT * FROM %s" % table_name
    df = gpd.read_postgis(con=engine,sql=query, geom_col=geometry_column)
    return df

def get_study_area(engine,rs):
    query = "SELECT * FROM germany_municipalities WHERE rs = '%s'" % rs
    df_area = gpd.read_postgis(con=engine,sql=query, geom_col='geom')
    df_area = df_area.filter(['geom'], axis=1)
    return df_area

def buffer_study_areas(list_areas, buffer=8300):
    area_union = pd.concat(list_areas,sort=False).reset_index(drop=True)
    area_union["dis_field"] = 1
    area_union = area_union.dissolve(by="dis_field")
    area_union_buffer = area_union
    area_union_buffer = area_union_buffer.to_crs(31468)
    area_union_buffer["geom"] = area_union_buffer["geom"].buffer(buffer)
    area_union_buffer = area_union_buffer.to_crs(4326)
    buffer_serie = area_union_buffer.difference(area_union)
    df_buffer_area = gpd.GeoDataFrame(geometry=buffer_serie)
    df_buffer_area = df_buffer_area.set_crs('epsg:4326')
    df_buffer_area = df_buffer_area.reset_index(drop=True)
    return df_buffer_area

# POIs fusion function for fusion custom pois data with osm data, referenced to table "germany_municipalities" with regions 
# RETURNS tuple(df,name) and saves data as file if is specified
# - engine - sqlalchemy engine with connection to geonode database
# - table_input - df with data to fuse with osm 
# - table_base - df of base osm data
# - df_area - df with polygon of study area 
# - amenity_replace - str name of amenity which objects will be removed from base osm df
# - columns2drop_input - list(str) with column names which are not merging to base os df 
# - return_name - str name of data, can be used as filename
# - return_type - str ("GeoJSON", "GPKG") file type for storage return
def fusion_data_area(engine, table_input, table_base, df_area, amenity_replace, columns2drop_input, return_name = None, return_type=None):
    # Fuction deaggregates address (street+housenumber) to separate strings returns tuple (street,number)
    def addr_deaggregate(addr_street):
        street_l = []
        number_l = []
        addr_split = addr_street.split()
        for a in addr_split:
            if len(a) > 3:
                street_l.append(a)
            else:
                number_l.append(a)

        street = ' '.join(street_l)
        number = ' '.join(number_l)
        
        return street, number

    # Open base data table (pois_bayern) and open input data table (custom_data)
    df_base = geonode_table2df(engine, table_base, geometry_column='geom')
    df_input = geonode_table2df(engine, table_input)

    # Cut data to given area 
    df_base2area = gpd.overlay(df_base, df_area, how='intersection')
    df_input2area = gpd.overlay(df_input, df_area, how='intersection')

    # Remove amenity class from base dataframe
    df_base2area = df_base2area[df_base2area.amenity != amenity_replace]

    # Prepare input data for concatination
    df_input2area = df_input2area.rename(columns={"category": "amenity", "extras": "tags", "spider": "source"})
    df_input2area = df_input2area.drop(columns={*columns2drop_input})
    df_input2area['amenity'] = df_input2area['amenity'].str.lower()

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

# 
def fusion_data_areas_rs_set(engine, table_input, table_base, rs_set, amenity_replace, columns2drop_input,return_name=None , return_type=None):
    df_res = pd.DataFrame()
    list_areas = []
    for rs in rs_set:
        df_area = get_study_area(engine,rs)
        list_areas.append(df_area)
        temp_df = fusion_data_area(engine, table_input, table_base, df_area, amenity_replace, columns2drop_input, return_name)[0]
        if rs_set.index(rs) == 0:
            df_res = temp_df
        else:
            df_res = pd.concat([df_res,temp_df],sort=False).reset_index(drop=True)
    df_buffer_area = buffer_study_areas(list_areas)
    df_buffer_pois = fusion_data_area(engine, table_input, table_base, df_buffer_area, amenity_replace, columns2drop_input, return_name)[0]
    df_res = pd.concat([df_res,df_buffer_pois],sort=False).reset_index(drop=True)

    return gdf_conversion(df_res, return_name, return_type=return_type)