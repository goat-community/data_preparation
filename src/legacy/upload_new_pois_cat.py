
from src.other.utility_functions import database_table2df, df2database, drop_table, migrate_table2localdb, file2df, gdf_conversion
from src.population.population_data_preparation import population_data_preparation
from src.population.produce_population_points import Population
from src.export.export_goat import getDataFromSql
from src.export.export_tables2basic import sql_queries_goat
from src.network.network_islands_municip import network_islands_mun
from preparation.poi import kindergarten_deaggrgation
from other.geocoding_functions import addLocationOfAdressToJson, GeoAddress

from src.db.db import Database
from src.config.config import Config

import json
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from preparation.poi import pois_preparation, landuse_preparation, buildings_preparation
from src.collection.fusion import df2area, pois_fusion, fuse_data_area
from src.network.ways import PrepareLayers, Profiles
from src.network.conversion_dem import conversion_dem

from src.db.prepare import PrepareDB
import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
from scipy.spatial import cKDTree 

def area_n_buffer2df(con, rs_set, buffer=8300):
    
    # Returns study area as df from remote db (germany_municipalities) according to rs code 
    def study_area2df(con,rs):
        query = "SELECT * FROM germany_municipalities WHERE rs = '%s'" % rs
        df_area = gpd.read_postgis(con=con,sql=query, geom_col='geom')
        df_area = df_area.filter(['geom'], axis=1)
        return df_area
    
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
    try:
        addr_split = addr_street.split()
        for a in addr_split:
            if len(a) < 2 or any(map(str.isdigit, a)):
                number_l.append(a)
            else:
                street_l.append(a)

        street = ' '.join(street_l)
        number = ' '.join(number_l)
        return street, number
    except:
        pass

def df2area(df,df_area):
    df2area = gpd.overlay(df, df_area, how='intersection')
    return df2area

def find_nearest(gdA, gdB, max_dist):
    gdA.crs = "epsg:4326"
    gdB.crs = "epsg:4326"
    gdA = gdA.to_crs(31468)
    gdB = gdB.to_crs(31468)
    nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    # gdB_nearest = gdB.iloc[idx].drop(columns={"geometry"}).reset_index(drop=True)
    gdB_nearest = gdB.iloc[idx].reset_index(drop=True)
    gdA = gdA.rename(columns={"addr:street": "street"})
    gdf = pd.concat(
        [
            gdA.reset_index(drop=True),
            gdB_nearest["uid"],
            pd.Series(dist, name='dist')
        ], 
        axis=1)

    gdf = gdf.rename(columns={"street": "addr:street", "city": "addr:city", "postcode" : "addr:postcode", "country": "addr:country"})
    gdf_fus = gdf[gdf.dist < max_dist]
    m = ~gdf.id.isin(gdf_fus.id)
    gdf_not_fus = gdf[m]
    gdf_fus = gdf_fus.to_crs(4326)
    gdf_not_fus = gdf_not_fus.to_crs(4326)
    gdf_fus = gdf_fus.drop(columns={"dist"})
    gdf_not_fus = gdf_not_fus.drop(columns={"dist"})
    gdf_not_fus["uid"] = None

    return gdf_fus, gdf_not_fus

# Indexing data in dataframe with goat indexes
def dataframe_goat_index(df):
    db = Database()
    con = db.connect_rd()
    cur = con.cursor()
    df = df[df['amenity'].notna()]
    df['id_x'] = df.centroid.x * 1000
    df['id_y'] = df.centroid.y * 1000
    df['id_x'] = df['id_x'].apply(np.floor)
    df['id_y'] = df['id_y'].apply(np.floor)
    df = df.astype({'id_x': int, 'id_y': int})
    df['poi_goat_id'] = df['id_x'].map(str) + '-' + df['id_y'].map(str) + '-' + df['amenity']
    df = df.drop(columns=['id_x','id_y'])
    df["osm_id"] = df["osm_id"].fillna(value=0)
    df_poi_goat_id = df[['poi_goat_id','osm_id', 'name']]

    cols = ','.join(list(df_poi_goat_id.columns))
    tuples = [tuple(x) for x in df_poi_goat_id.to_numpy()]

    cnt = 0

    for tup in tuples:
        tup_l = list(tup)
        id_number = tup_l[0]
        query_select = f"SELECT max(index) FROM poi_goat_id WHERE poi_goat_id = '{id_number}'"
        last_number = db.select_rd(query_select)
        if (list(last_number[0])[0]) is None:
            tup_new = tup_l
            tup_new.append(0)
            tup_new = tuple(tup_new)
            cur.execute("""INSERT INTO poi_goat_id(poi_goat_id, osm_id, name, index) VALUES (%s,%s,%s,%s)""",tup_new)
            con.commit()
            df.iloc[cnt, df.columns.get_loc('poi_goat_id')] = f'{id_number}-0000'
        else:
            new_ind = list(last_number[0])[0] + 1
            tup_new = tup_l
            tup_l.append(new_ind)
            tup_new = tuple(tup_new)
            cur.execute("""INSERT INTO poi_goat_id(poi_goat_id, osm_id, name, index) VALUES (%s, %s, %s, %s)""",tup_new)
            con.commit()
            df.iloc[cnt, df.columns.get_loc('poi_goat_id')] = f'{id_number}-{new_ind:04}'
        cnt += 1
    con.close()
    df = df.astype({'osm_id': int})
    return df

def fuse_newdata(df_base2area, df_area, df_input, amenity_fuse=None, amenity_set=False, amenity_brand_fuse=None, 
                   columns2rename=None, column_set_value=None, columns2fuse=None):
    # Cut input data to given area 
    df_input2area = gpd.overlay(df_input, df_area, how='intersection')

    # Sort df by config settings from base dataframe
    if amenity_fuse:
        df_base_amenity = df_base2area[df_base2area.category == amenity_fuse]
        df_base2area_rest = df_base2area[df_base2area.category != amenity_fuse]

    elif amenity_brand_fuse:
        amenity_brand_fuse = eval(amenity_brand_fuse)
        df_base_amenity = df_base2area[((df_base2area.operator.str.lower() == amenity_brand_fuse[1].lower()) |
                                        (df_base2area.brand.str.lower() == amenity_brand_fuse[1].lower())) & 
                                        (df_base2area.amenity.str.lower() == amenity_brand_fuse[0].lower())]
        df_base2area_rest = df_base2area[~df_base2area.apply(tuple,1).isin(df_base_amenity.apply(tuple,1))]
    else:
        print("Amenity (and brand) were not specified.. ")

    if "addr:street" in df_input2area.columns.tolist():
        df_input2area["addr:street"] = df_input2area["addr:street"].fillna(value="")
        if 'housenumber' not in df_input2area.columns.tolist():
            for i in df_input2area.index:
                df_row = df_input2area.iloc[i]
                address = addr_deaggregate(df_row["addr:street"])
                df_input2area.at[i,"addr:street"] = address[0]
                df_input2area.at[i,"housenumber"] = address[1]

    if column_set_value:
        for col in column_set_value.keys():
            df_input2area[col] = column_set_value[col]

    # Create temp sorted base dataframe 
    df_base_temp = df_base_amenity[["geometry", "uid"]]
    # find closest points to fuse, default max distance for fusion 150 meters
    # return 2 df - find closests and not
    df_fus, df_not_fus = find_nearest(df_input2area, df_base_temp, 500)
   
    if columns2fuse:
      fus_col_fus = columns2fuse.copy()
    else:
      fus_col_fus = ["source"]
    fus_col_fus.append("uid")
    fus_col_fus.remove("source")
    df_fus = df_fus[fus_col_fus]

    # Prepare input data for concatination
    # if columns2drop:
    #     df_not_fus = df_not_fus.drop(columns={*columns2drop})
    if columns2rename:
        df_not_fus = df_not_fus.rename(columns=columns2rename)

    if amenity_brand_fuse:
        df_not_fus['amenity'] = amenity_brand_fuse[0]
        df_not_fus['operator'] = amenity_brand_fuse[1].lower()
        columns2fuse.extend(('amenity', 'operator', 'geometry'))
        df_not_fus = df_not_fus[columns2fuse]
    elif amenity_set:
        df_not_fus['category'] = amenity_fuse.lower()
        columns2fuse.extend(('category', 'geometry'))
        df_not_fus = df_not_fus[columns2fuse]
    df_not_fus['amenity'] = df_not_fus['amenity'].str.lower()

    # Concatination of dataframes
    df_result = (df_fus.set_index('uid').combine_first(df_base_amenity.set_index('uid')))
    df_result = df_result.reset_index()
    df_result = gpd.GeoDataFrame(df_result, geometry="geometry")
   #  df_result = pd.concat([df_result,df_not_fus],sort=False)
    df_result = df_result.replace({np.nan: None})
    df_result = pd.concat([df_result,df_base2area_rest],sort=False)
    df_result.crs = "epsg:4326"

    return df_result, df_not_fus


##================================================================================##

config = Config('pois')

db = Database()
con = db.connect()
pois = database_table2df(con, 'basic.poi', geometry_column='geom')

df_area = config.get_areas_by_rs(buffer=8300)

df_base2area = df2area(pois, df_area)

df_input = file2df('bikesharing_nürnberg.geojson')

df_temp = df_input[['bike_racks','number']]
df_input['tags'] = df_temp.to_dict('records')
df_input = df_input[['id','source','name','geometry','tags']]

df_fused, df_not_fused = fuse_newdata(df_base2area, df_area, df_input, amenity_fuse='bike_sharing',amenity_set=True)

gdf_conversion(df_fused, 'df_fused', 'GeoJSON')
gdf_conversion(df_not_fused, 'df_not_fused', 'GeoJSON')