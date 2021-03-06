from other.utility_functions import database_table2df, df2database, drop_table,table_dump,table_restore
from collection.fusion import df2area
from config.config import Config
from db.db import Database
from datetime import datetime
from zoneinfo import ZoneInfo
from collection.fusion import dataframe_goat_index
from collection.sql_scripts import sql_queries_goat
import pandas as pd 

# IMPORTANT! This fuction creates new poi_goat_id in remote database table. 
# It is necessary to control your actions!
# Copy of 'poi' table should be in the local database -> local table will be updated
def pois_update(db,con):

    config = Config("pois")
    values = config.update['categories']

    db_rd = Database('reading')
    con_rd = db_rd.connect_rd()

    now = datetime.now(ZoneInfo("Europe/Berlin"))
    date_time = now.strftime("_%d%m%y_%H%M%S")

    prefix_r = '_geonode' + date_time

    table_dump('remote','poi','public',prefix_r)
    drop_table(con, 'poi')
    table_restore('local','poi'+prefix_r)   

    # POIs from OSM to update (by selected categories)
    df_pois_update = database_table2df(con, 'pois', geometry_column='geom')
    df_pois_update = df_pois_update[df_pois_update.amenity.isin(values)]

    # POIs from GOAT database (GeoNode) (by selected categories)
    df_poi_base = database_table2df(con_rd, 'poi', geometry_column='geom') 
    df_poi_base = df_poi_base[df_poi_base.category.isin(values)]

    # Cut both data sets to defined study areas
    df_area = config.get_areas_by_rs(con_rd, buffer=8300,process='update')
    df_pois = df2area(df_pois_update, df_area)
    df_poi_base = df2area(df_poi_base, df_area)

    # Dataframe from poi_goat_id table where values are from OSM
    select_id = '''SELECT concat(poi_goat_id, '-', to_char("index", 'fm0000')) AS uid, osm_id, origin_geometry, split_part(poi_goat_id, '-', 3) AS amenity
                   FROM poi_goat_id 
                   WHERE osm_id != 0;'''
    df_goat_id = pd.read_sql(select_id, con_rd)

    # Filter poi_goat_id table to study area boundaries and to defined categories
    i_pb = df_poi_base.set_index(['uid']).index
    i_gid = df_goat_id.set_index(['uid']).index
    df_goat_id_sa = df_goat_id[i_gid.isin(i_pb)]
    df_goat_id_sa = df_goat_id_sa.loc[df_goat_id_sa['amenity'].isin(values)]

    # Dataframe with newly found pois
    i1 = df_pois.set_index(['osm_id', 'origin_geometry']).index
    i2 = df_goat_id_sa.set_index(['osm_id', 'origin_geometry']).index
    df_new_pois = df_pois[~i1.isin(i2)]

    # Dataframe with 'poi_goat_id' which were removed from OSM but existed in GOAT database
    df_removed_poi_id = df_goat_id_sa[~i2.isin(i1)]

    # Indexing new pois and create table 'pois_upload' in local database
    df_new_pois = dataframe_goat_index(df_new_pois)
    drop_table(con,'pois_upload')
    df2database(df_new_pois, 'pois_upload')
    for v in values:
        df_temp = df_new_pois.loc[df_new_pois['amenity'] == v]
        number = len(df_temp.index)
        print(f'{number} new {v} were added to local "poi" database.')

    # Create table with uid for pois which should be removed
    conn = db.connect_sqlalchemy()
    drop_table(con,'pois_remove')
    df_removed_poi_id.to_sql('pois_remove', conn)
    for v in values:
        df_temp = df_removed_poi_id.loc[df_removed_poi_id['amenity'] == v]
        number = len(df_temp.index)
        print(f'{number} {v} were removed from local "poi" database.')

    # Remove entities from local 'poi' table and upload it local 'poi' database
    db = Database()
    db.perform(sql_queries_goat['pois_update'])

def poi_geonode_update():
    
    db_rd = Database('reading')
    conn = db_rd.connect_rd()

    table = 'poi'

    now = datetime.now(ZoneInfo("Europe/Berlin"))
    date_time = now.strftime("_%d%m%y_%H%M%S")

    prefix_r = '_geonode' + date_time
    prefix_l = '_local' + date_time

    table_dump('remote',table,'public',prefix_r)
    table_dump('local',table,'public',prefix_l)

    drop_table(conn, table)
    table_restore('remote',table+prefix_l)

    db_rd.perform_rd(sql_queries_goat['update_poi_id_table'])

