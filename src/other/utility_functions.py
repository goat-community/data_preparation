import os
import sys
import time
import json
import subprocess
from pathlib import Path
import geopandas as gp
from decouple import config
import sqlalchemy

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from db.db import Database, DATABASE_RD, DATABASE

# Function for creation backupfiles
# Convert and save dataframe as GPKG or GeoJSON file formats
# Use it as return result for data preparation functions
def gdf_conversion(gdf, name=None, return_type=None):
    """can convert a GeoGataFrame(gdf) into GeoJSON or GPKG, but always returns the gdf and name"""
    if return_type == "GeoJSON":
        print(f"Writing down the geojson file {name + '.geojson'} ...")
        start_time = time.time()
        gdf.to_file(
            os.path.join("src", "data", "output", (name + ".geojson")),
            driver=return_type,
        )
        print(f"Writing file {time.time() - start_time} seconds ---")
        print(f"GeoJSON {name + '.geojson'} was written.")
        return gdf, name
    elif return_type == "GPKG":
        print(f"Writing down the geopackage file {name + '.gpkg'} ...")
        start_time = time.time()
        gdf.to_file(
            os.path.join("src", "data", "output", (name + ".gpkg")), driver=return_type
        )
        print(f"Writing file {time.time() - start_time} seconds ---")
        print(f"GeoPackage {name + '.gpkg'} was written.")
        return gdf, name
    else:
        return gdf, name


def file2df(filename):
    name, extens = filename.split(".")
    if extens == "geojson":
        file = open(os.path.join("src", "data", "input", filename), encoding="utf-8")
        df = gp.read_file(file)
    elif extens == "gpkg":
        file = os.path.join("src", "data", "input", filename)
        df = gp.read_file(file)
    else:
        print(
            "Extension of file %s currently does not support by file2df() function."
            % filename
        )
        sys.exit()
    return df

# Create connection to remote database
def rdatabase_connection():
    db = Database()
    con = db.connect_rd()
    return con

# Publish dataframe in REMOTE database
# df - dataframe, name - table name to store, if_exists="replace" to overwrite datatable
def df2rdatabase(df, name, if_exists="replace"):
    db = Database()
    con = db.connect_rd_sqlalchemy()
    df.to_postgis(con=con, name=name, if_exists=if_exists)


# Returns remote database table as a dataframe
def database_table2df(con, table_name, attribute=None, value=None, geometry_column="geometry"):
    if not attribute or not value:
        query = f"SELECT * FROM {table_name};" 
    else:
        query = f"SELECT * FROM {table_name} WHERE {attribute} = {value};" 
    df = gp.read_postgis(con=con, sql=query, geom_col=geometry_column)
    return df

# Create table from dataframe in local database (goat)
def df2database(df, name, if_exists="replace"):
    db = Database()
    con = db.connect_sqlalchemy()
    def dict2json(dictionary):
        return json.dumps(dictionary, ensure_ascii=False)
    try:
        df['tags'] = df.tags.map(dict2json)
        df.to_postgis(con=con, name=name, if_exists=if_exists, dtype={'tags': sqlalchemy.dialects.postgresql.JSONB})
    except:
        df.to_postgis(con=con, name=name, if_exists=if_exists)


def drop_table(table):
    db = Database()
    conn = db.connect()
    cur = conn.cursor()
    drop_table = """DROP TABLE IF EXISTS {0};""".format(table)
    cur.execute(drop_table)
    conn.commit()
    conn.close()


# Create pgpass function
def create_pgpass():
    db_name = config("POSTGRES_DB")
    try:
        os.system(f"rm ~/.pgpass_{db_name}")
    except:
        pass
    os.system(
        "echo "
        + ":".join(
            [
                config("POSTGRES_HOST"),
                "5432",
                db_name,
                config("POSTGRES_USER"),
                config("POSTGRES_PASSWORD"),
            ]
        )
        + f" > ~/.pgpass_{db_name}"
    )
    os.system(f"chmod 600  ~/.pgpass_{db_name}")


# 'source' could be "remote" or "local". Need to specify
def table_dump(source, tablename,data_only=False):
    if source == "remote":
        dbname_rd, host_rd, username_rd, port_rd, password_rd = (
            DATABASE_RD["dbname"],
            DATABASE_RD["host"],
            DATABASE_RD["user"],
            DATABASE_RD["port"],
            DATABASE_RD["password"],
        )
        try:
            os.system(f"rm ~/.pgpass_{dbname_rd}")
        except:
            pass
        os.system(
            "echo "
            + ":".join([host_rd, str(port_rd), dbname_rd, username_rd, password_rd])
            + f" > ~/.pgpass_{dbname_rd}"
        )
        os.system(f"chmod 600  ~/.pgpass_{dbname_rd}")
        if not data_only:
            cmd_call = f"""PGPASSFILE=~/.pgpass_{dbname_rd} pg_dump -h {host_rd} -t {tablename} --no-owner -U {username_rd} {dbname_rd} > export_results/{tablename}.sql"""
        else:
            cmd_call = f"""PGPASSFILE=~/.pgpass_{dbname_rd} pg_dump -h {host_rd} -t {tablename} --no-owner -U {username_rd} --data-only  {dbname_rd} > export_results/{tablename}.sql"""
        subprocess.call(cmd_call, shell=True)
    elif source == "local":
        create_pgpass()
        dbname, host, username, port = (
            DATABASE["dbname"],
            DATABASE["host"],
            DATABASE["user"],
            DATABASE["port"],
        )
        if not data_only:
            cmd_call = f"""PGPASSFILE=~/.pgpass_{dbname} pg_dump -h {host} -t {tablename} --no-owner -U {username} {dbname} > export_results/{tablename}.sql"""
        else:
            cmd_call = f"""PGPASSFILE=~/.pgpass_{dbname} pg_dump -h {host} -t {tablename} --no-owner -U {username} --data-only  {dbname} > export_results/{tablename}.sql"""
        subprocess.call(cmd_call, shell=True)
    else:
        print("Please, specify 'source' - 'remote' or 'local'!")


def table_restore(source, filename):
    if source == "remote":
        dbname_rd, host_rd, username_rd, port_rd, password_rd = (
            DATABASE_RD["dbname"],
            DATABASE_RD["host"],
            DATABASE_RD["user"],
            DATABASE_RD["port"],
            DATABASE_RD["password"],
        )
        try:
            os.system(f"rm ~/.pgpass_{dbname_rd}")
        except:
            pass
        os.system(
            "echo "
            + ":".join([host_rd, str(port_rd), dbname_rd, username_rd, password_rd])
            + f" > ~/.pgpass_{dbname_rd}"
        )
        os.system(f"chmod 600  ~/.pgpass_{dbname_rd}")
        cmd_call = f"""PGPASSFILE=~/.pgpass_{dbname_rd} psql -h {host_rd} -U {username_rd} -p {port_rd} -d {dbname_rd} < export_results/{filename}.sql"""
        subprocess.call(cmd_call, shell=True)
    elif source == "local":
        create_pgpass()
        dbname, host, username, port = (
            DATABASE["dbname"],
            DATABASE["host"],
            DATABASE["user"],
            DATABASE["port"],
        )
        cmd_call = f"""PGPASSFILE=~/.pgpass_{dbname} psql -h {host} -U {username} -p {port} -d {dbname} < export_results/{filename}.sql"""
        subprocess.call(cmd_call, shell=True)
    else:
        print("Please, specify 'source' - 'remote' or 'local'!")


def migrate_table2localdb(source_table, dest_table):
    db = Database()
    db.perform_rd(query=f"DROP TABLE IF EXISTS {dest_table};")
    if source_table != dest_table:
        db.perform_rd(
            query=f"ALTER TABLE temporal.{source_table} RENAME TO {dest_table};"
        )
    db.perform_rd(query=f"ALTER TABLE temporal.{dest_table} SET SCHEMA public;")
    table_dump("remote", dest_table)
    db.perform_rd(query=f"DROP TABLE IF EXISTS {dest_table};")
    db.perform(query = f"DROP TABLE IF EXISTS {dest_table} CASCADE;")
    table_restore("local", dest_table)
    subprocess.run(f'rm export_results/{dest_table}.sql', shell=True, check=True)


def GetTableList(t_schema, source='remote'):
    # Retrieve the table list
    s = "SELECT table_schema, table_name FROM information_schema.tables WHERE (table_schema = '" + t_schema + "') ORDER BY table_schema, table_name;"

    db = Database()
    if source == 'remote':
        db_cursor = db.cursor_rd()
    elif source == 'local':
        db_cursor = db.cursor()
    else:
        print('Please, specify type of source of database "remote" or "local!"')
    # Retrieve all the rows from the cursor
    db_cursor.execute(s)
    list_tables = db_cursor.fetchall()
    # Print the names of the tables
    table_list = [y for x,y in list_tables]
    print(table_list)
    return table_list

def migrate_all_tables2localdb():
    tables = GetTableList('temporal')
    for t in tables:
        migrate_table2localdb(t,t)

def create_sql_dumps():
    tables_basic_standard = ['aoi', 'building', 'dem', 'edge', 'grid_calculation', 'grid_visualization', 'node', 'poi', 'population', 'study_area', 'sub_study_area']
    tables_extra_standard = ['accidents_walking', 'accidents_cycling', 'building', 'landuse_additional', 'landuse_atkis', 'landuse_osm']

    tables_basic = GetTableList('basic', 'local')
    for tab_b in tables_basic:
        if tab_b in tables_basic_standard:
            table_dump('local','.'.join(['basic',tab_b]), data_only=True)

    tables_extra = GetTableList('extra', 'local')
    for tab_e in tables_extra:
        if tab_e in tables_extra_standard:
            table_dump('local','.'.join(['extra',tab_e]))



########################TESTS############################
# create_sql_dumps()

# migrate_all_tables2localdb()

# migrate_table2localdb('accidents', 'accidents')

# table_dump('local','ways_vertices_pgr')
# table_restore('remote', 'ways_vertices_pgr')

# table_dump('local','ways')
# table_restore('remote', 'ways')

# table_dump('local','landuse_osm')
# table_restore('remote', 'landuse_osm')

# table_dump('local','buildings_osm')
# table_restore('remote', 'buildings_osm')

# table_dump('local','planet_osm_point')
# table_restore('remote', 'planet_osm_point')

# table_dump('local','pois')
# table_restore('remote', 'pois')