import os
import shutil
import subprocess
from rich import print as print
from urllib.request import urlopen, Request
from pathlib import Path
import geopandas as gpd
import pandas as pd
import numpy as np
from src.db.db import Database
from sqlalchemy.engine.base import Engine
import sys
import threading

def delete_file(file_path: str) -> None:
    """Delete file from disk."""
    try:
        os.remove(file_path)
    except OSError as e:
        pass

def delete_dir(dir_path: str) -> None:
    """Delete file from disk."""
    try:
        shutil.rmtree(dir_path)
    except OSError as e:
        pass

def print_hashtags():
    print(
        "#################################################################################################################"
    )

def print_info(message: str):
    print(f"\nINFO: {message}")

def print_warning(message: str):
    print(f"\nWARNING: {message}")

def download_link(directory: str, link: str, new_filename: str = None):
    if new_filename is not None:
        filename = new_filename
    else: 
        filename = os.path.basename(link)
        
    download_path = Path(directory) / filename
    with urlopen(link) as image, download_path.open("wb") as f:
        f.write(image.read())

    print_info(f"Downloaded ended for {link}")

def create_pgpass_for_db(db_config: dict):
    """Creates pgpass file for specified DB config

    Args:
        db_config (str): Database configuration dictionary.
    """

    delete_file(f"""~/.pgpass_{db_config["dbname"]}""")
    os.system(
        "echo "
        + ":".join(
            [
                db_config["host"],
                str(db_config["port"]),
                db_config["dbname"],
                db_config["user"],
                db_config["password"],
            ]
        )
        + f""" > ~/.pgpass_{db_config["dbname"]}"""
    )
    os.system(f"""chmod 600  ~/.pgpass_{db_config["dbname"]}""")

def create_table_dump(db_config: dict, table_name: str, data_only: bool = False):
    """Create a dump from a table

    Args:
        db_config (str): Database configuration dictionary.
        table_name (str): Specify the table name including the schema.
    """
    if data_only == True:
        data_only_tag = "--data-only"
    else:
        data_only_tag = ""
        
    try:
        dir_output = (
            os.path.abspath(os.curdir)
            + "/src/data/output/"
            + table_name.split(".")[1]
            + ".sql"
        )

        subprocess.run(
            f"""PGPASSFILE=~/.pgpass_{db_config["dbname"]} pg_dump -h {db_config["host"]} -t {table_name} {data_only_tag} --no-owner -U {db_config["user"]} {db_config["dbname"]} > {dir_output}""", #-F t
            shell=True,
            check=True,
        )
    except Exception as e:
        print_warning(f"The following exeption happened when dumping {table_name}: {e}")

def create_table_schema(db: Database, db_config: dict, table_full_name: str):
    """Function that creates a table schema from a database dump.

    Args:
        db (Database): Database connection class.
        db_conf (dict): Database configuration dictionary.
        table_full_name (str): Name with the schema of the table (e.g. basic.poi).
    """
    db.perform(query="CREATE SCHEMA IF NOT EXISTS basic;")
    db.perform(query="CREATE SCHEMA IF NOT EXISTS extra;")
    db.perform(query="DROP TABLE IF EXISTS %s" % table_full_name)
    table_name = table_full_name.split('.')[1]
    subprocess.run(
        f'PGPASSFILE=~/.pgpass_{db_config["dbname"]} pg_restore -U {db_config["user"]} --schema-only -h {db_config["host"]} -n basic -d {db_config["dbname"]} -t {table_name} {"/app/src/data/input/dump.tar"}',
        shell=True,
        check=True,
    )
    # TODO: Temp fix here only to convert poi.id a serial instead of integer
    db.perform(
        f"""
        CREATE SEQUENCE IF NOT EXISTS {table_name}_serial;
        ALTER TABLE {table_full_name} ALTER COLUMN id SET DEFAULT nextval('{table_name}_serial');
        """
    )

def return_tables_as_gdf(db_engine: Engine, tables: list):

    df_combined = gpd.GeoDataFrame()
    for table in tables:
        df = gpd.read_postgis(sql="SELECT * FROM %s" % table, con=db_engine, geom_col='way')
        df_combined = pd.concat([df_combined,df], sort=False).reset_index(drop=True)
    
    df_combined["osm_id"] = abs(df_combined["osm_id"])
    df_combined = df_combined.replace({np.nan: None})

    return df_combined


# Based on: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()
