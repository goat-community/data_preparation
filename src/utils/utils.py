import os
import shutil
import subprocess
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import geopandas as gpd
import numpy as np
import pandas as pd
from rich import print as print
from sqlalchemy.engine.base import Engine

from src.db.db import Database


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
    print(f"[bold green]INFO[/bold green]: {message}")

def print_error(message: str):
    print(f"[bold red]ERROR[/bold red]: {message}")

def print_warning(message: str):
    print(f"[red magenta]WARNING[/red magenta]: {message}")

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


def download_dir(self, prefix, local, bucket, client):
    """ Downloads data directory from AWS S3
    Args:
        prefix (str): Path to the directory in S3
        local (str): Path to the local directory
        bucket (str): Name of the S3 bucket
        client (obj): S3 client object
    """
    keys = []
    dirs = []
    next_token = ""
    base_kwargs = {
        "Bucket": bucket,
        "Prefix": prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != "":
            kwargs.update({"ContinuationToken": next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get("Contents")
        for i in contents:
            k = i.get("Key")
            if k[-1] != "/":
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get("NextContinuationToken")
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)

def upload_dir(self, prefix, local, bucket, client):
    """ Uploads data directory to AWS S3
    Args:
        prefix (str): Path to the directory in S3
        local (str): Path to the local directory
        bucket (str): Name of the S3 bucket
        client (obj): S3 client object
    """
    for root, dirs, files in os.walk(local):
        for filename in files:
            # construct the full local path
            local_path = os.path.join(root, filename)
            

def prepare_mask(mask_config: str, buffer_distance: int = 0, db: Any = None):
    """Prepare mask geometries
    Args:
        mask_config (str): Path to a GeoJSON file or a PostGIS query
        buffer_distance (int, optional): Buffer distance in meters. Defaults to 0.
        db (Any, optional): Database connection. Defaults to None.
    Returns:
        [GeoDataFrame]: Returns a GeoDataFrame with the mask geometries as polygons
    """
    if Path(mask_config).is_file():
        mask_geom = gpd.read_file(mask_config)
    else:
        try:                
            mask_geom = gpd.GeoDataFrame.from_postgis(mask_config, db)
        except Exception as e:
            print_error(f"Error while reading mask geometry")
    mask_gdf = gpd.GeoDataFrame.from_features(mask_geom, crs="EPSG:4326")
    mask_gdf = mask_gdf.to_crs("EPSG:3857")
    mask_gdf.geometry = mask_gdf.geometry.buffer(buffer_distance)
    mask_gdf = mask_gdf.to_crs("EPSG:4326")
    mask_gdf = mask_gdf.explode(index_parts=True)
    return mask_gdf