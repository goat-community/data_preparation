import os
import shutil
import subprocess
from cdifflib import CSequenceMatcher
import difflib
import diff_match_patch

from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import geopandas as gpd
import numpy as np
import pandas as pd
from rich import print as print
from shapely.geometry import MultiPolygon
from sqlalchemy.engine.base import Engine
from src.db.db import Database
from functools import wraps
import time

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        total_time = te - ts
        if total_time > 1:
            total_time = round(total_time, 2)
            total_time_string = f"{total_time} seconds"
        elif total_time > 0.001:
            time_miliseconds = int((total_time) * 1000)
            total_time_string = f"{time_miliseconds} miliseconds"
        else:
            time_microseconds = int((total_time) * 1000000)
            total_time_string = f"{time_microseconds} microseconds"
        print(f"func: {f.__name__} took: {total_time_string}")

        return result

    return wrap

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

def similarity_ratio(string1, string2):
    dmp = diff_match_patch.diff_match_patch()
    diff = dmp.diff_main(string1, string2)
    matching_text = sum(len(text) for op, text in diff if op == 0)
    total_length = len(string1) + len(string2)
    return 2.0 * matching_text / total_length

@timing
def check_string_similarity(
    input_value: str, match_values: list[str], target_ratio: float
) -> bool:
    """Check if a string is similar to a list of strings.

    Args:
        input_value (str): Input value to check.
        match_values (list[str]): List of strings to check against.
        target_ratio (float): Target ratio to match.

    Returns:
        bool: True if the input value is similar to one of the match values.
    """    

    for match_value in match_values:
        if input_value in match_value or match_value in input_value:
            return True
        elif CSequenceMatcher(None, input_value, match_value).ratio() >= target_ratio:
            return True
        else:
            pass
    return False

@timing
def check_string_similarity_new(
    input_value: str, match_values: list[str], target_ratio: float
) -> bool:
    """Check if a string is similar to a list of strings.

    Args:
        input_value (str): Input value to check.
        match_values (list[str]): List of strings to check against.
        target_ratio (float): Target ratio to match.

    Returns:
        bool: True if the input value is similar to one of the match values.
    """    

    for match_value in match_values:
        if input_value in match_value or match_value in input_value:
            return True
        elif similarity_ratio(input_value, match_value) >= target_ratio:
            return True
        else:
            pass
    return False


input = "edekmarkt"
match = ["edeka"]

check_string_similarity(input, match, 0.7)
check_string_similarity_new(input, match, 0.7)

def check_string_similarity_bulk(input_value: str, match_dict: dict, target_ratio: float) -> bool:
    """Check if a string is similar to a dictionary with lists of strings.

    Args:
        input_value (str): Input value to check.
        match_dict (dict): Dictionary with lists of strings to check against.
        target_ratio (float): Target ratio to match.

    Returns:
        bool: True if the input value is similar to one of the match values.
    """        
    if input_value is None:
        return False
    
    for key, match_values in match_dict.items():
        if check_string_similarity(match_values=match_values, input_value=input_value.lower(), target_ratio=target_ratio):
            return True 
    return False

vector_check_string_similarity_bulk = np.vectorize(check_string_similarity_bulk)

def create_pgpass(db_config):
    """Creates pgpass file for specified DB config

    Args:
        db_config: Database configuration.
    """
    db_name = db_config.path[1:]
    delete_file(f"""~/.pgpass_{db_name}""")
    os.system(
        "echo "
        + ":".join(
            [
                db_config.host,
                str(db_config.port),
                db_name,
                db_config.user,
                db_config.password,
            ]
        )
        + f""" > ~/.pgpass_{db_name}"""
    )
    os.system(f"""chmod 600  ~/.pgpass_{db_name}""")


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
            f"""PGPASSFILE=~/.pgpass_{db_config["dbname"]} pg_dump -h {db_config["host"]} -t {table_name} {data_only_tag} --no-owner -U {db_config["user"]} {db_config["dbname"]} > {dir_output}""",  # -F t
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
    table_name = table_full_name.split(".")[1]
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
        df = gpd.read_postgis(
            sql="SELECT * FROM %s" % table, con=db_engine, geom_col="way"
        )
        df_combined = pd.concat([df_combined, df], sort=False).reset_index(drop=True)

    df_combined["osm_id"] = abs(df_combined["osm_id"])
    df_combined = df_combined.replace({np.nan: None})

    return df_combined


def download_dir(self, prefix, local, bucket, client):
    """Downloads data directory from AWS S3
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


def parse_poly(dir):
    """Parse an Osmosis polygon filter file.
    Based on: https://wiki.openstreetmap.org/wiki/Osmosis/Polygon_Filter_File_Python_Parsing
    Args:
        dir (str): Path to the polygon filter file.

    Returns:
        (shapely.geometry.multipolygon): Returns the polygon in the poly foramat as a shapely multipolygon.
    """
    in_ring = False
    coords = []
    with open(dir, "r") as polyfile:
        for (index, line) in enumerate(polyfile):
            if index == 0:
                # first line is junk.
                continue

            elif index == 1:
                # second line is the first polygon ring.
                coords.append([[], []])
                ring = coords[-1][0]
                in_ring = True

            elif in_ring and line.strip() == "END":
                # we are at the end of a ring, perhaps with more to come.
                in_ring = False

            elif in_ring:
                # we are in a ring and picking up new coordinates.
                ring.append(list(map(float, line.split())))

            elif not in_ring and line.strip() == "END":
                # we are at the end of the whole polygon.
                break

            elif not in_ring and line.startswith("!"):
                # we are at the start of a polygon part hole.
                coords[-1][1].append([])
                ring = coords[-1][1][-1]
                in_ring = True

            elif not in_ring:
                # we are at the start of a polygon part.
                coords.append([[], []])
                ring = coords[-1][0]
                in_ring = True

        return MultiPolygon(coords)
