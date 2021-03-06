import os
import shutil
import subprocess
from rich import print as print
from urllib.request import urlopen, Request
from pathlib import Path


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
    print(f"INFO: {message}")


def print_warning(message: str):
    print(f"WARNING: {message}")


def download_link(directory, link):
    download_path = Path(directory) / os.path.basename(link)
    with urlopen(link) as image, download_path.open("wb") as f:
        f.write(image.read())
    print_info(f"Downloaded ended for {link}")


def create_pgpass_for_db(db_config):
    """Creates pgpass file for specified DB config

    Args:
        db_config (str): Pass a database config specified using the DATABASE object.
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


def create_table_dump(db_config, table_name):
    """Create a dump from a table

    Args:
        db_config (str): Pass a database config specified using the DATABASE object.
        table_name (str): Specify the table name including the schema.
    """
    try:
        dir_output = (
            os.path.abspath(os.curdir)
            + "/src/data/output/"
            + table_name.split(".")[1]
            + ".tar"
        )

        subprocess.run(
            f"""PGPASSFILE=~/.pgpass_{db_config["dbname"]} pg_dump -h {db_config["host"]} -t {table_name} -F t --no-owner -U {db_config["user"]} {db_config["dbname"]} > {dir_output}""",
            shell=True,
            check=True,
        )
    except Exception as e:
        print_warning(f"The following exeption happened when dumping {table_name}: {e}")
