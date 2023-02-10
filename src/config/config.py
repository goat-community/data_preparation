import sys
import yaml
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
from src.config.osm_dict import OSM_tags, OSM_germany
from src.utils.utils import print_info, download_link


class Config:
    """Reads the config file and returns the config variables.
    """    
    def __init__(self, name: str = "global"):
        #TODO: Add validation of config files here
        self.root_dir = "/app"
        
        # Read config for data set or read global config
        if name == "global":
            config_path = os.path.join(self.root_dir, "src", "config", "config" + ".yaml")
        else:
            config_path = os.path.join(self.root_dir, "src", "config", "data_variables", name + ".yaml")
        
        # Read config file
        with open(
            config_path,
            encoding="utf-8",
        ) as stream:
            config = yaml.safe_load(stream)
        self.config = config
        
        if name != "global":   
            self.name = name
            self.pbf_data = self.config.get("region_pbf")
            self.collection = self.config.get("collection")
            self.preparation = self.config.get("preparation")
        
    def osm2pgsql_create_style(self):
        add_columns = self.collection["additional_columns"]
        osm_tags = self.collection["osm_tags"]

        pol_columns = [tag for tag in osm_tags if tag in ("railway", "highway")]

        f = open(
            os.path.join(self.root_dir, "src", "config", "style_template.style"), "r"
        )
        sep = "#######################CUSTOM###########################"
        text = f.read()
        text = text.split(sep, 1)[0]

        f1 = open(
            os.path.join(
                self.root_dir, "src", "data", "temp", (self.name + "_p4b.style")
            ),
            "w",
        )
        f1.write(text)
        f1.write(sep)
        f1.write("\n")

        print_info(f"Creating osm2pgsql style file({self.name}_p4b.style)...")

        for column in add_columns:
            if column in pol_columns:
                style_line = f"node,way  {column}  text  polygon"
                f1.write(style_line)
                f1.write("\n")
            else:
                style_line = f"node,way  {column}  text  linear"
                f1.write(style_line)
                f1.write("\n")

        for tag in osm_tags:
            if tag in ["railway", "highway"]:
                style_line = f"node,way  {tag}  text  linear"
                f1.write(style_line)
                f1.write("\n")
            else:
                style_line = f"node,way  {tag}  text  polygon"
                f1.write(style_line)
                f1.write("\n")

    def download_db_schema(self):
        """Download database schema from PostGIS database."""
        download_link(
            self.root_dir + "/src/data/input", self.config["db_schema"], "dump.tar"
        )


