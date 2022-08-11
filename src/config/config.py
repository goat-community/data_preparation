import sys
import yaml
import os
from pathlib import Path
import pandas as pd
import geopandas as gpd
from src.config.osm_dict import OSM_tags, OSM_germany
from src.other.utility_functions import file2df
from src.other.utils import print_info, download_link


class Config:
    def __init__(self, name):
        self.root_dir = "/app"
        with open(
            os.path.join(self.root_dir, "src", "config", "config.yaml"),
            encoding="utf-8",
        ) as stream:
            config = yaml.safe_load(stream)

        self.config = config["VARIABLES_SET"]
        self.name = name
        self.pbf_data = self.config[name].get("region_pbf")
        self.collection = self.config[name].get("collection")
        self.preparation = self.config[name].get("preparation")
        self.fusion = self.config[name].get("fusion")
        self.update = self.config[name].get("update")

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

    def fusion_key_set(self, typ):
        fus = self.fusion
        try:
            key_set = fus["fusion_data"]["source"][typ].keys()
        except:
            key_set = []
        return key_set

    def fusion_set(self, typ, key):
        fus = self.fusion["fusion_data"]["source"][typ][key]
        fus_set = (
            fus["amenity"],
            fus["amenity_set"],
            fus["amenity_operator"],
            fus["columns2rename"],
            fus["column_set_value"],
            fus["columns2fuse"],
        )
        return fus_set

    def fusion_type(self, typ, key):
        fus = self.fusion["fusion_data"]["source"][typ][key]
        fus_type = fus["fusion_type"]
        return fus_type

    def get_areas_by_rs(self, con, buffer, process="fusion"):

        # Returns study area as df from remote db (germany_municipalities) according to rs code
        def study_area_remote2df(con, rs):
            query = "SELECT * FROM sub_study_area WHERE rs = '%s'" % rs
            df_area = gpd.read_postgis(con=con, sql=query, geom_col="geom")
            df_area = df_area.filter(["geom"], axis=1)

            return df_area

        def study_area_file2df(rs_set):
            filename = "germany_municipalities.gpkg"
            df_rs = file2df(filename)
            df_bool = df_rs.rs.isin(rs_set)
            df_res = df_rs[df_bool]
            df_res = df_res.filter(["geometry"], axis=1)
            return df_res

        if process == "fusion":
            rs_set = self.fusion["rs_set"]
        elif process == "update":
            rs_set = self.update["rs_set"]
        else:
            print("Process not defined! Choose 'fusion' or 'update'.")

        try:
            list_areas = []
            for rs in rs_set:
                df_area = study_area_remote2df(con, rs)
                list_areas.append(df_area)
            df_area_union = pd.concat(list_areas, sort=False).reset_index(drop=True)
        except:
            try:
                df_area_union = study_area_file2df(rs_set)
                print("File extraction..")
            except:
                print(
                    "Please make sure that in remote DB is table 'sub_study_area' or in folder data/input is 'germany_municipalities.gpkg' file."
                )
                sys.exit()
        # if column geometry rename to geom
        try:
            df_area_union = df_area_union.rename(columns={"geom": "geometry"})
        except:
            pass

        df_area_union["dis_field"] = 1
        df_area_union = df_area_union.dissolve(by="dis_field")
        area_union_buffer = df_area_union
        area_union_buffer = area_union_buffer.to_crs(31468)
        area_union_buffer["geometry"] = area_union_buffer["geometry"].buffer(buffer)
        area_union_buffer = area_union_buffer.to_crs(4326)
        buffer_serie = area_union_buffer.difference(df_area_union)
        df_buffer_area = gpd.GeoDataFrame(geometry=buffer_serie)
        df_buffer_area = df_buffer_area.set_crs("epsg:4326")
        df_buffer_area = df_buffer_area.reset_index(drop=True)
        # df_buffer_area = df_buffer_area.rename(columns={"geometry":"geom"})
        df = pd.concat([df_area_union, df_buffer_area], sort=False).reset_index(
            drop=True
        )
        df["dis_field"] = 1
        df = df.dissolve(by="dis_field").reset_index(drop=True)
        return df
