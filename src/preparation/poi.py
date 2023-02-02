import time
import json
import numpy as np
import pandas as pd
import geopandas as gp
from src.db.config import DATABASE, DATABASE_RD
from src.db.db import Database

# from pandas.core.accessor import PandasDelegate
from src.config.config import Config
from src.other.utility_functions import gdf_conversion, table_dump
from src.other.utils import return_tables_as_gdf
from collection.osm_collection_base import OsmCollection
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import JSONB

from src.other.utils import create_table_schema, create_table_dump


# ================================== POIs preparation =============================================#
#!!!!!!!!!!!!!!! This codebase needs to be rewritten !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!#
def osm_poi_classification(df: gp.GeoDataFrame, config: dict):


    # Timer start
    print("Preparation started...")
    start_time = time.time()


  

    # This section getting var from conf class (variables container)
    var = config.preparation
    # Related to sport facilities
    sport_var_disc = var["sport"]["sport_var_disc"]
    leisure_var_add = var["sport"]["leisure_var_add"]
    leisure_var_disc = var["sport"]["leisure_var_disc"]
    # Related to Supermarkets
    health_food_var = var["health_food"]
    hypermarket_var = var["hypermarket"]
    no_end_consumer_store_var = var["no_end_consumer_store"]
    discount_supermarket_var = var["discount_supermarket"]
    supermarket_var = var["supermarket"]
    chemist_var = var["chemist"]
    organic_var = var["organic"]
    # Banks
    bank_var = var["bank"]
    # Related to Discount Gyms
    discount_gym_var = var["discount_gym"]

    # drop operator value for supermarkets
    df.loc[df["shop"] == "supermarket", "operator"] = ""

    # bycicle rental & doctors renaming
    df.loc[df.amenity == "bicycle_rental", "amenity"] = "bike_sharing"
    df.loc[df.amenity == "doctors", "amenity"] = "general_practitioner"

    # Iterate through the rows
    for i in df.index:
        df_row = df.iloc[i]

        # Gyms and discount gyms -> Fitness centers
        if (
            (
                df_row[i_leisure] == "fitness_centre"
                or (
                    df_row[i_leisure] == "sport_centre" and df_row[i_sport] == "fitness"
                )
            )
            and (df_row[i_sport] in ["multi", "fitness"] or not df_row[i_sport])
            and "yoga" not in df_row[i_name].lower()
        ):
            operator = poi_return_search_condition(
                df_row[i_name].lower(), discount_gym_var
            )
            if operator:
                df.iat[i, i_operator] = operator
                df.iat[i, i_amenity] = "discount_gym"
            else:
                df.iat[i, i_amenity] = "gym"
            continue

        # Yoga centers check None change here
        if (
            df_row[i_sport] == "yoga"
            or "yoga" in df_row[i_name]
        ) and not df_row[i_shop]:
            df.iat[i, i_amenity] = "yoga"
            continue

        # Recclasify shops. Define convenience and clothes, others assign to amenity. If not rewrite amenity with shop value
        if df_row[i_shop] == "grocery" and df_row[i_amenity] == "":
            if df_row[i_organic] == "only":
                df.iat[i, i_amenity] = "organic_supermarket"
                df.iat[i, i_tags]["organic"] = df_row[i_organic]
                operator = poi_return_search_condition(
                    df_row[i_name].lower(), organic_var
                )
                if operator:
                    df.iat[i, i_operator] = operator
                continue
            elif df_row[i_origin]:
                df.iat[i, i_amenity] = "international_hypermarket"
                df.iat[i, i_tags]["origin"] = df_row[i_origin]
                continue
            else:
                df.iat[i, i_amenity] = "convenience"
                df.iat[i, i_shop] = None
                continue

        elif df_row[i_shop] == "fashion" and df_row[i_amenity] == "":
            df.iat[i, i_amenity] = "clothes"
            df.iat[i, i_shop] = None
            continue

        # Supermarkets recclassification
        elif df_row[i_shop] == "supermarket" and df_row[i_amenity] == "":
            operator = [
                poi_return_search_condition(df_row[i_name].lower(), health_food_var),
                poi_return_search_condition(df_row[i_name].lower(), hypermarket_var),
                poi_return_search_condition(
                    df_row[i_name].lower(), no_end_consumer_store_var
                ),
                poi_return_search_condition(
                    df_row[i_name].lower(), discount_supermarket_var
                ),
                poi_return_search_condition(df_row[i_name].lower(), supermarket_var),
            ]
            if any(operator):
                for op in operator:
                    if op:
                        df.iat[i, i_operator] = op
                        o_ind = operator.index(op)
                        df.iat[i, i_amenity] = [
                            cat
                            for i, cat in enumerate(
                                [
                                    "health_food",
                                    "hypermarket",
                                    "no_end_consumer_store",
                                    "discount_supermarket",
                                    "supermarket",
                                ]
                            )
                            if i == o_ind
                        ][0]
                        continue
                    else:
                        pass
            else:
                if df_row[i_organic] == "only":
                    df.iat[i, i_amenity] = "organic_supermarket"
                    df.iat[i, i_tags]["organic"] = df_row[i_organic]
                    operator = poi_return_search_condition(
                        df_row[i_name].lower(), organic_var
                    )
                    if operator:
                        df.iat[i, i_operator] = operator
                    continue
                elif df_row[i_origin]:
                    df.iat[i, i_amenity] = "international_hypermarket"
                    df.iat[i, i_tags]["origin"] = df_row[i_origin]
                    continue
                # rewrite next block - move search condition to config, write function for search
                elif (
                    "müller " in df_row[i_name].lower()
                    or df_row[i_name].lower() == "müller"
                ):
                    df.iat[i, i_amenity] = "chemist"
                    df.iat[i, i_operator] = "müller"
                    continue
                elif "dm " in df_row[i_name].lower() or "dm-" in df_row[i_name].lower():
                    df.iat[i, i_amenity] = "chemist"
                    df.iat[i, i_operator] = "dm"
                    continue
                else:
                    df.iat[i, i_amenity] = "supermarket"
                    continue
        elif df_row[i_shop] == "chemist" and df_row[i_amenity] == "":
            operator = poi_return_search_condition(df_row[i_name].lower(), chemist_var)
            if operator:
                df.iat[i, i_operator] = operator
                df.iat[i, i_amenity] = "chemist"
                continue
            else:
                df.iat[i, i_amenity] = "chemist"
                continue
        elif df_row[i_shop] and df_row[i_shop] != "yes" and df_row[i_amenity] == "":
            df.iat[i, i_amenity] = df.iat[i, i_shop]
            df.iat[i, i_tags]["shop"] = df_row[i_shop]
            continue

        # Banks
        if df_row[i_amenity] == "bank":
            operator = poi_return_search_condition(df_row[i_name].lower(), bank_var)
            if operator:
                df.iat[i, i_operator] = operator
                continue

        # Transport stops
        if df_row[i_highway] == "bus_stop" and df_row[i_name] != "":
            df.iat[i, i_amenity] = "bus_stop"
            continue
        elif (
            df_row[i_public_transport] == "platform"
            and df_row[i_tags]
            and df_row[i_highway] != "bus_stop"
            and df_row[i_name] != ""
            and ("bus", "yes") in df_row[i_tags].items()
        ):
            df.iat[i, i_amenity] = "bus_stop"
            df.iat[i, i_tags]["public_transport"] = df_row[i_public_transport]
            continue
        elif (
            df_row[i_public_transport] == "stop_position"
            and isinstance(df_row[i_tags], dict)
            and ("tram", "yes") in df_row[i_tags].items()
            and df_row[i_name] != ""
        ):
            df.iat[i, i_amenity] = "tram_stop"
            df.iat[i, i_tags]["public_transport"] = df_row[i_public_transport]
            continue
        elif df_row[i_railway] == "subway_entrance":
            df.iat[i, i_amenity] = "subway_entrance"
            df.iat[i, i_tags]["railway"] = df_row[i_railway]
            continue
        elif (
            df_row[i_railway] == "stop"
            and df_row[i_tags]
            and ("train", "yes") in df_row[i_tags].items()
        ):
            df.iat[i, i_amenity] = "rail_station"
            df.iat[i, i_tags]["railway"] = df_row[i_railway]
            continue
        elif df_row[i_highway]:
            df.iat[i, i_tags]["highway"] = df_row[i_highway]
        elif df_row[i_public_transport]:
            df.iat[i, i_tags]["public_transport"] = df_row[i_public_transport]
        elif df_row[i_railway]:
            df.iat[i, i_tags]["railway"] = df_row[i_railway]
        elif df_row[i_subway]:
            df.iat[i, i_tags]["subway"] = df_row[i_subway]

    df = df.reset_index(drop=True)

    # # # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry="geom")
    df.crs = "EPSG:4326"

    # Filter subway entrances
    try:
        df_sub_stations = df[
            (df["public_transport"] == "station")
            & (df["subway"] == "yes")
            & (df["railway"] != "proposed")
        ]
        df_sub_stations = df_sub_stations[["name", "geom", "id"]]
        df_sub_stations = df_sub_stations.to_crs(31468)
        df_sub_stations["geom"] = df_sub_stations["geom"].buffer(250)
        df_sub_stations = df_sub_stations.to_crs(4326)

        df_sub_entrance = df[(df["amenity"] == "subway_entrance")]
        df_sub_entrance = df_sub_entrance[["name", "geom", "id"]]

        df_snames = gp.overlay(df_sub_entrance, df_sub_stations, how="intersection")
        df_snames = df_snames[["name_2", "id_1"]]
        df = (
            df_snames.set_index("id_1")
            .rename(columns={"name_2": "name"})
            .combine_first(df.set_index("id"))
        )
    except:
        print("No subway stations for given area.")
        df = df.drop(columns={"id"})

    # Remove irrelevant columns an rows with not defined amenity
    df = df.drop(
        columns={
            "shop",
            "tourism",
            "leisure",
            "sport",
            "highway",
            "origin",
            "organic",
            "public_transport",
            "railway",
            "subway",
        }
    )
    df = df.drop_duplicates(subset=["osm_id", "amenity", "name"], keep="first")
    df = df.drop(df[df.amenity == ""].index)

    # Timer finish
    print("Preparation took %s seconds ---" % (time.time() - start_time))

    return gp.GeoDataFrame(df, geometry="geom")



import geopandas as gpd
from sqlalchemy.engine.base import Connection as SQLAlchemyConnectionType
import polars as pl 

class PoisPreparation:
    """Class to preprare the POIs."""

    def __init__(self, db):
        self.root_dir = "/app"
        # self.dbname, self.host, self.username, self.port = (
        #     db_config["dbname"],
        #     db_config["host"],
        #     db_config["user"],
        #     db_config["port"],
        # )
        # self.db_config = db_config
        # self.db = Database(self.db_config)
        self.sqlalchemy_engine = self.db.return_sqlalchemy_engine()
        self.config_pois = Config("pois")
        self.config_pois_preparation = self.config_pois.preparation

    def read_poi(self):
        
    
    
    def perform_pois_preparation(self, db: SQLAlchemyConnectionType):
        """_summary_

        Args:
            db_reading (SQLAlchemyConnectionType): Database object to read the custom POI data.

        Returns:
            GeoDataFrame: the prepared POIs
        """

        created_tables = ["osm_pois_point", "osm_pois_polygon"]
        for table in created_tables:
            self.db.perform(
                f"ALTER TABLE {table} ALTER COLUMN tags TYPE jsonb USING tags::jsonb;"
            )

        df_combined = gpd.GeoDataFrame()
        for table in created_tables:
            df = gpd.read_postgis(sql="SELECT * FROM %s" % table, con=db_engine, geom_col='way')
            df_combined = pd.concat([df_combined,df], sort=False).reset_index(drop=True)
        
        df_combined["osm_id"] = abs(df_combined["osm_id"])
        df_combined = df_combined.replace({np.nan: None})
            
        poi_gdf = osm_poi_classification(poi_gdf, self.config_pois)
        create_table_schema(self.db, self.db_config, "basic.poi")

        poi_gdf = poi_gdf.reset_index(drop=True)
        copy_gdf = poi_gdf.copy()
        keys_for_tags = [
            "phone",
            "website",
            "operator",
            "source",
            "brand",
            "addr:city",
            "addr:country",
            "origin_geometry",
            "osm_id",
        ]

        poi_gdf.rename(
            columns={
                "amenity": "category",
                "addr:street": "street",
                "addr:postcode": "zipcode",
            },
            inplace=True,
        )
        poi_gdf.drop(
            columns=keys_for_tags,
            inplace=True,
        )
        
        # Match schema and put remaining attributes in tags
        loc_tags = poi_gdf.columns.get_loc("tags")
        for i in range(len(poi_gdf.index)):
            row = copy_gdf.iloc[i]

            new_tags = row["tags"]
            for key in keys_for_tags:
                if row[key] is not None:
                    new_tags[key] = str(row[key])

            poi_gdf.iat[i, loc_tags] = json.dumps(new_tags)

        
        # Upload to PostGIS
        poi_gdf.to_postgis(
            name="poi",
            con=self.sqlalchemy_engine,
            schema="basic",
            if_exists="append",
            index=False,
            dtype={"tags": JSONB},
        )
        return poi_gdf


def main():
    db = Database(DATABASE)
    poi_preparation = PoiPreparation()



# pois_preparation = PoisPreparation(DATABASE)
# db_reading = Database(DATABASE_RD)
# pois_preparation.perform_pois_preparation(db_reading)

# db = Database(DATABASE)
#create_table_schema(db, DATABASE, "basic.aoi")


# create_table_schema(db, DATABASE, "basic.building")
# create_table_schema(db, DATABASE, "basic.population")
# create_table_schema(db, DATABASE, "basic.study_area")
# create_table_schema(db, DATABASE, "basic.sub_study_area")


# create_table_schema(db, DATABASE, "basic.grid_calculation")
# create_table_schema(db, DATABASE, "basic.grid_visualization")
# create_table_schema(db, DATABASE, "basic.study_area_grid_visualization")
 # create_table_schema(db, DATABASE, "basic.poi")

# create_table_dump(db_config=DATABASE, table_name="basic.building", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.population", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.study_area", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.sub_study_area", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.grid_calculation", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.grid_visualization", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.study_area_grid_visualization", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.edge", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.node", data_only=True)
# create_table_dump(db_config=DATABASE, table_name="basic.poi", data_only=True)

# create_table_schema(db, DATABASE, "basic.aoi")