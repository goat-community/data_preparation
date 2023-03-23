import geopandas as gpd
import numpy as np
import polars as pl
from sqlalchemy.engine.base import Connection as SQLAlchemyConnectionType

from src.config.config import Config
from src.core.config import settings
from src.utils.utils import vector_check_string_similarity_bulk
from src.utils.utils import timing, polars_df_to_postgis, create_table_dump
from src.db.db import Database
from src.preparation.subscription import Subscription

class PoiPreparation:
    """Class to prepare the POIs from OpenStreetMap."""

    def __init__(self, db: Database, region: str):
        """Constructor method.

        Args:
            db (Database): Database object
        """
        self.db = db
        self.db_config = self.db.db_config
        self.db_uri = f"postgresql://{self.db_config.user}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}{self.db_config.path}"
        self.engine = self.db.return_sqlalchemy_engine()
        self.root_dir = "/app"

        self.config_pois = Config("poi", region)
        self.config_pois_preparation = self.config_pois.preparation

    @timing
    def read_poi(self) -> pl.DataFrame:
        """Reads the POIs from the database from the OSM point and OSM polygon table.

        Returns:
            pl.DataFrame: Polars DataFrame with the POIs. The geometry is stored as WKT inside a column called geom.
        """

        # Relevant column names
        column_names = """
        osm_id::bigint, name, brand, "addr:street" AS street, "addr:housenumber" AS housenumber, 
        "addr:postcode" AS zipcode, phone, website, opening_hours, operator, origin, organic, 
        subway, amenity, shop, tourism, railway, leisure, sport, highway, public_transport, tags::jsonb AS tags
        """

        # Read POIs from database
        sql_query = [
            f"""SELECT {column_names}, 'n' AS osm_type, ST_ASTEXT(way) AS geom FROM public.osm_poi_point""",
            f"""SELECT {column_names}, 'w' AS osm_type, ST_ASTEXT(ST_CENTROID(way)) AS geom FROM public.osm_poi_polygon""",
        ]
        df = pl.read_sql(sql_query, self.db_uri)
        return df

    @timing
    def check_by_tag(
        self, df: pl.DataFrame, poi_config: dict, key: str, new_column_names: list[str]
    ) -> list[pl.DataFrame, list[str]]:  
        """Checks if a POI has a specific value for a tag.

        Args:
            df (pl.DataFrame): POIs to classify
            poi_config (dict): Configuration for poi category
            key (str): POI category to assign if check is True
            new_column_names (list[str]): List with column names of already classified POIs

        Raises:
            ValueError: Raise error if tag does not exist

        Returns:
            list[pl.DataFrame, list[str]]: Classified POIs and the list with new column names.
        """

        # Get tag and values
        tag = list(poi_config.keys())[0]
        values = poi_config[tag]

        # Check if values are True then only check if tag has value
        if new_column_names == []:
            when_condition = pl.when((pl.col(tag) != None))
        elif new_column_names != [] and values == [True]:
            when_condition = pl.when(
                (pl.col(tag) != None) & ~pl.any(pl.col(new_column_names) == True)
            )
        # Check if values are in list of conditions
        elif new_column_names == [] and values != [True]:
            values = [x.lower() for x in values]
            when_condition = pl.when((pl.col(tag).str.to_lowercase().is_in(values)))
        elif new_column_names != [] and values != [True]:
            values = [x.lower() for x in values]
            when_condition = pl.when(
                (pl.col(tag).str.to_lowercase().is_in(values))
                & ~pl.any(pl.col(new_column_names) == True)
            )
        else:
            raise ValueError("Conditions for tags cannot be set.")

        # Apply condition
        df = df.with_columns(
            when_condition.then(True).otherwise(False).alias(key + "___tag")
        )

        # Add new column name to list
        new_column_names.append(key + "___tag")
        return df, new_column_names

    @timing
    def check_by_name_in(
        self,
        df: pl.DataFrame,
        poi_config: list[str],
        key: str,
        new_column_names: list[str],
    ) -> list[pl.DataFrame, list[str]]:
        """Checks if a POI has a specific value in the name, brand or website.

        Args:
            df (pl.DataFrame): POIs to classify
            poi_config (list[str]): Configuration for poi category
            key (str): POI category to assign if check is True
            new_column_names (list[str]): List with column names of already classified POIs

        Returns:
            list[pl.DataFrame, list[str]]: Classified POIs and the list with new column names.
        """

        # Make sure that all values are lower case
        values = [x.lower() for x in poi_config]

        # Merge array to text with | as seperator for regex
        values_joined = "|".join(values)

        # Check if values are in list of conditions
        when_condition = pl.when(
            ~pl.any(pl.col(new_column_names) == True)
            & (
                (pl.col("name").str.to_lowercase().is_in(values))
                | (pl.col("brand").str.to_lowercase().is_in(values))
                | (pl.col("website").str.to_lowercase().is_in(values))
                | (pl.col("name").str.to_lowercase().str.contains(values_joined))
                | (pl.col("brand").str.to_lowercase().str.contains(values_joined))
                | (pl.col("website").str.to_lowercase().str.contains(values_joined))
            )
        )

        df = df.with_columns(
            when_condition.then(True).otherwise(False).alias(key + "___name_in")
        )

        # Add new column name to list
        new_column_names.append(key + "___name_in")
        return df, new_column_names

    @timing
    def check_name_similarity(
        self,
        df: pl.DataFrame,
        poi_config: dict,
        threshold: float,
        key: str,
        new_column_names: list[str],
    ) -> list[pl.DataFrame, list[str]]:
        """Checks if a POI has a similar name or brand to a list of POIs.

        Args:
            df (pl.DataFrame): POIs to classify
            poi_config (dict): Configuration for poi category
            threshold (float): Threshold for similarity. Higher is stricter.
            key (str): POI category to assign if check is True
            new_column_names (list[str]): List with column names of already classified POIs

        Returns:
            list[pl.DataFrame, list[str]]: Classified POIs and the list with new column names.
        """        

        df_unclassified = df.filter(~pl.any(pl.col(new_column_names) == True))
        df_classified = df.filter(pl.any(pl.col(new_column_names) == True))

        arr_names = df_unclassified["name"].to_numpy()
        arr_brands = df_unclassified["brand"].to_numpy()

        # Check if name or brand is similar
        check_brands = vector_check_string_similarity_bulk(
            arr_names, poi_config, threshold
        )
        check_names = vector_check_string_similarity_bulk(
            arr_brands, poi_config, threshold
        )

        # Check if name or brand is true
        check_both = np.logical_or(check_brands, check_names)
        df_unclassified = df_unclassified.with_columns(
            pl.Series(name=key + "___name_similarity", values=check_both)
        )

        # Add new column to list
        new_column_names.append(key + "___name_similarity")

        df = pl.concat([df_unclassified, df_classified], how="diagonal")
        return df, new_column_names

    def classify_by_config(self, df: pl.DataFrame, category: str) -> pl.DataFrame:        
        """Classifies POIs by config file.

        Args:
            df (pl.DataFrame): POIs to classify.
            category (str): POI category to assign.

        Returns:
            pl.DataFrame: Classified POIs.
        """

        # New columns for classification
        new_column_names = []

        # Classify by tag
        config_by_tag = self.config_pois_preparation[category].get("classify_by_tag")
        if config_by_tag is not None:
            for key in config_by_tag:
                df, new_column_names = self.check_by_tag(
                    df=df,
                    poi_config=config_by_tag[key],
                    key=key,
                    new_column_names=new_column_names,
                )

        # Classify by name in list
        config_by_name = self.config_pois_preparation[category].get("classify_by_name")
        if config_by_name is not None:
            for key in config_by_name:
                # Merge all values of children
                poi_config = [
                    [k] + v for k, v in config_by_name[key]["children"].items()
                ]
                # Make one dimensional list
                poi_config = list(
                    set([item for sublist in poi_config for item in sublist])
                )

                # Check if name inside list or wise versa
                df, new_column_names = self.check_by_name_in(
                    df=df,
                    poi_config=poi_config,
                    key=key,
                    new_column_names=new_column_names,
                )

        # Classify by name and brand similarity
        if config_by_name is not None:
            for key in config_by_name:
                df, new_column_names = self.check_name_similarity(
                    df=df,
                    poi_config=config_by_name[key]["children"],
                    threshold=config_by_name[key]["threshold"],
                    key=key,
                    new_column_names=new_column_names,
                )

        # Classify categories
        for column_name in new_column_names:
            # Assign category
            value = column_name.split("__")[0]
            df = df.with_columns(
                pl.when(pl.col(column_name) == True)
                .then(value)
                .otherwise(pl.col("category"))
                .alias("category")
            )
            # Drop not need columns
            df = df.drop(column_name)

        unmatched_category = self.config_pois_preparation[category].get("unmatched")
        df = df.with_columns(
            pl.when(pl.col("category") == "str")
            .then(unmatched_category)
            .otherwise(pl.col("category"))
            .alias("category")
        )
        return df

    def classify_public_transport(self, df: pl.DataFrame) -> pl.DataFrame:
        """Classifies public transport POIs.

        Args:
            df (pl.DataFrame): POIs to classify.

        Returns:
            pl.DataFrame: Classified POIs.
        """        
        
        # Classify bus stops
        df = df.with_columns(
            pl.when(
                (pl.col("highway") == "bus_stop")
                & (pl.col("name").is_not_null())
                & (pl.col("category") == "str")
            )
            .then("bus_stop")
            .otherwise(pl.col("category"))
            .alias("category")
        )

        df = df.with_columns(
            pl.when(
                (pl.col("public_transport") == "platform")
                & (pl.col("name").is_not_null())
                & (pl.col("tags").str.json_path_match(r"$.bus") == "yes")
                & (pl.col("category") == "str")
            )
            .then("bus_stop")
            .otherwise(pl.col("category"))
            .alias("category")
        )

        # Classify tram stops
        df = df.with_columns(
            pl.when(
                (pl.col("public_transport") == "stop_position")
                & (pl.col("name").is_not_null())
                & (pl.col("tags").str.json_path_match(r"$.tram") == "yes")
            )
            .then("tram_stop")
            .otherwise(pl.col("category"))
            .alias("category")
        )

        # Classify railway stations
        df = df.with_columns(
            pl.when(
                (pl.col(["railway"]) == "station")
                & (pl.col("name").is_not_null())
                & (pl.col("tags").str.json_path_match(r"$.disused") == None)
                & (pl.col("tags").str.json_path_match(r"$.railway:disused") == None)
                & (pl.col("tags").str.json_path_match(r"$.usage") != "tourism")
            )
            .then("rail_station")
            .otherwise(pl.col("category"))
            .alias("category")
        )

        df = df.with_columns(
            pl.when(
                (
                    pl.any(
                        pl.col(["railway", "public_transport"]).is_in(
                            ["stop", "station", "stop_position"]
                        )
                    )
                )
                & ~(pl.col("railway").is_in(["disused_station", "proposed"]))
                & (pl.col("name").is_not_null())
                & (
                    (pl.col("tags").str.json_path_match(r"$.train") == "yes")
                    | (pl.col("tags").str.json_path_match(r"$.rail") == "yes")
                )
                & (pl.col("tags").str.json_path_match(r"$.disused") == None)
                & (pl.col("tags").str.json_path_match(r"$.usage") != "tourism")
            )
            .then("rail_station")
            .otherwise(pl.col("category"))
            .alias("category")
        )
        
        # Classify subway entrances with names
        df = df.with_columns(
            pl.when(
                (pl.col("railway") == "subway_entrance")
                & (pl.col("name").is_not_null())
            )
            .then("subway_entrance")
            .otherwise(pl.col("category"))
            .alias("category")
        )
        
        # Classify subway stations without names by assigning the name of the nearest subway station
        # Get subway stations
        pdf_subway_stations = df.filter(
            (pl.col("railway") == "station")
            & (pl.col("subway") == "yes")
            & (pl.col("railway") != "proposed")
        ).to_pandas()

        # Filter subway entrances
        df_subway_entrances = df.filter(
            (pl.col("railway") == "subway_entrance") & (pl.col("category") == "str")
        )

        # Get dataframe without subway entrances
        df = df.filter(
            ~((pl.col("railway") == "subway_entrance") & (pl.col("category") == "str"))
        )

        # Convert pandas dataframe to geopandas dataframe
        pdf_subway_entrances = df_subway_entrances.to_pandas()
        gdf_subway_stations = gpd.GeoDataFrame(
            pdf_subway_stations,
            geometry=gpd.GeoSeries.from_wkt(
                pdf_subway_stations["geom"], crs="EPSG:4326"
            ),
            crs="EPSG:4326",
        ).to_crs(3857)

        # Convert pandas dataframe to geopandas dataframe
        gdf_subway_entrances = gpd.GeoDataFrame(
            pdf_subway_entrances,
            geometry=gpd.GeoSeries.from_wkt(
                pdf_subway_entrances["geom"], crs="EPSG:4326"
            ),
            crs="EPSG:4326",
        ).to_crs(3857)

        # Join subway stations to subway entrances and assign the nearest subway station name to the subway entrance
        # Converting to numpy array and then to normal list due to problem with type conversion in polars
        name_to_assign = list(
            gdf_subway_entrances.sjoin_nearest(
                gdf_subway_stations[["geometry", "name"]],
                how="left",
                max_distance=350,
                distance_col="distance",
            )["name_right"].to_numpy()
        )

        # Assigning the name to the subway entrance
        df_subway_entrances = (
            df_subway_entrances.with_columns(
                pl.Series(name="new_name", values=name_to_assign, dtype=pl.Utf8)
            )
            .with_columns(
                pl.when(pl.col("new_name") != "nan")
                .then(pl.col("new_name"))
                .otherwise(pl.col("name"))
                .alias("name")
            )
            .with_columns(
                pl.when((pl.col("name") != None))
                .then("subway_entrance")
                .otherwise(pl.col("category"))
                .alias("category")
            )
        )
        df_subway_entrances = df_subway_entrances.drop("new_name")

        # Concatenate the dataframes
        df = pl.concat(
            [df, df_subway_entrances.filter(pl.col("name").is_not_null())],
            how="diagonal",
        )
        return df
        
    def classify_poi(self, df):

         
        # Create dictionary with empty lists to track classified tags
        classified_tags = {k: [] for k in self.config_pois.collection["osm_tags"].keys()}
        
        # Adding category column
        df = df.with_columns(pl.lit("str").alias("category"))

        # Classify Playgrounds
        df = df.with_columns(
            pl.when(
                (pl.col("leisure") == "playground")
                | (pl.col("amenity") == "playground")
            )
            .then("playground")
            .otherwise(pl.col("category"))
            .alias("category")
        )
        classified_tags["leisure"].append("playground")
        classified_tags["amenity"].append("playground")
        
        # Classify bikesharing stations
        df = df.with_columns(
            pl.when(
                (pl.col("amenity") == "bicycle_rental")
                & (pl.col("shop") == None)
                & (pl.col("osm_type") == "n")
            )
            .then("bike_sharing")
            .otherwise(pl.col("category"))
            .alias("category")
        )
        classified_tags["amenity"].append("bicycle_rental")

        # Classify gyms
        df = df.with_columns(
            pl.when(
                (pl.col("leisure") == "fitness_centre")
                | ((pl.col("leisure") == "sports_centre") & (pl.col("sport") == "fitness"))
                & (pl.col("sport").is_in(["multi", "fitness"]) | (pl.col("sport") == None))
                & (pl.col("name").str.to_lowercase().str.contains("yoga") == False)
            )
            .then("gym")
            .otherwise(pl.col("category"))
            .alias("category")
        )
        classified_tags["leisure"].extend(["fitness_centre", "sports_centre"])
        classified_tags["sport"].extend(["fitness", "multi"])
        
        # Classify yoga studios
        df = df.with_columns(
            pl.when(
                (
                    (pl.col("sport") == "yoga")
                    | (pl.col("name").str.to_lowercase().str.contains("yoga"))
                )
                & (pl.col("shop") == None)
            )
            .then("yoga")
            .otherwise(pl.col("category"))
            .alias("category")
        ) 
        classified_tags["sport"].append("yoga")

        # Classify public transport
        df = self.classify_public_transport(df=df)
        classified_tags["public_transport"].extend(["stop_position", "station"])
        classified_tags["railway"].extend(["station", "platform", "stop", "tram_stop", "subway_entrance"])
        classified_tags["highway"].append("bus_stop")
    
        # Classify POIs by config
        df_classified_config = pl.DataFrame()

        # Loop through config
        for key in self.config_pois_preparation:
            # Check if config should be inherited
            if list(self.config_pois_preparation[key].keys())[0] == "inherit":
                category = self.config_pois_preparation[key]["inherit"]
            else:
                category = key

            df_classified = self.classify_by_config(
                df=df.filter(pl.col("shop") == key), category=category
            )
            df_classified_config = pl.concat(
                [df_classified_config, df_classified], how="diagonal"
            )
        
        # Remove rows classified by config
        df = df.filter(~pl.col("shop").is_in(list(self.config_pois_preparation.keys())))
        df = pl.concat([df_classified_config, df], how="diagonal")
        
        # Append classified categories to config
        classified_categories = self.config_pois_preparation.keys()
        classified_tags["shop"].extend(classified_categories)
        
        # Remove all categories from config that were already classified 
        cleaned_config_poi = self.config_pois.collection["osm_tags"]
        for key in cleaned_config_poi:
            cleaned_config_poi[key] = list(set(cleaned_config_poi[key]) - set(classified_tags[key]))    

        # Assign remaining categories 
        for key in cleaned_config_poi:
            for value in cleaned_config_poi[key]:
                df = df.with_columns(
                    pl.when(
                        (pl.col(key) == value)
                    )
                    .then(value)
                    .otherwise(pl.col("category"))
                    .alias("category")
                )
        
        return df 
    
def main():
    db = Database(settings.LOCAL_DATABASE_URI)
    db_rd = Database(settings.REMOTE_DATABASE_URI)
    poi_preparation = PoiPreparation(db=db, region="at")
    # df = poi_preparation.read_poi()
    # df = poi_preparation.classify_poi(df)

    # #db = Database(settings.REMOTE_DATABASE_URI)
    # engine = db.return_sqlalchemy_engine()
    # # Export to PostGIS
    # polars_df_to_postgis(
    #     engine=engine,
    #     df=df.filter(pl.col("category") != "str"),
    #     table_name="poi_osm",
    #     schema="temporal",
    #     if_exists="replace",
    #     geom_column="geom",
    #     srid=4326,
    #     create_geom_index=True,
    #     jsonb_column="tags",
    # )

    subscription = Subscription(db=db)
    # subscription.subscribe_osm()
    subscription.export_to_poi_schema()
    
    create_table_dump(db.db_config, 'basic.poi', 'dump', False)
    
if __name__ == "__main__":
    main()
