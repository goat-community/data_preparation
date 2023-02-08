import geopandas as gpd
from sqlalchemy.engine.base import Connection as SQLAlchemyConnectionType
import polars as pl
from src.config.config import Config
from src.core.config import settings
from src.utils.utils import check_string_similarity_bulk, vector_check_string_similarity_bulk
import time 
import numpy as np

class PoiPreparation:
    """Class to preprare the POIs."""

    def __init__(self, db_config):

        self.db_config = settings.LOCAL_DATABASE_URI
        self.db_uri = f"postgresql://{self.db_config.user}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}{self.db_config.path}"

        self.root_dir = "/app"
        # self.dbname, self.host, self.username, self.port = (
        #     db_config["dbname"],
        #     db_config["host"],
        #     db_config["user"],
        #     db_config["port"],
        # )
        # self.db_config = db_config
        # self.db = Database(self.db_config)
        # self.sqlalchemy_engine = self.db.return_sqlalchemy_engine()
        self.config_pois = Config("poi")
        self.config_pois_preparation = self.config_pois.preparation

    def read_poi(self):

        column_names = """
        osm_id, name, brand, "addr:street" AS street, "addr:housenumber" AS housnumber, 
        "addr:postcode" AS zipcode, phone, website, opening_hours, operator, origin, organic, 
        subway, amenity, shop, tourism, railway, leisure, sport, highway, public_transport, tags::jsonb AS tags
        """
        sql_query = [
            f"""SELECT {column_names}, 'point' AS geom_type, ST_ASTEXT(way) AS geom_text FROM public.osm_poi_point""",
            f"""SELECT {column_names}, 'polygon' AS geom_type, ST_ASTEXT(ST_CENTROID(way)) AS geom_text FROM public.osm_poi_polygon""",
        ]
        df = pl.read_sql(sql_query, self.db_uri)
        return df

    def classify_poi(self, df):

        # Adding category column
        df = df.with_columns(pl.lit("str").alias("category"))
        
        ############################################################
        # Basic POI classification
        ############################################################
        
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
        # Classify bikesharing stations
        df = df.with_columns(
            pl.when(
                (pl.col("amenity") == "bicycle_rental")
                & (pl.col("shop") == None)
                & (pl.col("geom_type") == "point")
            )
            .then("bike_sharing")
            .otherwise(pl.col("category"))
            .alias("category")
        )

        
        ############################################################
        # Complex POI classification
        ############################################################
        
        # Apply function to classify Supermarkets
        df_supermarket = df.filter(pl.col("shop") == "supermarket")
        types_classify_by_name = list(self.config_pois_preparation["supermarket"]["classify_by_name"].keys())
        types_classify = list(self.config_pois_preparation["supermarket"]["classify_by_name"].keys())
        
        begin = time.time()
        
        # Classify by tag
        for key in self.config_pois_preparation["supermarket"]["classify_by_tag"]:  
            config_pair = self.config_pois_preparation["supermarket"]["classify_by_tag"][key]
            tag = list(config_pair.keys())[0]
            value = config_pair[tag]
            
            if value == "True":      
                df_supermarket = df_supermarket.with_columns(
                    pl.when(
                        (pl.col(tag) != None)
                    )        
                    .then(True)
                    .otherwise(False)
                    .alias(key + "_tag")
                )
            else: 
                df_supermarket = df_supermarket.with_columns(
                    pl.when(
                        (pl.col(tag) == value)
                    )        
                    .then(True)
                    .otherwise(False)
                    .alias(key + "_tag")
                ) 
                
            types_classify.append(key)
            
        arr_names = df_supermarket["name"].to_numpy()
        arr_brands = df_supermarket["brand"].to_numpy()
        
        # Classify by string similarity
        for key in types_classify_by_name:
            
            condition_obj = self.config_pois_preparation["supermarket"]["classify_by_name"][key]["children"]
            string_similarity = self.config_pois_preparation["supermarket"]["classify_by_name"][key]["string_similarity"]
            
            check_brands = vector_check_string_similarity_bulk(arr_names, condition_obj, string_similarity)
            check_names = vector_check_string_similarity_bulk(arr_brands, condition_obj, string_similarity)
            
            # Check if name or brand is true
            check_both = np.logical_or(check_brands, check_names)
            df_supermarket = df_supermarket.with_columns(pl.Series(name=key + "_similarity", values=check_both))

        # Classify categories
        for key in types_classify:
            # Assign category
            df_supermarket = df_supermarket.with_columns(
                pl.when(
                    pl.col(key) == True
                ).then(key)
                .otherwise(pl.col("category"))
                .alias("category")
            )
            # Drop not need columns 
            df_supermarket = df_supermarket.drop(key)
            
        
        end = time.time()
        print(f"Time to classify supermarkets: {end - begin}")
        
        # Convert polars to pandas dataframe
        pdf = df_supermarket.to_pandas()
        # Convert pandas dataframe to geopandas dataframe
        gdf = gpd.GeoDataFrame(
            pdf,
            geometry=gpd.GeoSeries.from_wkt(
                pdf["geom_text"], index=None, crs="EPSG:4326"
            ),
            crs="EPSG:4326",
        )
        # Geopandas dataframe to geopackage
        gdf.to_file("poi.gpkg", driver="GPKG")

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
            df = gpd.read_postgis(
                sql="SELECT * FROM %s" % table, con=db_engine, geom_col="way"
            )
            df_combined = pd.concat([df_combined, df], sort=False).reset_index(
                drop=True
            )

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
    # db = Database(DATABASE)
    poi_preparation = PoiPreparation(settings.LOCAL_DATABASE_URI)
    df = poi_preparation.read_poi()
    poi_preparation.classify_poi(df)


if __name__ == "__main__":
    main()
