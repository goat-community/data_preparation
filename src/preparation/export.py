import argparse
import json
import logging
from pathlib import Path

import geopandas as gpd
import h3
from shapely.geometry import Polygon

from src.core.config import settings
from src.db.db import Database
from src.utils.utils import prepare_mask, print_error, print_info


class Export:
    """
    Export data from the database to parquet files. Each layer is tiled with H3 based and stored in a separate parquet file.
    The parquet files are stored in a directory structure that is compatible with H3 grid ids which can be loaded for later use.
    Optionally the data can be filtered by a mask geometry.
    """

    def __init__(
        self,
        db,
        layer_config: dict,
        mask_config: str,
        mask_buffer_distance: int = 0,
        h3_resolution: int = 6,
        output_dir: str = "../data/output",
    ):
        """Initialize the Export class

        Args:
            db (Database): Database Engine (local or remote)
            layer_config (dict): Layer configuration for the export. The keys are the layer names and the values are the SQL queries, GeoJSON or Shapefile file path.
            h3_resolution (int, optional): H3 Grid resolution. Defaults to 6.
            mask_config (dict, optional): Mask configuration as SQL query string, GeoJSON or Shapefile file path. Defaults to None.
            mask_buffer_distance (int, optional): Mask buffer distance in meters. Defaults to None.
            output_dir (str, optional): Output directory. Defaults to "../data/output".
        """
        self.db = db
        self.layer_config = layer_config
        self.h3_resolution = h3_resolution
        self.mask_config = mask_config
        self.output_dir = output_dir
        self.mask_buffer_distance = mask_buffer_distance


    def _create_h3_indexes(self, mask_gdf: gpd.GeoDataFrame):
        """Create a list of H3 indexes

        Args:
            mask_gdf ([GeoDataFrame]): Mask geometries

        Returns:
            [GeoDataFrame]: Returns a GeoDataFrame with the H3 indexes and the corresponding geometries
        """
        h3_indexes = []
        for row in mask_gdf.__geo_interface__["features"]:
            h3_index = h3.polyfill(row["geometry"], self.h3_resolution)
            h3_indexes.extend(h3_index)
        h3_indexes = list(set(h3_indexes))
        h3_indexes_gdf = gpd.GeoDataFrame(
            columns=["h3_index", "geometry"], geometry="geometry", crs="EPSG:4326"
        )
        h3_indexes_gdf["h3_index"] = h3_indexes
        h3_indexes_gdf["geometry"] = h3_indexes_gdf["h3_index"].apply(
            lambda x: Polygon(h3.h3_to_geo_boundary(h=x))
        )
        return h3_indexes_gdf

    def _read_from_postgis(self, input_sql: str, clip: str = None):
        """Read a layer from the database

        Args:
            input_sql (str): SQL query
            clip (str, optional): Clip geometry as WKT. Defaults to None.

        Returns:
            [GeoDataFrame]: Returns a GeoDataFrame with the layer data
        """
        if clip:
            query_sql = (
                input_sql
                + " WHERE ST_Intersects(ST_SETSRID(ST_GEOMFROMTEXT('"
                + clip
                + "'), 4326), geom)"
            )
        else:
            query_sql = input_sql
        h3_gdf = gpd.read_postgis(query_sql, self.db)
        return h3_gdf

    def _export_layers(self, h3_indexes_gdf: gpd.GeoDataFrame):
        """Export the layers to parquet files

        Args:
            h3_indexes_gdf ([GeoDataFrame]): H3 indexes
        """
        layer_input = {}
        for layer_name, layer_source in self.layer_config.items():
            if Path(layer_source).is_file():
                layer_input[layer_name] = gpd.read_file(layer_source)
            else:
                layer_input[layer_name] = layer_source

        for index, row in h3_indexes_gdf.iterrows():
            print_info(f"Processing H3 index {row['h3_index']}")
            h3_output_file_dir = Path(self.output_dir, row["h3_index"])
            h3_output_file_dir.mkdir(parents=True, exist_ok=True)
            for layer_name, layer_source in layer_input.items():
                print(f"Processing {layer_name} for H3 index {row['h3_index']}")
                try:
                    if isinstance(layer_source, str):
                        h3_gdf = self._read_from_postgis(layer_source, row["geometry"].wkt)
                    else:
                        h3_gdf = gpd.clip(layer_source, row["geometry"])
                    h3_gdf.to_parquet(h3_output_file_dir / (layer_name + ".parquet"))
                except Exception as e:
                    message = f'Processing {layer_name} for H3 index {row["h3_index"]}'
                    print_error(message)
                    logging.error(message)
                    
                    

    def run(self):
        """Run the export"""
        print_info("PREPARING MASK")
        mask_gdf = prepare_mask(self.mask_config, self.mask_buffer_distance, self.db)
        print_info("CREATING H3 INDEXES")
        h3_indexes_gdf = self._create_h3_indexes(mask_gdf)
        print_info("EXPORTING LAYERS")
        self._export_layers(h3_indexes_gdf)
        print_info("DONE")


def main():
    parser = argparse.ArgumentParser(description='Export data from the database to parquet files.')
    parser.add_argument('--input_config', type=str, required=True, help='Json file containing input configuration')
    parser.add_argument('--output_dir', type=str, default='../data/output', help='Output directory')
    args = parser.parse_args()
    with open(args.input_config, 'r') as f:
        input_config = json.load(f)
    mask_config = input_config["mask_config"]
    h3_resolution = input_config["h3_resolution"]
    mask_buffer_distance = input_config["mask_buffer_distance"]
    layer_config = input_config["layer_config"]
    db = Database(settings.REMOTE_DATABASE_URI)
    db.return_sqlalchemy_engine()
    Export(db.return_sqlalchemy_engine(), layer_config, mask_config, mask_buffer_distance, h3_resolution, args.output_dir).run()

if __name__ == '__main__':
    main()


# Example input_config.json
# {
#     "mask_config": "/app/src/data/input/munich_study_area.geojson",
#     "h3_resolution": 8,
#     "mask_buffer_distance": 50,
#     "layer_config": { 
#         "poi": """SELECT uid, category, name, street, housenumber, zipcode, opening_hours, wheelchair, tags, geom FROM basic.poi""",
#         "population": """SELECT id, population, demography, building_id, sub_study_area_id, geom FROM basic.population p""",
#         "aoi": """SELECT id, category, name, opening_hours, wheelchair, tags, geom  FROM basic.aoi p""",
#     },
# }

# Example Run the script
# python3 -m src.preparation.export --input_config /app/src/preparation/input_config.json --output_dir /app/src/data/output