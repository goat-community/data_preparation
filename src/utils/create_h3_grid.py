import os
import sys
import h3.api.numpy_int as h3
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from db.db import Database
from src.db.config import DATABASE_RD
from other.utility_functions import database_table2df
import json 

class H3Grid:   
    """"This class is used to create a hexgon grid using Uber H3 grid index."""
    def __init__(self):
        self.data_dir = '/app/src/data/output'
    def create_geojson_from_bbox(self, top, left, bottom, right):
        """
        Creates a geojson-like dictionary for H3 from a bounding box
        """
        return {'type': 'Polygon', 'coordinates': [[[bottom, left], [bottom, right], [top, right], [top, left], [bottom, left]]]}
   
    def read_geojson_from_file(self, file_path):
        geojson = json.load(open(file_path))
        flipped_coordinates = []
        for i in geojson["features"][0]["geometry"]['coordinates'][0]:
            flipped_coordinates.append([i[1], i[0]])
            
        geojson["features"][0]["geometry"]['coordinates'] = [flipped_coordinates]
        return geojson["features"][0]["geometry"]
    
    def create_grid(self, polygon, resolution, layer_name):
        """
        Creates a h3 grid for passed bounding box in SRID 4326
        """
        # Get hexagon ids for selected area and resolution
        hex_ids = h3.polyfill(polygon, resolution, geo_json_conformant=False)

        # Get hexagon geometries and convert to GeoDataFrame
        hex_polygons = lambda hex_id: Polygon(
                                h3.h3_to_geo_boundary(
                                    hex_id, geo_json=True)
                                )

        hex_polygons = gpd.GeoSeries(list(map(hex_polygons, hex_ids)), 
                                      crs="EPSG:4326" \
                                     )

        # Get parent ids of one level from hexagons           
        hex_parents = lambda hex_id: h3.h3_to_parent(hex_id, resolution - 1)
        hex_parents = pd.Series(list(map(hex_parents, hex_ids)))

        # Create series from hex_ids array
        hex_ids = pd.Series(hex_ids)
        gdf = gpd.GeoDataFrame(pd.concat([hex_ids,hex_parents,hex_polygons], keys=['id','parent_id','geometry'], axis=1))
        
        return gdf.astype({"id": int, "parent_id": int})
    
    def export_to_geopackage(self, gdf, layer_name):
        """
        Exports a GeoDataFrame to a geopackage
        """
        gdf.to_file('%s/%s.gpkg' % (self.data_dir,layer_name), driver='GPKG')
    
    def save_to_db(self, gdf, layer_name, engine):
        gdf.to_postgis(layer_name, engine, if_exists='replace', index=False)

# db = Database(DATABASE_RD)
# db_engine = db.return_sqlalchemy_engine()
# Create grid from bouding box example
#bbox = H3Grid().create_geojson_from_bbox(top=48.352598707539315, left=11.255493164062498, bottom=47.92738566360356, right=11.8927001953125)
#bbox = H3Grid().create_geojson_from_bbox(top=50.621, left=8.532, bottom=47.546, right=13.869)
# h3_grid = H3Grid()
# bbox = h3_grid.read_geojson_from_file('/app/src/data/input/clip.geojson')
# gdf = h3_grid.create_grid(polygon=bbox, resolution=9, layer_name='grid_visualization')
# h3_grid.save_to_db(gdf, layer_name='grid_visualization', engine=db_engine)
# gdf = h3_grid.create_grid(polygon=bbox, resolution=10, layer_name='grid_calculation')
# h3_grid.save_to_db(gdf, layer_name='grid_calculation', engine=db_engine)
# bbox = H3Grid().create_geojson_from_bbox(top=48.0710465709655708, left=7.6619009582867594, bottom=47.9035964575060191, right=7.9308570081842120)
# H3Grid().create_grid(polygon=bbox, resolution=9, layer_name='grid_visualization')
# H3Grid().create_grid(polygon=bbox, resolution=10, layer_name='grid_calculation')

# grid = H3Grid()
# bbox_coords = grid.create_grids_study_area_table('091620000')
# bbox = H3Grid().create_geojson_from_bbox(*bbox_coords)
# H3Grid().create_grid(polygon=bbox, resolution=9, layer_name='grid_visualization')
# H3Grid().create_grid(polygon=bbox, resolution=10, layer_name='grid_calculation')

