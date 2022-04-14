import os
import sys
import h3.api.numpy_int as h3
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from db.db import Database
from other.utility_functions import database_table2df

class H3Grid:   
    """"This class is used to create a hexgon grid using Uber H3 grid index."""
    def __init__(self):
        self.data_dir = '/app/src/data/output'
    def create_geojson_from_bbox(self, top, left, bottom, right):
        """
        Creates a geojson-like dictionary for H3 from a bounding box
        """
        return {'type': 'Polygon', 'coordinates': [[[bottom, left], [bottom, right], [top, right], [top, left], [bottom, left]]]}

    def create_grid(self, study_area, polygon, resolution):
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
        gdf['id'] = gdf['id'].astype(np.int64)
        gdf['study_area_id'] = int(study_area.lstrip('0'))
        # Save as Geopackage
        # gdf.to_file('%s/%s.gpkg' % (self.data_dir,layer_name), driver='GPKG')
        # Return gdf
        return gdf

    def create_grids_study_area_table(self,municipality):
        db = Database()
        con = db.connect()
        df = database_table2df(con, 'temporal.study_area', attribute='rs', value=f"'{municipality}'", geometry_column="geom")
        df = df.to_crs(31468)
        df_ds = df.dissolve()
        df_ds["geom"] = df_ds["geom"].buffer(1000)
        df_ds = df_ds.to_crs(4326)
        bounds = df_ds.geometry.bounds
        top = bounds['maxy'][0]
        left = bounds['minx'][0]
        bottom = bounds['miny'][0]
        right = bounds['maxx'][0]
        return [top,left,bottom,right]
        

# Create grid from bouding box example
#bbox = H3Grid().create_geojson_from_bbox(top=48.352598707539315, left=11.255493164062498, bottom=47.92738566360356, right=11.8927001953125)
#bbox = H3Grid().create_geojson_from_bbox(top=48.05924, left=7.68083, bottom=47.92881, right=7.96208)
#H3Grid().create_grid(polygon=bbox, resolution=9, layer_name='grid_visualization')
#H3Grid().create_grid(polygon=bbox, resolution=10, layer_name='grid_calculation')


# bbox = H3Grid().create_geojson_from_bbox(top=48.0710465709655708, left=7.6619009582867594, bottom=47.9035964575060191, right=7.9308570081842120)
# H3Grid().create_grid(polygon=bbox, resolution=9, layer_name='grid_visualization')
# H3Grid().create_grid(polygon=bbox, resolution=10, layer_name='grid_calculation')

# grid = H3Grid()
# bbox_coords = grid.create_grids_study_area_table('091620000')
# bbox = H3Grid().create_geojson_from_bbox(*bbox_coords)
# H3Grid().create_grid(polygon=bbox, resolution=9, layer_name='grid_visualization')
# H3Grid().create_grid(polygon=bbox, resolution=10, layer_name='grid_calculation')

