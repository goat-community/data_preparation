#%%

import geopandas as gp
import os
import pandas

# POIs preparation from geojson imported from OSM 
filename = "osm_pois.geojson"
file     = open(os.path.join("pois_fusion", filename), encoding="utf-8")
df       = gp.read_file(file)

df = df.drop(columns={"lat", "lon", "version", "timestamp", "changeset"})
df = df.rename(columns={"geometry": "geom", "id ":"osm_id", "addr:housenumber": "housenumber", "osm_type" : "origin_geometry"})
df = df.assign(elem_type = None)
        


# convert polygons to points
i_geom_idx   = df.columns.get_loc("geom")
i_orig_geom  = df.columns.get_loc("origin_geometry")
i_amenity    = df.columns.get_loc("amenity")
i_shop       = df.columns.get_loc("shop")
i_tourism    = df.columns.get_loc("tourism")
#i_elem_type = df.columns.get_loc("elem_type")

for i in df.index:
    if df.iloc[i,i_geom_idx].geom_type == "Polygon":
        df.iloc[i,i_geom_idx] = df.iloc[i,i_geom_idx].centroid
        df.iloc[i,i_orig_geom] = "point"
    if df.iloc[i,i_orig_geom] == "node":
        df.iloc[i,i_orig_geom] = "point"
    #df.iloc[i,amenity] = df.iloc[i,amenity].strip()
    # Not sure that it is necessary to include
    if df.iloc[i,i_tourism]:
    # if df.iloc[i,i_amenity] and not df.iloc[i,i_shop]:
    #     df.iloc[i,i_elem_type] = df.iloc[i,i_amenity]
    # elif not df.iloc[i,i_amenity] and df.iloc[i,i_shop]:
    #     df.iloc[i,i_elem_type] = df.iloc[i,i_shop]


       
     print(df)
    df.to_file(os.path.join("pois_fusion" ,"osm_pois_2nd_step.geojson"), driver="GeoJSON")


# for d in df["elem_type"]:
#     if d == "kindergarten":
#        df["amenity"] = "kindergarten"
#        df["kindergarten"] = None
#        print(d)







    print(df.columns)
# %%
