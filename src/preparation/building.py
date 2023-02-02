def buildings_preparation(dataframe, config=None, filename=None, return_type=None):
    """introduces the landuse_simplified column and classifies it according to the config input"""
    if not config:
        config = Config("buildings")

    df = dataframe

    config_pop = Config("population")

    # Timer start
    print("Preparation started...")
    start_time = time.time()
    # Preprocessing: removing, renaming, reordering and data type adjustments of columns

    if "geometry" in df.columns:
        df = df.rename(columns={"geometry": "geom"})
    if "way" in df.columns:
        df = df.rename(columns={"way": "geom"})

    df = df.rename(
        columns={
            "addr:street": "street",
            "addr:housenumber": "housenumber",
            "building:levels": "building_levels",
            "roof:levels": "roof_levels",
        }
    )
    df["residential_status"] = None
    df["area"] = None

    # classify by geometry
    df.at[df["geom"].geom_type == "Point", "origin_geometry"] = "point"
    df.at[df["geom"].geom_type == "MultiPolygon", "origin_geometry"] = "polygon"
    df.at[df["geom"].geom_type == "Polygon", "origin_geometry"] = "polygon"
    df.at[df["geom"].geom_type == "LineString", "origin_geometry"] = "line"

    # remove lines and points from dataset
    df = df[df.origin_geometry != "line"]
    df = df.reset_index(drop=True)
    df = df[df.origin_geometry != "point"]
    df = df.reset_index(drop=True)

    df = df[
        [
            "osm_id",
            "building",
            "amenity",
            "leisure",
            "residential_status",
            "street",
            "housenumber",
            "area",
            "building_levels",
            "roof_levels",
            "origin_geometry",
            "geom",
        ]
    ]
    df["building_levels"] = pd.to_numeric(
        df["building_levels"], errors="coerce", downcast="float"
    )
    df["roof_levels"] = pd.to_numeric(
        df["roof_levels"], errors="coerce", downcast="float"
    )
    df = df.assign(source="osm")

    # classifying residential_status in 'with_residents', 'potential_residents', 'no_residents'
    df.loc[
        (
            (df.building.str.contains("yes"))
            & (df.amenity.isnull())
            & (df.amenity.isnull())
        ),
        "residential_status",
    ] = "potential_residents"
    df.loc[
        df.building.isin(config_pop.preparation["building_types_residential"]),
        "residential_status",
    ] = "with_residents"
    df.residential_status.fillna("no_residents", inplace=True)

    # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry="geom")
    df.crs = "EPSG:4326"
    df = df.reset_index(drop=True)

    # calculating the areas of the building outlines in m^2
    df = df.to_crs({"init": "epsg:3857"})
    df["area"] = df["geom"].area.round(2)
    df = df[df.area != 0]
    df = df.to_crs({"init": "epsg:4326"})

    # Timer finish
    print(f"Preparation took {time.time() - start_time} seconds ---")

    return gdf_conversion(df, filename, return_type)
