def landuse_preparation(dataframe, config=None, filename=None, return_type=None):
    """introduces the landuse_simplified column and classifies it according to the config input"""

    df = dataframe

    if not config:
        config = Config("landuse")

    # Timer start
    print("Preparation started...")
    start_time = time.time()

    df = df.rename(columns={"id": "osm_id"})

    # Preprocessing: removing, renaming and reordering of columns
    # df = df.drop(columns={"timestamp", "version", "changeset"})
    if "geometry" in df.columns:
        df = df.rename(columns={"geometry": "geom"})
    if "way" in df.columns:
        df = df.rename(columns={"way": "geom"})

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

    df["landuse_simplified"] = None
    df = df[
        [
            "landuse_simplified",
            "landuse",
            "tourism",
            "amenity",
            "leisure",
            "natural",
            "name",
            "tags",
            "osm_id",
            "origin_geometry",
            "geom",
        ]
    ]

    df = df.assign(source="osm")

    # Fill landuse_simplified coulmn with values from the other columns
    custom_filter = config.collection["osm_tags"]

    if custom_filter is None:
        print(
            "landuse_simplified can only be generated if the custom_filter of collection\
               is passed"
        )
    else:
        for i in custom_filter.keys():
            df["landuse_simplified"] = df["landuse_simplified"].fillna(
                df[i].loc[df[i].isin(custom_filter[i])]
            )

        # import landuse_simplified dict from config
        landuse_simplified_dict = config.preparation["landuse_simplified"]

        # Rename landuse_simplified by grouping
        # e.g. ["basin","reservoir","salt_pond","waters"] -> "water"
        for i in landuse_simplified_dict.keys():
            df["landuse_simplified"] = df["landuse_simplified"].replace(
                landuse_simplified_dict[i], i
            )

    if df.loc[
        ~df["landuse_simplified"].isin(list(landuse_simplified_dict.keys()))
    ].empty:
        print("All entries were classified in landuse_simplified")
    else:
        print(
            "The following tags in the landuse_simplified column need to be added to the\
               landuse_simplified dict in config.yaml:"
        )
        print(
            df.loc[~df["landuse_simplified"].isin(list(landuse_simplified_dict.keys()))]
        )

    # remove lines from dataset
    df = df[df.origin_geometry != "line"]
    df = df.reset_index(drop=True)

    # Convert DataFrame back to GeoDataFrame (important for saving geojson)
    df = gp.GeoDataFrame(df, geometry="geom")
    df.crs = "EPSG:4326"
    df = df.reset_index(drop=True)

    # Timer finish
    print(f"Preparation took {time.time() - start_time} seconds ---")

    return gdf_conversion(df, filename, return_type)

