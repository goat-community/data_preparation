from sedona.spark import SedonaContext

from src.collection.overture_collection_base import OvertureBaseCollection
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import get_region_bbox_coords, print_error, print_info, timing


class OvertureNetworkCollection(OvertureBaseCollection):

    def __init__(self, db_local: Database, db_remote: Database, region: str, collection_type: str):
        super().__init__(db_local, db_remote, region, collection_type)


    def initialize_data_source(self, sedona: SedonaContext):
        """Initialize Overture geoparquet file source and data frames for transportation data."""

        # Load Overture geoparquet data into Spark DataFrames
        self.segments_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=segment"
        ) # Segments/edges
        self.connectors_df = sedona.read.format("geoparquet").load(
            path=f"{self.data_config_collection['source']}/type=connector"
        ) # Connectors/nodes

        print_info("Initialized data source.")


    def initialize_tables(self):
        """Create tables in PostgreSQL database for segments and connectors."""

        sql_create_table_segments = """
            DROP TABLE IF EXISTS temporal.segments;
            CREATE TABLE temporal.segments (
                id TEXT PRIMARY KEY,
                subtype TEXT,
                connectors TEXT[],
                road TEXT,
                geometry TEXT
            );
        """
        self.db_local.perform(sql_create_table_segments)
        print_info("Created table: temporal.segments.")

        sql_create_table_connectors = """
            DROP TABLE IF EXISTS temporal.connectors;
            CREATE TABLE temporal.connectors (
                id TEXT PRIMARY KEY,
                geometry TEXT
            );
        """
        self.db_local.perform(sql_create_table_connectors)
        print_info("Created table: temporal.connectors.")


    def filter_region_segments(self, bbox_coords: dict):
        """Initialize the segments dataframe and apply relevant filters."""

        seg = self.segments_df.selectExpr(
            "id",
            "subType",
            "connectors",
            "road",
            "bbox",
            "ST_AsText(geometry) AS geometry"
        )
        seg = seg.filter(
            (seg.subType == "road") &
            (seg.bbox.minx > bbox_coords["xmin"]) &
            (seg.bbox.miny > bbox_coords["ymin"]) &
            (seg.bbox.maxx < bbox_coords["xmax"]) &
            (seg.bbox.maxy < bbox_coords["ymax"])
        )
        seg = seg.drop(seg.bbox)
        return seg


    def filter_region_connectors(self, bbox_coords: dict):
        """Initialize the segments dataframe and apply relevant filters."""

        conn = self.connectors_df.selectExpr(
            "id",
            "bbox",
            "ST_AsText(geometry) AS geometry"
        )
        conn = conn.filter(
            (conn.bbox.minx > bbox_coords["xmin"]) &
            (conn.bbox.miny > bbox_coords["ymin"]) &
            (conn.bbox.maxx < bbox_coords["xmax"]) &
            (conn.bbox.maxy < bbox_coords["ymax"])
        )
        conn = conn.drop(conn.bbox)
        return conn


    @timing
    def alter_tables(self):
        """Alter tables to update column data types and create indexes."""

        print_info("Altering table: temporal.segments.")
        sql_alter_table_segments = """
            ALTER TABLE temporal.segments
            ALTER COLUMN geometry SET DATA TYPE GEOMETRY(LINESTRING, 4326);
            CREATE INDEX ON temporal.segments USING GIST (geometry);
        """
        self.db_local.perform(sql_alter_table_segments)

        print_info("Altering table: temporal.connectors.")
        sql_alter_table_connectors = """
            ALTER TABLE temporal.connectors
            ALTER COLUMN geometry SET DATA TYPE GEOMETRY(POINT, 4326);
            CREATE INDEX ON temporal.connectors USING GIST (geometry);
        """
        self.db_local.perform(sql_alter_table_connectors)


    def run(self):
        """Run Overture network collection."""

        sedona = self.initialize_sedona_context()
        self.initialize_jdbc_properties()
        self.initialize_data_source(sedona)
        self.initialize_tables()

        bbox_coords = get_region_bbox_coords(
            geom_query=self.data_config_collection["region"],
            db=self.db_remote
        )

        region_segments = self.filter_region_segments(bbox_coords)
        region_connectors = self.filter_region_connectors(bbox_coords)

        self.fetch_data(
            data_frame=region_segments,
            output_schema="temporal",
            output_table="segments"
        )
        self.fetch_data(
            data_frame=region_connectors,
            output_schema="temporal",
            output_table="connectors"
        )

        self.alter_tables()


def collect_overture_network(region: str):
    print_info(f"Collect Overture network data for region: {region}.")
    db_local = Database(settings.LOCAL_DATABASE_URI)
    db_remote = Database(settings.RAW_DATABASE_URI)

    try:
        OvertureNetworkCollection(
            db_local=db_local,
            db_remote=db_remote,
            region=region,
            collection_type="network_overture"
        ).run()
        db_local.close()
        db_remote.close()
        print_info("Finished Overture network collection.")
    except Exception as e:
        print_error(e)
        raise e
    finally:
        db_local.close()
        db_remote.close()
