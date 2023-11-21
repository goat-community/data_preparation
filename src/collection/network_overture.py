from sedona.spark import SedonaContext

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import get_region_bbox_coords, print_error, print_info, timing


class OvertureNetworkCollection:

    def __init__(self, db_local: Database, db_remote: Database, region: str):
        self.db_local = db_local
        self.db_remote = db_remote
        self.region = region
        self.config = Config("network_overture", region)

        self.OVERTURE_RELEASE = "2023-10-19-alpha.0"


    def initialize_sedona_context(self):
        """Initialze Sedona context with required dependencies, AWS credentials provider and resource allocations."""

        config = SedonaContext.builder() \
                .config('spark.jars.packages',
                    'org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,'
                    'org.datasyslab:geotools-wrapper:1.4.0-28.2,'
                    'org.apache.hadoop:hadoop-aws:3.3.4,'
                    'com.amazonaws:aws-java-sdk-bundle:1.12.583,'
                    'org.postgresql:postgresql:42.6.0'
                ) \
                .config(
                    "fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
                ) \
                .config("spark.driver.host", "localhost") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
        return SedonaContext.create(config)


    def initialize_jdbc_properties(self):
        """Initialize PostgreSQL JDBC connection properties."""

        self.jdbc_url = f"jdbc:postgresql://{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        self.jdbc_conn_properties = {
            "user": settings.POSTGRES_USER,
            "password": settings.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
            "batchsize": "10000"
        }


    def initialize_data_source(self, sedona: SedonaContext):
        """Initialize Overture parquet file source and data frames for transportation data."""

        # Load Overture parquet data into Spark DataFrames
        self.segments_df = sedona.read.format("parquet").load(
            path=f"s3a://overturemaps-us-west-2/release/{self.OVERTURE_RELEASE}/theme=transportation/type=segment"
        ) # Segments/edges
        self.connectors_df = sedona.read.format("parquet").load(
            path=f"s3a://overturemaps-us-west-2/release/{self.OVERTURE_RELEASE}/theme=transportation/type=connector"
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
            "ST_AsText(ST_GeomFromWKB(geometry)) AS geometry"
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
            "ST_AsText(ST_GeomFromWKB(geometry)) AS geometry"
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
    def fetch_data(self, data_frame, output_schema: str, output_table: str):
        """Fetch data from Overture S3 bucket and write to local PostgreSQL database."""

        print_info(f"Downloading Overture network data to: {output_schema}.{output_table}.")
        data_frame.write.jdbc(
            url=self.jdbc_url,
            table=f"{output_schema}.{output_table}",
            mode="append",
            properties=self.jdbc_conn_properties
        )


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
            geom_query=self.config.collection["geom_query"],
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
            region=region
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


# Run as main
if __name__ == "__main__":
    collect_overture_network("de")
