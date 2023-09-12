import duckdb

class DuckDB:
    """DuckDB Database class."""
    def __init__(self, db_config: dict):
        self.db_config = db_config
        try:
            self.conn = duckdb.connect(database=db_config['database'], read_only=False)
        except duckdb.OperationalError as e:
            raise e

    def select(self, query, params=None):
        """Run a SQL query to select rows from a table."""
        with self.conn.cursor() as cur:
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
            records = cur.fetchall()
        return records

    def perform(self, query, params=None):
        """Run a SQL query that does not return anything."""
        with self.conn.cursor() as cur:
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)

    def table_exists(self, table_name, schema=None):
        """Check if a table exists in the database."""
        if schema is None:
            schema = 'public'
        query = f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table_name}');"
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
        return result[0]

    def mogrify_query(self, query, params=None):
        """Return the query as a string for testing."""
        with self.conn.cursor() as cur:
            if params is None:
                result = cur.mogrify(query)
            else:
                result = cur.mogrify(query, params)
        return result

    def cursor(self):
        """Return a cursor object."""
        return self.conn.cursor()

    def close(self):
        self.conn.close()



class OverturePOICollection:
    """Collection of POIs from Overture Maps"""
    def __init__(self, db_config, region):
        self.db_config = db_config
        self.region = region
        self.db = DuckDB(db_config)  # Create a Database instance

    def initialize_duckdb(self):
        """Initialize DuckDB"""
        self.db.perform("""
            INSTALL spatial;
            INSTALL parquet;
            INSTALL httpfs;
            LOAD spatial;
            LOAD parquet;
            LOAD httpfs;
            SET s3_region='us-west-2';
            """)
    
    def poi_collection(self):
        self.db.perform(f"""
            LOAD httpfs;
            LOAD spatial;

            COPY (
                SELECT
                    id,
                    updatetime,
                    version,
                    CAST(names AS JSON) AS names,
                    CAST(categories AS JSON) AS categories,
                    confidence,
                    CAST(websites AS JSON) AS websites,
                    CAST(socials AS JSON) AS socials,
                    CAST(emails AS JSON) AS emails,
                    CAST(phones AS JSON) AS phones,
                    CAST(brand AS JSON) AS brand,
                    CAST(addresses AS JSON) AS addresses,
                    CAST(sources AS JSON) AS sources,
                    ST_GeomFromWKB(geometry)
                FROM
                    read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places/type=*/*', hive_partitioning=1)
                WHERE
                    bbox.minx > 5.8663153
                    AND bbox.maxx < 15.0419319
                    AND bbox.miny > 47.2701114	
                    AND bbox.maxy < 55.099161
            ) TO 'places_germany.geojsonseq'
            WITH (FORMAT GDAL, DRIVER 'GeoJSONSeq');
        """)

    # ogr2ogr

