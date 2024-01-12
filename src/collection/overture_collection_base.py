from sedona.spark import SedonaContext

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database

class OvertureBaseCollection:
    def __init__(self, db_local: Database, db_remote: Database, region: str, collection_type: str):
        self.region = region
        self.db_local = db_local
        self.db_remote = db_remote
        self.data_config = Config(collection_type, region)
        self.data_config_collection = self.data_config.collection
        self.dataset_dir = self.data_config.dataset_dir

    def initialize_sedona_context(self):
        """Initialze Sedona context with required dependencies, AWS credentials provider and resource allocations."""

        config = SedonaContext.builder() \
            .config('spark.jars.packages',
                'org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.5.0,'
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
            .config("spark.logConf", "true") \
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


