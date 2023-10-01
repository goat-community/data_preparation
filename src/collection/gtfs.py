from src.db.db import Database
from src.config.config import Config
from src.utils.utils import print_info
from src.core.config import settings
import os
import subprocess
from src.utils.utils import replace_dir
from src.db.tables.gtfs import GtfsTables


class GTFSCollection:
    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region
        self.config = Config("gtfs", region)
        self.schema = self.config.preparation["target_schema"]
        self.chunk_size = 1000000
        # Create tables
        gtfs_tables = GtfsTables(self.schema)
        self.create_queries = gtfs_tables.sql_create_table()

    def create_table_schema(self):
        """Create the schema for the gtfs data."""

        print_info("Create schema for gtfs data.")
        # Check if schema exists
        schema_exists = self.db.select(
            f"SELECT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{self.schema}');"
        )[0][0]
        if not schema_exists:
            print_info(f"Create schema {self.schema}.")
        else:
            print_info(
                f"Schema {self.schema} already exists. It will be dropped and recreated."
            )
            self.db.perform(f"DROP SCHEMA {self.schema} CASCADE;")
            self.db.perform(f"CREATE SCHEMA {self.schema};")

        print_info("Create tables for gtfs data.")


        for table in self.create_queries:
            self.db.perform(self.create_queries[table])

        # Make stop, stop_times and shapes distributed
        distributed_tables = ["stop_times", "shapes", "stops"]
        print_info(f"Distribute tables using CITUS: {distributed_tables}")
        for table in distributed_tables:
            sql_make_table_distributed = f"SELECT create_distributed_table('{self.schema}.{table}', 'h3_3');"
            self.db.perform(sql_make_table_distributed)


    def split_file(self, table: str, output_dir: str):
        """Split file into chunks and removes header."""

        input_file = os.path.join(settings.INPUT_DATA_DIR, "gtfs", table + ".txt")
        print_info(
            f"Split file {input_file} into chunks of max. {self.chunk_size} rows."
        )

        # Read header to define column order and remove header afterwards from file
        with open(input_file, "r") as f:
            header = f.readline().strip().split(",")

        # Check if header is same as column of table
        columns = self.db.select(
            f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{self.schema}' AND table_name = '{table}';"
        )
        # Get all columns besides the loop_id and id column
        columns = [
            column[0]
            for column in columns
            if column[0] not in ["loop_id", "id", "h3_3"]
        ]

        # Compare columns of table with columns of gtfs file. Identify missing columns in gtfs file and in table.
        if set(columns) - set(header):
            print_info(f"Columns of table {table} are missing in the gtfs file.")

        # Columns are missing in table return them to drop them later.
        excess_columns = None

        if set(header) - set(columns):
            raise ValueError(
                f"Columns {excess_columns} are missing in table {table} that are in the gtfs file. Import stopped."
            )

        # Split file into chunks and use file name as prefix
        subprocess.run(
            [
                "split",
                "-l",
                str(self.chunk_size),
                input_file,
                os.path.join(output_dir, table + "_"),
            ]
        )

        # Get first file
        first_file = os.path.join(output_dir, table + "_aa")

        # Remove header from the first file
        with open(first_file, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        with open(first_file, "w", encoding="utf-8") as f:
            f.writelines(lines[1:])

        return header

    def import_file(self, input_dir: str, table: str, header: list):
        """Import file into database using copy."""

        files = os.listdir(input_dir)
        print_info(f"Importing {len(files)} files for table {table}.")
        cnt = 0
        # Loop over all files and import them one by one
        for file in files:
            # Create temp table
            sql_create_temp_table = f"""
                DROP TABLE IF EXISTS {self.schema}.{table}_temp;
                CREATE UNLOGGED TABLE {self.schema}.{table}_temp AS
                SELECT *
                FROM {self.schema}.{table}
                LIMIT 0;
            """
            self.db.perform(sql_create_temp_table)

            # Copy data to temp table
            file_path_postgres = os.path.join("/tmp/gtfs/temp", file)
            sql_copy = f"""
                COPY {self.schema}.{table}_temp ({",".join(header)}) FROM '{file_path_postgres}'
                CSV DELIMITER ',' QUOTE '"' ESCAPE '"';
            """
            self.db.perform(sql_copy)

            # Check if table not shapes or stops
            if table not in ["shapes", "stops", "stop_times"]:
                # Copy data directly from temp table to table
                sql_copy = f"""
                    INSERT INTO {self.schema}.{table} ({",".join(header)})
                    SELECT {",".join(header)}
                    FROM {self.schema}.{table}_temp;
                """
            elif table == "shapes":
                # Copy data and create geometry
                sql_copy = f"""
                    INSERT INTO {self.schema}.{table} ({",".join(header)}, geom, h3_3)
                    SELECT {",".join(header)}, ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326) AS geom,
                    public.to_short_h3_3(h3_lat_lng_to_cell(ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326)::point, 3)::bigint) AS h3_3
                    FROM {self.schema}.{table}_temp;
                """
            elif table == "stops":
                sql_copy = f"""
                    INSERT INTO {self.schema}.{table} ({",".join(header)}, geom, h3_3)
                    SELECT {",".join(header)}, ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326) AS geom,
                    public.to_short_h3_3(h3_lat_lng_to_cell(ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326)::point, 3)::bigint) AS h3_3
                    FROM {self.schema}.{table}_temp;
                """
            elif table == "stop_times":
                # Make temp table logged and create index on stop_id
                self.db.perform(f"ALTER TABLE {self.schema}.{table}_temp SET LOGGED;")
                self.db.perform(f"CREATE INDEX ON {self.schema}.{table}_temp (stop_id);")

                # Get h3_3 from stops table
                columns = ", t.".join(header)
                columns = "t." + columns
                sql_copy = f"""
                    INSERT INTO {self.schema}.{table} ({",".join(header)}, h3_3)
                    SELECT {columns}, s.h3_3
                    FROM {self.schema}.{table}_temp t
                    LEFT JOIN {self.schema}.stops s ON t.stop_id = s.stop_id;
                """
            self.db.perform(sql_copy)

            cnt += 1
            print_info(f"Imported {cnt} of {len(files)} files for table {table}.")

        # Make table logged
        self.db.perform(f"ALTER TABLE {self.schema}.{table} SET LOGGED;")

    def create_indices(self, table: str):
        """Make tables distributed using CITUS and create indices."""

        # Add Constraints
        print_info("Add constraints to tables.")

        if table == "stops":
            sql_command = f"""
                ALTER TABLE {self.schema}.stops ADD PRIMARY KEY (h3_3, stop_id);
                CREATE INDEX ON {self.schema}.stops USING GIST (h3_3, geom);
                CREATE INDEX ON {self.schema}.stops (h3_3, parent_station);
            """
        elif table == "stop_times":
            sql_command = f"""
                ALTER TABLE {self.schema}.stop_times ADD FOREIGN KEY (h3_3, stop_id)
                REFERENCES {self.schema}.stops(h3_3, stop_id);
                ALTER TABLE {self.schema}.stop_times ADD FOREIGN KEY (h3_3, trip_id)
                REFERENCES {self.schema}.trips(trip_id);
                CREATE INDEX ON {self.schema}.stop_times (h3_3, stop_id);
                CREATE INDEX ON {self.schema}.stop_times (h3_3, trip_id);
                """
        elif table == "trips":
            sql_command = f"""
                ALTER TABLE {self.schema}.trips ADD PRIMARY KEY (trip_id);
                ALTER TABLE {self.schema}.trips ADD FOREIGN KEY (route_id) REFERENCES {self.schema}.routes(route_id);
                CREATE INDEX ON {self.schema}.trips (shape_id);
            """
        elif table == "routes":
            sql_command = f"""
                ALTER TABLE {self.schema}.routes ADD PRIMARY KEY (route_id);
            """
        elif table == "shapes":
            sql_command = f"""
                CREATE EXTENSION IF NOT EXISTS btree_gist;
                CREATE INDEX ON {self.schema}.shapes (h3_3, shape_id);
                CREATE INDEX ON {self.schema}.shapes USING GIST(h3_3, geom);
            """
        elif table == "calendar":
            sql_command = f"""
                ALTER TABLE {self.schema}.calendar ADD PRIMARY KEY (service_id);
            """
        else:
            return

        # Add indices
        self.db.perform(sql_command)


    def run(self):
        """Run the gtfs preparation."""
        #self.create_table_schema()

        # Check if for all table there is a gtfs file

        for table in self.create_queries:
            if table not in ["shapes", "calendar"]:
                continue
            file_dir = os.path.join(settings.INPUT_DATA_DIR, "gtfs", table + ".txt")
            if not os.path.exists(file_dir):
                raise Exception(f"File {file_dir} not found.")

            # Create temp dir
            temp_dir = os.path.join(settings.INPUT_DATA_DIR, "gtfs", "temp")
            replace_dir(temp_dir)
            # Split file into chunks
            header = self.split_file(table, temp_dir)
            # Import file into database
            self.import_file(temp_dir, table, header)
            self.create_indices(table)




def collect_gtfs(region: str):
    print_info(f"Prepare GTFS data for the region {region}.")
    db = Database(settings.LOCAL_DATABASE_URI)

    try:
        GTFSCollection(db=db, region=region).run()
        db.close()
        print_info("Finished GTFS preparation.")
    except Exception as e:
        print(e)
        raise e
    finally:
        db.close()


# Run as main
if __name__ == "__main__":
    collect_gtfs("eu")