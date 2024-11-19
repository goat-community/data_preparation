import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from zipfile import ZipFile

from src.config.config import Config
from src.core.config import settings
from src.core.enums import DumpType
from src.db.db import Database
from src.utils.utils import (
    check_table_exists,
    create_table_dump,
    create_table_schema,
    delete_file,
    print_hashtags,
    print_info,
    print_separator_message,
    replace_dir,
    restore_table_dump,
)


class CityGMLCollection:
    def __init__(self, db, region):
        self.db = db
        # Path to data variables
        self.path_data_variables = os.path.join(
            settings.CONFIG_DIR, "data_variables", "building"
        )

        # Parse building function xml and create dictionary
        tree = ET.parse(
            os.path.join(
                self.path_data_variables,
                "building_function.xml",
            )
        )
        root = tree.getroot()
        dictentry = root.findall("{http://www.opengis.net/gml}dictionaryEntry")
        dict_building_types = {}
        for definitions in dictentry:
            for descAndNames in definitions:
                list_name = []
                for k in descAndNames.findall("{http://www.opengis.net/gml}name"):
                    list_name.append(k.text)
                dict_building_types[list_name[0]] = list_name[1]

        self.dict_building_types = dict_building_types
        # Set other variables
        self.s3_client = settings.S3_CLIENT
        config = Config("building", region)
        config.download_db_schema()
        self.path_s3_citygml = config.collection["city_gml"]["path_s3"]
        self.relevant_s3_citygml_files = config.collection["city_gml"][
            "relevant_s3_files"
        ]
        self.path_local_citygml = os.path.join(
            settings.INPUT_DATA_DIR, "building", "citygml"
        )
        self.settings_xml = os.path.join(
            self.path_data_variables,
            "settings_import_citygml.xml",
        )
        self.batch_size = 10
        self.remote_target_schema = config.collection["city_gml"][
            "remote_target_schema"
        ]
        self.remote_target_table = config.collection["city_gml"]["remote_target_table"]
        self.geographical_extent = config.collection["city_gml"]["geographical_extent"]

    def download_citygml_files(self, dir_zipped_files):

        for dir_zipped_file in dir_zipped_files:
            print_info(f"Downloading {dir_zipped_file} from S3 bucket.")
            self.s3_client.download_file(
                settings.AWS_BUCKET_CITYGML,
                dir_zipped_file,
                os.path.join(self.path_local_citygml, dir_zipped_file.split("/")[-1]),
            )

    def building_citygml_collection(self, download: bool = True):
        # List files in S3 bucket
        print_separator_message("Searching for files in S3 bucket.")
        res = self.s3_client.list_objects_v2(
            Bucket=settings.AWS_BUCKET_CITYGML, Prefix=self.path_s3_citygml
        )
        dirs_zipped_files = []
        # Get all files in S3 bucket with prefix and ending .zip
        for obj in res["Contents"]:
            if (
                obj["Key"].endswith(".zip")
                and obj["Key"].split("/")[-1] in self.relevant_s3_citygml_files
            ):
                dirs_zipped_files.append(obj["Key"])
                # converted to GB
                file_size = round(obj["Size"] / 1073741824, 3)  # covert to GB
                print_info(
                    f"""Found {obj['Key']} with with a size of {file_size} GB in S3 bucket to process."""
                )

        # Get citygml data
        if download is True:
            # Replace directory if exists
            replace_dir(self.path_local_citygml)
            # Download files from S3 bucket
            self.download_citygml_files(dirs_zipped_files)

        # Process file by file
        for dir_zipped_file in dirs_zipped_files:
            print_separator_message(f"Processing {dir_zipped_file}.")

            # Get files inside zip file
            zip_file = ZipFile(
                os.path.join(self.path_local_citygml, dir_zipped_file.split("/")[-1]),
                "r",
            )
            files_in_zip = zip_file.namelist()
            len_files_in_zip = len(files_in_zip)
            print_info(f"Found {len_files_in_zip} files in zip file.")

            # # Create temporary table to save preliminary results
            target_table = "building_" + dir_zipped_file.split("/")[-1].split(".")[0]
            self.create_temp_building_table(target_table)

            # Truncate CityGML tables to start clean
            self.truncate_citygml_tables()

            for i in range(0, len_files_in_zip, self.batch_size):
                print_separator_message("Importing of batch")
                print_info(
                    f"Processing files {i} to {i+self.batch_size} out of {len_files_in_zip}"
                )
                files_in_batch = files_in_zip[i : i + self.batch_size]

                path_batch = os.path.join(self.path_local_citygml, "batch")
                if os.path.exists(path_batch):
                    shutil.rmtree(path_batch)
                os.makedirs(path_batch)
                # Loop through files and extract the relevant ones
                for single_file in files_in_batch:

                    zip_file.extract(single_file, path_batch)
                    # single_file_path = os.path.join(
                    #     self.path_local_citygml, path_batch
                    # )

                # Import file using 3DCityDB CLI
                subprocess.run(
                    f"/opt/3dcitydb/bin/impexp import -c {self.settings_xml} {path_batch}",
                    shell=True,
                    check=True,
                )
                # # Delete file after import
                # delete_file(single_file_path)

                # Convert building into 2D schema
                self.convert_buildings(target_table)

                # Truncate CityGML tables to save space
                self.truncate_citygml_tables()

    # TODO: Exchange DB credendials automatically in settings.xml file before import

    def create_temp_building_table(self, table_name):
        create_table_schema(self.db, "basic.building")

        # Rename table
        self.db.perform(
            f"""
            DROP TABLE IF EXISTS basic.{table_name};
            ALTER TABLE basic.building RENAME TO {table_name};
            ALTER TABLE basic.{table_name} DROP COLUMN id;
            ALTER TABLE basic.{table_name} ADD COLUMN id integer;
            """
        )
        print_separator_message("Created temporary table to save preliminary results.")
        print_info(
            f"""Created empty table {table_name} in schema basic to save preliminary results."""
        )

    def truncate_citygml_tables(self):

        # This will select all tables besides the system tables
        tables_to_truncate = self.db.select(
            """
            SELECT relname
            FROM pg_stat_user_tables 
            WHERE n_live_tup > 0 
            AND schemaname = 'citydb'
            AND relname NOT IN ('aggregation_info', 'index_table', 'objectclass', 'database_srs'); 
        """
        )
        tables_to_truncate = [table[0] for table in tables_to_truncate]

        print_separator_message("Truncating CityGML tables.")
        for table in tables_to_truncate:
            self.db.perform(f"TRUNCATE citydb.{table} CASCADE;")
            print_info(f"Truncated table citydb.{table}.")

    def convert_buildings(self, table_name):
        """_summary_

        Args:
            table_name (_type_): _description_
        """

        # Get approximate meter in the reference system
        meter_ref_system = self.db.select(
            """
            SELECT ST_Distance(
                ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), srid),
                ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat + 0.00001), 4326), srid)
            )
            FROM (
                SELECT 
                    ST_SRID(ST_Force2D(ST_MakeValid(geometry))) AS srid, 
                    ST_X(ST_Centroid(ST_Force2D(ST_MakeValid(ST_Transform(geometry, 4326))))) AS lon, 
                    ST_Y(ST_Centroid(ST_Force2D(ST_MakeValid(ST_Transform(geometry, 4326))))) AS lat
                FROM surface_geometry 
                WHERE geometry IS NOT NULL 
                LIMIT 1
            ) x;
            """
        )
        meter_ref_system = meter_ref_system[0][0]
        self.db.perform(
            f"""
            INSERT INTO basic.{table_name} (id, roof_levels, building_levels, building_type, height, geom)
            WITH relevant_buildings AS 
            (
                SELECT DISTINCT b.building_root_id, s.cityobject_id, s.geometry AS geom 
                FROM surface_geometry s, building b
                WHERE geometry IS NOT NULL 
                AND ST_GeometryType (ST_FORCE2D(ST_MakeValid(s.geometry))) = 'ST_Polygon'
                AND cityobject_id = b.id 
            ), 
            unioned_buildings AS 
            (
                SELECT m.building_root_id, (ST_DUMP(ST_BOUNDARY(ST_UNION(ST_FORCE2D(ST_makeValid(m.geom)))))).geom AS geom 
                FROM relevant_buildings m, thematic_surface t
                WHERE m.cityobject_id = t.building_id 
                GROUP BY m.building_root_id
            ),
            unioned_building_filled_small_holes AS 
            (
                SELECT building_root_id, ST_UNION(ST_MakePolygon(geom)) AS geom 
                FROM unioned_buildings
                WHERE ST_AREA(ST_MakePolygon(geom)) > {meter_ref_system}
                GROUP BY building_root_id
            )
            SELECT u.building_root_id, CASE WHEN b.roof_type = '1000' THEN 0 ELSE 1 END as roof_level, 
            CASE WHEN b.measured_height > 3.5 THEN FLOOR(b.measured_height / 3.5) ELSE 1 END AS building_levels, 
            '{str(self.dict_building_types).replace("'", '"')}'::jsonb ->> b.function,
            b.measured_height, ST_TRANSFORM((ST_DUMP(u.geom)).geom, 4326) AS geom 
            FROM unioned_building_filled_small_holes u, building b
            WHERE u.building_root_id = b.id;    
            """
        )
        print_hashtags()
        print_info(
            f"Converted buildings to 2D schema and inserted into {table_name} table."
        )
        print_hashtags()

    def export_to_remote_db(self, db_rd, on_exists_drop: bool = True):

        print_separator_message("Exporting data to remote database.")

        # Get all table candidates
        tables_to_export = [
            "building_" + x.split(".")[0] for x in self.relevant_s3_citygml_files
        ]

        # Drop table if the user does wants to drop the table
        if on_exists_drop == True:
            db_rd.perform(
                f"DROP TABLE IF EXISTS {self.remote_target_schema}.{self.remote_target_table};"
            )
            print_info(
                f"""Dropped table {self.remote_target_schema}.{self.remote_target_table} from remote database. A new table with the same name will be created."""
            )
        else:
            print_info(
                f"Data will be appended to {self.remote_target_schema}.{self.remote_target_table} table."
            )

        for table in tables_to_export:

            # Check if table exists in the local database
            if (
                check_table_exists(db=self.db, schema="basic", table_name=table) is False
            ):
                continue

            print_info(f"Exporting table {table} to remote database.")
            # Rename table to meet target schema and table name
            self.db.perform(
                f"DROP TABLE IF EXISTS {self.remote_target_schema}.{self.remote_target_table};"
            )
            self.db.perform(
                f"""
                ALTER TABLE basic.{table} SET SCHEMA {self.remote_target_schema};
                ALTER TABLE {self.remote_target_schema}.{table} RENAME TO {self.remote_target_table};
            """
            )
            # Dump table by table
            create_table_dump(
                db_config=self.db.db_config,
                schema=self.remote_target_schema,
                table_name=self.remote_target_table,
            )
            # Rename table back to original name
            self.db.perform(
                f"""
                ALTER TABLE {self.remote_target_schema}.{self.remote_target_table} RENAME TO {table};
                ALTER TABLE {self.remote_target_schema}.{table} SET SCHEMA basic;
                """
            )
            # If the table exists then restore only the data and not the schema
            data_only = check_table_exists(
                db=db_rd,
                schema=self.remote_target_schema,
                table_name=self.remote_target_table,
            )
            # Restore table dump
            restore_table_dump(
                db_config=db_rd.db_config,
                schema=self.remote_target_schema,
                table_name=self.remote_target_table,
                dump_type=DumpType.data if data_only else DumpType.all,
            )

    def replace_building(self, db_rd):
        
        print_separator_message("Replacing buildings in remote database.")
        units = db_rd.select(self.geographical_extent)

        for unit in units:

            check_if_building_exists = db_rd.select(
                f"""
                SELECT id
                FROM basic.building
                WHERE ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), ST_CENTROID(geom))
                AND ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), geom)
                LIMIT 1;
                """
            )

            if check_if_building_exists != []:

                # Delete buildings in the unit
                db_rd.perform(
                    f"""
                    DELETE FROM basic.building 
                    WHERE ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), ST_CENTROID(geom))
                    AND ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), geom);
                    """
                )
                print_info(f"Deleted buildings in {unit[1]}.")

            else:
                print_info(f"No buildings to delete in {unit[1]}.")

        # Temporary drop indices
        db_rd.perform(
            """
            DROP INDEX IF EXISTS basic.building_geom_idx;
            ALTER TABLE basic.building DROP CONSTRAINT IF EXISTS building_pkey;
            """
        )

        for unit in units:

            check_if_building_exists = db_rd.select(
                f"""
                SELECT id
                FROM {self.remote_target_schema}.{self.remote_target_table}
                WHERE ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), ST_CENTROID(geom))
                AND ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), geom)
                LIMIT 1;
                """
            )

            if check_if_building_exists != []:

                # Add new buildings
                db_rd.perform(
                    f"""
                    INSERT INTO basic.building (amenity, housenumber, street, roof_levels, building_levels, building_type, height, geom)
                    SELECT amenity, housenumber, street, roof_levels, building_levels, building_type, height, geom
                    FROM {self.remote_target_schema}.{self.remote_target_table}
                    WHERE ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), ST_CENTROID(geom))
                    AND ST_Intersects(ST_GEOMFROMTEXT('{unit[2]}', 4326), geom);
                    """
                )
                print_info(f"Inserted buildings in {unit[1]}.")
            else:
                print_info(f"No buildings to insert in {unit[1]}.")

        print_info("Recreating indices...")
        db_rd.perform(
            """
            CREATE INDEX ON basic.building USING GIST (geom);
            ALTER TABLE basic.building ADD PRIMARY KEY (id);
            """
        )


db = Database(settings.CITYGML_DATABASE_URI)
city_gml_collection = CityGMLCollection(db, region="de")
# city_gml_collection.building_citygml_collection(download=True)
db_rd = Database(settings.RAW_DATABASE_URI)
# city_gml_collection.export_to_remote_db(db_rd=db_rd, on_exists_drop=False)
# city_gml_collection.replace_building(db_rd=db_rd)
