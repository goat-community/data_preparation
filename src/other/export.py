import boto3 
from botocore.exceptions import ClientError
import os      
import subprocess
import logging
from src.db.config import S3
from utils import print_info, print_warning, create_pgpass_for_db, ProgressPercentage
from src.db.config import DATABASE, DATABASE_RD, REMOTE_SERVER
from datetime import datetime

class Export: 
    """Class to dump and export tables from the database
    """    
    def __init__(self, db_config, db_config_rd) -> None:
        """Constructor

        Args:
            db_config (dict): Settings for local database
            db_config_rd (dict): Settings for remote database
        """        
        self.username, self.password, self.host, self.port, self.dbname = db_config.values()
        self.username_rd, self.password_rd, self.host_rd, self.port_rd, self.dbname_rd = db_config_rd.values()
        self.remote_server_user = REMOTE_SERVER["user"]
        self.remote_server_host = REMOTE_SERVER["host"] 
        
        self.root_dir = "/app"
        self.data_dir_output = self.root_dir + "/src/data/output/"
        self.dump_formats = {
            "custom": ".dump",
            "sql": ".sql"
        }
        self.server_data_dir = "/tmp/"
        self.default_dump_format = "custom"
        self.dir_pem_file = self.root_dir + "/src/config/remote_server.pem"
        create_pgpass_for_db(db_config=db_config)
        
    def dump_table(self, schema: str, table_name: str, format: str = "custom", data_only: bool = False):
        """Dump a table from the database

        Args:
            schema (str): Schema name of the table
            table_name (str): Table name
            format (str, optional): Desired format ("sql" or "custom"). Defaults to "custom".
            data_only (bool, optional): Get only data without table schema. Defaults to False.
        """        
        if format not in self.dump_formats.keys():
            format = self.default_dump_format
            print_warning(f"Format {format} not supported. Using default format '{self.default_dump_format}'")

        file_dir = self.data_dir_output + table_name + self.dump_formats[format]
           
        if data_only == True:
            data_only_tag = "--data-only"
        else:
            data_only_tag = ""
     
        try:
            if format == 'custom':
                cmd = f"""PGPASSFILE=~/.pgpass_{self.dbname} pg_dump -h {self.host} -p {self.port} -U {self.username} -d {self.dbname} --no-owner -x -Fc -t {schema}.{table_name} {data_only_tag} -f {file_dir}"""
            elif format == 'sql':
                cmd = f"""PGPASSFILE=~/.pgpass_{self.dbname} pg_dump -h {self.host} -p {self.port} -U {self.username} -d {self.dbname} --no-owner -x -t {schema}.{table_name} {data_only_tag} -f {file_dir}"""
            
            subprocess.run(cmd, shell=True)
            print_info(f"Dump ended for {table_name}")
        except Exception as e:
            print_warning(f"Dump failed for {table_name}")
            print(e)
    
    def restore_table_remote(self, file_name: str):
        """Restores previously dumped table in a remote server

        Args:
            file_name (str): Name of the file to restore
        """        
        #TODO: Check if table exists in database 
        try: 
            cmd = f"""ssh -i {self.dir_pem_file} {self.remote_server_user}@{self.remote_server_host} PGPASSWORD="{self.password_rd}" pg_restore --no-owner -x -j 8 -h {self.host_rd} -p {self.port_rd} -U {self.username_rd} -d {self.dbname_rd} {self.server_data_dir}{file_name}"""
            subprocess.run(cmd, shell=True)
        except Exception as e:
            print_warning(f"Restore failed for {file_name}")
            print(e)
            
    def connect_to_s3(self):
        """Connect to S3

        Returns:
            S3: S3 client
        """        
        session = boto3.Session(
            aws_access_key_id=S3['access_key_id'],
            aws_secret_access_key=S3['secret_access_key'],
        )
        s3 = session.client('s3')
        return s3

    def create_empty_folder_s3(self, bucket: str, dir_name: str):
        """Create an empty folder in S3

        Args:
            bucket (str): Bucket name
            dir_name (str): Folder name
        """        
        s3_client = self.connect_to_s3()
        s3_client.put_object(Bucket=bucket, Key=(dir_name + '/'))
        
    # Based on: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    def upload_file_s3(self, dir_name: str, bucket: str, object_name: str = None):
        """Upload a file to an S3 bucket
        Args:
            dir_name (str): Define the directory of the file to upload
            bucket (str): Bucket name
            object_name (str, optional): Desired object name in S3 bucker. Defaults to the filename of None.

        Returns:
            msg: Message of the upload
        """    
        # If S3 object_name was not specified, use dir_name
        if object_name is None:
            object_name = os.path.basename(dir_name)

        # Upload the file
        s3_client = self.connect_to_s3()
        try:
            response = s3_client.upload_file(dir_name, bucket, object_name, Callback=ProgressPercentage(dir_name))
            print_info(f"Upload ended for {dir_name}")
        except ClientError as e:
            logging.error(e)
            return {"msg": "Error"}
        return {"msg": "Success"}
    
    def download_file_s3_remote(self, file_dir: str, bucket: str):
        """Download a file from S3 to a remote server

        Args:
            file_dir (str): Local directory of the file to download
            bucket (str): S3 Bucket name
        """        
        
        #TODO: Check file size and available storage at server
        file_name = os.path.basename(file_dir)

        cmd = f"ssh -i {self.dir_pem_file} {self.remote_server_user}@{self.remote_server_host} aws s3 cp s3://{bucket}/{file_dir} {self.server_data_dir}"
        print_info(f"Downloading {file_name} from S3 to remote server")
        subprocess.run(cmd, shell=True)

        
    def export_and_restore(self, schema: str, table: str):
        """Export and restore a table from a database to another

        Args:
            schema (str): Schema name of the table
            table (str): Table name
        """        
        
        file_name = table + ".dump"
        s3_dir_name = 'data_preparation/' + datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%MS")
        s3_file_dir = s3_dir_name + '/' + file_name

        self.dump_table(schema=schema, table_name=table, format="custom", data_only=False)
        self.create_empty_folder_s3(bucket=S3["bucket"], dir_name=s3_dir_name)
        self.upload_file_s3(dir_name=self.data_dir_output + file_name, bucket=S3["bucket"], object_name=s3_file_dir)
        self.download_file_s3_remote(file_dir=s3_file_dir, bucket=S3["bucket"])
        self.restore_table_remote(file_name=file_name)
        
# export = Export(db_config=DATABASE, db_config_rd=DATABASE_RD)
# export.export_and_restore(schema="public", table="planet_osm_point")
# export.export_and_restore(schema="public", table="planet_osm_line")
# export.export_and_restore(schema="public", table="planet_osm_polygon")
# export.export_and_restore(schema="public", table="planet_osm_roads")