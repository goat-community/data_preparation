import subprocess
import os
import sys
import psutil
import geopandas as gpd
import re
from src.utils.utils import (
    print_info,
    download_link,
    delete_dir,
    print_hashtags,
    delete_file,
    print_warning,
)
from src.utils.utils import create_pgpass, parse_poly
from functools import partial
from multiprocessing.pool import Pool
from src.config.config import Config
from src.core.config import settings
from src.db.db import Database

class OSMBaseCollection:
    def __init__(self, db_config: str, dataset_type: str, region: str = "de"):  
        """Constructor for OSM Base Class.

        Args:
            db_config (str): Configuration for database.
            dataset_type (str): Type of dataset. Currently supported: "poi", "network", "building".
        """        
        
        self.db_config = db_config
        self.dataset_type = dataset_type
        self.dbname = self.db_config.path.replace("/", "")
        self.host = self.db_config.host
        self.username = self.db_config.user
        self.port = self.db_config.port
        self.password = self.db_config.password
        
        self.data_config = Config(self.dataset_type, region)
        self.region_links = self.data_config.pbf_data
        
        self.dataset_dir = self.data_config.dataset_dir
        self.s3_osm_basedir = os.path.join("osm-raw", self.dataset_type)
        self.available_cpus = os.cpu_count()
        self.memory = psutil.virtual_memory().total
        self.cache = round(self.memory / 1073741824 * 1000 * 0.75)
        create_pgpass(db_config=self.db_config)

    def prepare_osm_data(self, link: str, osm_filter: str):
        """Prepare OSM data for import into PostGIS database.

        Args:
            link (str): _Download link to OSM data.
            osm_filter (str): Filter for OSM data.
        """        

        # Change directory
        os.chdir(self.dataset_dir)
        
        # Process OSM data
        full_name = link.split("/")[-1]
        only_name = full_name.split(".")[0]
        print_info(f"Preparing file {full_name}")
        subprocess.run(
            f"osmconvert {full_name} --drop-author --drop-version --out-osm -o={only_name}.o5m",
            shell=True,
            check=True,
        )
        subprocess.run(
            f'osmfilter {only_name}.o5m -o={only_name + "_" + self.dataset_type}.o5m --keep="{osm_filter}"',
            shell=True,
            check=True,
        )
        subprocess.run(
            f'osmconvert {only_name + "_" + self.dataset_type}.o5m -o={only_name + "_" + self.dataset_type}.osm',
            shell=True,
            check=True,
        )
        
        # Delete temporary files
        delete_file(f"{only_name}.o5m")
        delete_file(f"{only_name + '_' + self.dataset_type}.o5m")

    def get_timestamp_osm_file(self, path: str) -> str:
        """Get timestamp of OSM file.

        Args:
            path (str): Download link to OSM data.

        Returns:
            str: Timestamp of OSM file.
        """
        
        # Get timestamp of OSM file using osmium
        cmd = ['osmium', 'fileinfo', path]
        result = subprocess.run(cmd, capture_output=True, text=True)

        # Extract the timestamp from the command output using regular expressions
        match = re.search(r'timestamp=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', result.stdout)
        timestamp = match.group(1)
        
        return timestamp

    def export_osm_boundaries_db(self, db: Database, use_poly=True):
        """Export OSM boundaries to PostGIS database.

        Args:
            db (Database): Database object.
            use_poly (bool, optional): Shall use poly file. Defaults to True.
        """    

        if use_poly is True:
            file_ending = ".poly"
        else:
            file_ending = ".geojson"

        # Create list of download links to OSM Poly Boundaries
        region_poly_links = []
        for link in self.region_links:
            region_poly_links.append(
                os.path.dirname(link)
                + "/"
                + os.path.basename(link).split("-latest")[0]
                + file_ending
            )

        # Download OSM Poly Boundaries
        download = partial(download_link, self.dataset_dir)
        pool = Pool(processes=self.available_cpus)
        print_hashtags()
        print_info(f"Downloading OSM Poly Boundaries started.")
        print_hashtags()
        pool.map(download, region_poly_links)
        pool.close()
        pool.join()

        # Create table for OSM boundaries
        db.perform(f"DROP TABLE IF EXISTS {self.dataset_type}_osm_boundary;")
        sql_create_table = f"""
            CREATE TABLE {self.dataset_type}_osm_boundary
            (
                id serial,
                name text,
                date timestamp,
                geom geometry(MULTIPOLYGON, 4326)
            );
            ALTER TABLE {self.dataset_type}_osm_boundary ADD PRIMARY KEY(id);
            CREATE INDEX ON {self.dataset_type}_osm_boundary USING GIST(geom); 
        """
        db.perform(sql_create_table)

        # Insert OSM boundaries into database
        for idx, link in enumerate(region_poly_links):
            # Get path to OSM file
            path_osm_file = os.path.join(self.dataset_dir, os.path.basename(self.region_links[idx]))
            # Get timestamp of OSM file
            time_stamp_osm_file = self.get_timestamp_osm_file(path_osm_file)
            
            # Get geometry of OSM boundary
            file_dir = os.path.join(self.dataset_dir, os.path.basename(link))
            if use_poly is True:
                geom = parse_poly(file_dir)
            else:
                gdf = gpd.read_file(file_dir)
                geom = gdf.iloc[0]["geometry"]

            # Insert OSM boundary, name and timestamp into database table
            sql_insert = f"""
                INSERT INTO {self.dataset_type}_osm_boundary(name, date, geom)
                SELECT %s, %s, ST_MULTI(ST_GEOMFROMTEXT(%s));
            """
            db.perform(
                query=sql_insert,
                params=[os.path.basename(link).split("-latest")[0], time_stamp_osm_file, geom.wkt],
            )
        print_info(f"OSM boundaries inserted into database.")
        
        # Export OSM boundaries to GeoJSON
        gdf = gpd.read_postgis(f"SELECT * FROM {self.dataset_type}_osm_boundary", db.return_sqlalchemy_engine(), geom_col="geom")
        gdf.to_file(os.path.join(self.dataset_dir, f"{self.dataset_type}_osm_boundary.geojson"), driver="GeoJSON")
        print_info(f"OSM boundaries exported to GeoJSON.")
        
    def import_dem(self, filepath=None):
        """Import DEM data into PostGIS database.

        Args:
            filepath (str, optional): Filepath to file can specified. Defaults to None and therefore will use default data directory.
        """
        if not filepath:
            filepath = os.path.join(self.dataset_dir, "dem.tif")

        if not os.path.exists(filepath):
            print_warning(f"{filepath} for dem.tif does not exist. Processing will be continued but without DEM data.")
            return

        filepath_no_ext = os.path.splitext(filepath)[0]
        filepath_converted_dem = filepath_no_ext + "_conv.tif"
        filepath_sql_dem = filepath_no_ext + ".sql"

        delete_file(filepath_converted_dem)
        delete_file(filepath_sql_dem)

        # Prepare and import digital elevation model
        subprocess.run(
            f"gdalwarp -t_srs EPSG:4326 -dstnodata -999.0 -r near -ot Float32 -of GTiff {filepath} {filepath_converted_dem}",
            shell=True,
            check=True,
        )
        subprocess.run(
            f"raster2pgsql -c -C -s 4326 -f rast -F -I -M -t 100x100 {filepath_converted_dem} public.dem > {filepath_sql_dem}",
            shell=True,
            check=True,
        )
        subprocess.run(
            f'PGPASSFILE=~/.pgpass_{self.dbname} psql -d {self.dbname} -U {self.username} -h {self.host} -p {self.port} --command="DROP TABLE IF EXISTS dem;" -q',
            shell=True,
            check=True,
        )
        subprocess.run(
            f"PGPASSFILE=~/.pgpass_{self.dbname} psql -d {self.dbname} -U {self.username} -h {self.host} -p {self.port} -f {filepath_sql_dem} -q",
            shell=True,
            check=True,
        )

    def download_bulk_osm(self):
        """Download bulk OSM data.
        """        
        
        # Cleanup
        delete_dir(self.dataset_dir)            
        os.mkdir(self.dataset_dir)
        os.chdir(self.dataset_dir)

        # Download all needed files
        # download = partial(download_link, "")
        # pool = Pool(processes=self.available_cpus)

        print_hashtags()
        print_info(f"Downloading OSM files started.")
        print_hashtags()
        
        for link in self.region_links:
            download_link("", link)
        
        # pool.map(download, self.region_links)
        # pool.close()
        # pool.join()

        # Check if all files are downloaded and are not empty
        data_source_date = []
        for link in self.region_links:
            dir = os.path.join(self.dataset_dir, os.path.basename(link))
            data_source_date.append(self.get_timestamp_osm_file(dir))
            if not os.path.exists(dir) or os.stat(dir).st_size == 0:
                print_warning(f"File {os.path.basename(link)} could not be downloaded. Processing stopped.")
                sys.exit()
        
        # Check if all files are from the same date
        if len(set(data_source_date)) > 1:
            print_warning(f"OSM files are not from the same date. Processing stopped.")
            sys.exit()
        
    def prepare_bulk_osm(
        self, osm_filter: str
    ):
        """Prepare all osm files using filter.

        Args:
            osm_filter (str): OSM filter to use.
        """        
        pool = Pool(processes=self.available_cpus)

        # Prepare and filter osm files
        print_hashtags()
        print_info(f"Preparing OSM files started.")
        print_hashtags()
        pool.map(
            partial(
                self.prepare_osm_data,
                osm_filter=osm_filter,
            ),
            self.region_links,
        )
        pool.close()
        pool.join()

    def merge_osm_and_import(self):
        """Merge all osm files and import them into PostGIS database.
        """        
        # Change to data directory
        os.chdir(self.dataset_dir)
        
        # Merge all osm files
        print_info("Merging files")
        file_names = [
            f.split("/")[-1].split(".")[0] + f"_{self.data_config.name}.osm" for f in self.region_links
        ]

        subprocess.run(
            f'osmium merge {" ".join(file_names)} -o merged.osm',
            shell=True,
            check=True,
        )
        # Import merged osm file using customer osm2pgsql style
        self.data_config.osm2pgsql_create_style()
        path_style_file = os.path.join(self.dataset_dir, "osm2pgsql.style")
        subprocess.run(
            f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgsql -d {self.dbname} -H {self.host} -U {self.username} --port {self.port} --hstore -E 4326 -r .osm -c "
            + "merged.osm"
            + f" -s --drop -C {self.cache} --style {path_style_file} --prefix osm_{self.data_config.name}",
            shell=True,
            check=True,
        )
     
    def upload_raw_osm_data(self, boto_client):
        """Uploads raw osm data to s3 bucket.

        Args:
            boto_client (_type_): Boto3 client
        """        
        print_hashtags()
        print_info(f"Uploading raw OSM data to s3 bucket {settings.AWS_BUCKET_NAME} started.")
        print_hashtags()
        
        for file in os.listdir(self.dataset_dir):
            
            # Continue if files does not end with .pbf or .geojson
            if not file.endswith(".pbf") and not file.endswith(".geojson"):
                continue
            
            # Upload file to s3 bucket
            file_path = os.path.join(self.dataset_dir, file)
            if os.path.isfile(file_path):
                boto_client.upload_file(
                    file_path,
                    settings.AWS_BUCKET_NAME,
                    f"{self.s3_osm_basedir}/{file}",
                )
                print_info(f"Uploaded {file_path} to s3 bucket {settings.AWS_BUCKET_NAME}")
            else:
                print_warning(f"File {file_path} does not exist.")      
                             

    def clip_osm_by_bbox(self, bbox: str, filename: str, fileToClip: str):
        """Clips the OSM data by the polygon file"""

        raw_path = os.path.join(self.dataset_dir, fileToClip)
        clipped_filename = os.path.join(self.dataset_dir, filename)
        print_info("Clipping OSM data by polygon.")
        subprocess.run(
            f'osmconvert {raw_path} -b={bbox} -o={clipped_filename}',
            shell=True,
            check=True,
        )

    # def clip_osm_network_for_r5(self, db):
    #     """Clips a large OSM file into the R5 regions"""
    #     conf = Config("ways")
    #     download_url = conf.config["ways"]["region_pbf_r5"]
    #     download_link(directory=self.dataset_dir,
    #                   link=download_url, new_filename="raw.osm.pbf")

    #     regions = db.select(
    #         """SELECT id, CONCAT(ST_XMin(geom)::TEXT, ',', ST_YMin(geom)::text,',', ST_XMax(geom)::text, ',', ST_YMax(geom)) AS bbox
    #         FROM 
    #         (
	#             SELECT id, st_envelope(geom_buffer) AS geom  
	#             FROM region_gtfs rg 
    #             WHERE id < 7 
    #         ) x """
    #     )

    #     # TODO: Run this in parallel
    #     for region in regions:
    #         self.clip_osm_by_bbox(
    #             bbox=region[1], filename=f"region{region[0]}.osm.pbf", fileToClip="raw.osm.pbf")

    # def clip_data_to_osm(self, filename: str, fileToClip: str):
    #     """Clips data from a certain file to osm."""
    #     raw_path = os.path.join(self.dataset_dir, fileToClip)
    #     clipped_filename = os.path.join(self.dataset_dir, filename)
    #     print_info("Clipping to OSM")
    #     subprocess.run(
    #         f'osmconvert {raw_path} --complete-ways --drop-relations --drop-author --drop-version -o={clipped_filename}',
    #         shell=True,
    #         check=True,
    #     )