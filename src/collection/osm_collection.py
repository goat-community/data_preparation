import subprocess
import os
import sys
import time
import psutil

from urllib import request
from src.other.utils import (
    print_info,
    download_link,
    delete_dir,
    print_hashtags,
    delete_file,
    print_warning,
    return_tables_as_gdf
)
from shapely.geometry import MultiPolygon, Polygon

from src.config.config import Config
from src.db.config import DATABASE, DATABASE_RD
from src.db.db import Database

from src.other.utility_functions import create_pgpass
from decouple import config
from functools import partial
from multiprocessing.pool import Pool
from time import time


class OsmCollection:
    def __init__(self, db_config):
        self.dbname = db_config["dbname"]
        self.host = db_config["host"]
        self.username = db_config["user"]
        self.port = db_config["port"]
        self.password = db_config["password"]
        self.root_dir = "/app"
        self.data_dir_input = self.root_dir + "/src/data/input/"
        self.temp_data_dir = self.data_dir_input + "temp/"
        self.available_cpus = os.cpu_count()
        self.memory = psutil.virtual_memory().total
        self.cache = round(self.memory / 1073741824 * 1000 * 0.75)
        create_pgpass()

    def prepare_osm_data(self, link: str, dataset_type: str, osm_filter: str):
        """Prepare OSM data for import into PostGIS database.

        Args:
            link (str): Download link to OSM data.
            dataset_type (str): Type of dataset.
        """

        full_name = link.split("/")[-1]
        only_name = full_name.split(".")[0]
        subprocess.run(
            f"osmconvert {full_name} --drop-author --drop-version --out-osm -o={only_name}.o5m",
            shell=True,
            check=True,
        )
        subprocess.run(
            f'osmfilter {only_name}.o5m -o={only_name + "_" + dataset_type}.o5m --keep="{osm_filter}"',
            shell=True,
            check=True,
        )
        subprocess.run(
            f'osmconvert {only_name + "_" + dataset_type}.o5m -o={only_name + "_" + dataset_type}.osm',
            shell=True,
            check=True,
        )
        print_info(f"Preparing file {full_name}")

    @staticmethod
    def parse_poly(dir):
        """Parse an Osmosis polygon filter file.
        Based on: https://wiki.openstreetmap.org/wiki/Osmosis/Polygon_Filter_File_Python_Parsing
        Args:
            dir (_type_): _description_

        Returns:
            (shapely.geometry.multipolygon): Returns the polygon in the poly foramat as a shapely multipolygon.
        """

        in_ring = False
        coords = []
        with open(dir, "r") as polyfile:
            for (index, line) in enumerate(polyfile):
                if index == 0:
                    # first line is junk.
                    continue

                elif index == 1:
                    # second line is the first polygon ring.
                    coords.append([[], []])
                    ring = coords[-1][0]
                    in_ring = True

                elif in_ring and line.strip() == "END":
                    # we are at the end of a ring, perhaps with more to come.
                    in_ring = False

                elif in_ring:
                    # we are in a ring and picking up new coordinates.
                    ring.append(list(map(float, line.split())))

                elif not in_ring and line.strip() == "END":
                    # we are at the end of the whole polygon.
                    break

                elif not in_ring and line.startswith("!"):
                    # we are at the start of a polygon part hole.
                    coords[-1][1].append([])
                    ring = coords[-1][1][-1]
                    in_ring = True

                elif not in_ring:
                    # we are at the start of a polygon part.
                    coords.append([[], []])
                    ring = coords[-1][0]
                    in_ring = True

            return MultiPolygon(coords)

    def create_osm_extract_boundaries(self, db):
        """Create OSM extract boundaries.
        Args:
            db (Database): Database object.
        """

        region_poly_links = []
        for link in Config("ways").pbf_data:
            region_poly_links.append(
                os.path.dirname(link)
                + "/"
                + os.path.basename(link).split("-latest")[0]
                + ".poly"
            )

        download = partial(download_link, self.temp_data_dir)
        pool = Pool(processes=self.available_cpus)
        print_hashtags()
        print_info(f"Downloading OSM Poly Boundaries started.")
        print_hashtags()
        pool.map(download, region_poly_links)
        pool.close()
        pool.join()

        db.perform("DROP TABLE IF EXISTS osm_extract_boundaries;")
        sql_create_table = """
            CREATE TABLE osm_extract_boundaries
            (
                id serial,
                name text,
                geom geometry(MULTIPOLYGON, 4326)
            );
            ALTER TABLE osm_extract_boundaries ADD PRIMARY KEY(id);
            CREATE INDEX ON osm_extract_boundaries USING GIST(geom); 
        """
        db.perform(sql_create_table)

        for link in region_poly_links:
            file_dir = self.temp_data_dir + os.path.basename(link)
            geom = self.parse_poly(file_dir)
            sql_insert = """
                INSERT INTO osm_extract_boundaries(name, geom)
                SELECT %s, ST_GEOMFROMTEXT(%s);
            """
            db.perform(
                query=sql_insert,
                params=[os.path.basename(link).split("-latest")[0], geom.wkt],
            )

    def import_dem(self, filepath=None):
        """Import DEM data into PostGIS database.

        Args:
            filepath (str, optional): Filepath to file can specified. Defaults to None and therefore will use default data directory.
        """
        if not filepath:
            filepath = self.data_dir_input + "dem.tif"

        if not os.path.exists(filepath):
            print_warning(f"{filepath} for dem.tif does not exist.")
            sys.exit()

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

    def download_bulk_osm(self, region_links: list):
        # Cleanup
        delete_dir(self.temp_data_dir)
        os.mkdir(self.temp_data_dir)
        os.chdir(self.temp_data_dir)

        # Download all needed files
        download = partial(download_link, "")
        pool = Pool(processes=self.available_cpus)

        print_hashtags()
        print_info(f"Downloading OSM files started.")
        print_hashtags()
        pool.map(download, region_links)
        pool.close()
        pool.join()

    def prepare_bulk_osm(
        self, region_links: list, dataset_type: str, osm_filter: str
    ):
        pool = Pool(processes=self.available_cpus)

        # Prepare and filter osm files
        print_hashtags()
        print_info(f"Preparing OSM files started.")
        print_hashtags()
        pool.map(
            partial(
                self.prepare_osm_data,
                dataset_type=dataset_type,
                osm_filter=osm_filter,
            ),
            region_links,
        )
        pool.close()
        pool.join()

           
    def merge_osm_and_import(self, region_links: list, conf: Config):
        # Merge all osm files
        print_info("Merging files")
        file_names = [
            f.split("/")[-1].split(".")[0] + f"_{conf.name}.osm" for f in region_links
        ]
        subprocess.run(
            f'osmium merge {" ".join(file_names)} -o merged.osm',
            shell=True,
            check=True,
        )
        
        # Import merged osm file using customer osm2pgsql style
        conf.osm2pgsql_create_style()
        subprocess.run(
            f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgsql -d {self.dbname} -H {self.host} -U {self.username} --port {self.port} --hstore -E 4326 -r .osm -c "
            + "merged.osm"
            + f" -s --drop -C {self.cache} --style /app/src/data/temp/{conf.name}_p4b.style --prefix osm_{conf.name}",
            shell=True,
            check=True,
        )
        
    def pois_collection(self):
        """Collects all POIs from OSM."""
        conf = Config("pois")
        region_links = conf.pbf_data

        # Create OSM filter for POIs
        osm_filter = " ".join([i + "=" for i in conf.collection["osm_tags"].keys()])
        osm_filter = ""
        for tag in conf.collection["osm_tags"]:
            osm_filter += tag
            for tag_value in conf.collection["osm_tags"][tag]:
                osm_filter += "=" + tag_value + " "

        # Remove not needed osm feature categories
        if conf.collection["nodes"] == False:
            osm_filter += "--drop-nodes "
        if conf.collection["ways"] == False:
            osm_filter += "--drop-ways "
        if conf.collection["relations"] == False:
            osm_filter += "--drop-relations "

        self.download_bulk_osm(region_links)
        self.prepare_bulk_osm(region_links, "pois", osm_filter=osm_filter)
        self.merge_osm_and_import(region_links, conf)
        
    def building_collection(self):
        """Collects all building from OSM"""
        conf = Config("buildings")
        region_links = conf.pbf_data
        osm_filter = "building= --drop-nodes --drop-relations"
        
        self.download_bulk_osm(region_links)
        self.prepare_bulk_osm(region_links, "buildings", osm_filter=osm_filter)
        self.merge_osm_and_import(region_links, conf)
                
    def network_collection(self, db):
        """Creates and imports the network using osm2pgsql into the database"""
        conf = Config("ways")
        region_links = conf.pbf_data
        self.download_bulk_osm(region_links)
        self.prepare_bulk_osm(
            db,
            region_links,
            dataset_type="network",
            osm_filter="highway= cycleway= junction=",
        )

        # Merge all osm files
        print_info("Merging files")
        file_names = [f.split("/")[-1] for f in region_links]
        subprocess.run(
            f'osmium merge {" ".join(file_names)} -o merged.osm.pbf',
            shell=True,
            check=True,
        )
        subprocess.run(
            f"osmconvert merged.osm.pbf -o=merged.osm", shell=True, check=True
        )

        total_cnt_links = len(region_links)
        cnt_link = 0

        for link in region_links:
            cnt_link += 1
            full_name = link.split("/")[-1]
            network_file_name = full_name.split(".")[0] + "_network.osm"
            print_info(f"Importing {full_name}")

            if cnt_link == 1 and cnt_link == total_cnt_links:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --clean --conf {self.root_dir}/src/config/mapconfig.xml --chunk 40000",
                    shell=True,
                    check=True,
                )
            elif cnt_link == 1:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --no-index --clean --conf {self.root_dir}/src/config/mapconfig.xml --chunk 40000",
                    shell=True,
                    check=True,
                )
            elif cnt_link != total_cnt_links:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --no-index --conf {self.root_dir}/src/config/mapconfig.xml --chunk 40000",
                    shell=True,
                    check=True,
                )
            else:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --conf {self.root_dir}/src/config/mapconfig.xml --chunk 40000",
                    shell=True,
                    check=True,
                )

        # Import all OSM data using OSM2pgsql
        # TODO: Avoid creating osm_planet_polygon table here
        subprocess.run(
            f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgsql -d {self.dbname} -H {self.host} -U {self.username} --port {self.port} --hstore -E 4326 -r pbf -c merged.osm.pbf -s --drop -C {self.cache}",
            shell=True,
            check=True,
        )
        db.perform(query="CREATE INDEX ON planet_osm_line (osm_id);")
        db.perform(query="CREATE INDEX ON planet_osm_point (osm_id);")
        db.perform(query="CREATE INDEX ON planet_osm_line USING GIST(way);")
        db.perform(query="CREATE INDEX ON planet_osm_point USING GIST(way);")

    
    def clip_osm_by_bbox(self, bbox: str, filename: str):
        """Clips the OSM data by the polygon file"""
        
        raw_path = os.path.join(self.temp_data_dir,'raw.osm.pbf')
        clipped_filename = os.path.join(self.temp_data_dir, filename)
        print_info("Clipping OSM data by polygon.")
        subprocess.run(
            f'osmconvert {raw_path} -b={bbox} -o={clipped_filename}',
            shell=True,
            check=True,
        )
        
    def clip_osm_network_for_r5(self, db):
        """Clips a large OSM file into the R5 regions"""
        conf = Config("ways")
        download_url = conf.config["ways"]["region_pbf_r5"]
        download_link(directory=self.temp_data_dir, link=download_url, new_filename="raw.osm.pbf")
        
        regions = db.select(
            """SELECT id, CONCAT(ST_XMin(geom)::TEXT, ',', ST_YMin(geom)::text,',', ST_XMax(geom)::text, ',', ST_YMax(geom)) AS bbox
            FROM 
            (
	            SELECT id, st_envelope(geom_buffer) AS geom  
	            FROM region_gtfs rg 
                WHERE id < 7 
            ) x """
        )

        # TODO: Run this in parallel  
        for region in regions:
            self.clip_osm_by_bbox(bbox=region[1], filename=f"region{region[0]}.osm.pbf")


# db = Database(DATABASE_RD)
# osm_collection = OsmCollection(DATABASE_RD)
# osm_collection.clip_osm_network_for_r5(db)

# osm_collection.building_collection()
# db.conn.close()