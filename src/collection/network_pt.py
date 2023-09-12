import os
import yaml
import subprocess
from pathlib import Path
from src.db.db import Database
from src.core.config import settings
from src.utils.utils import delete_file
from src.utils.utils import download_link_with_progress


class NetworkPTCollection():
    """Collects GTFS network and OSM data for a specified region and its sub-regions"""
    
    def __init__(self, db_rd, region, region_conf):
        self.db_rd = db_rd
        self.region = region
        self.s3_client = settings.S3_CLIENT
        self.region_osm_url = region_conf.get("region_pbf")
        self.region_gtfs_uri = region_conf.get("region_gtfs")
        self.sub_regions = self.db_rd.select(region_conf.get("sub_regions_query"))
    
    
    def collect_gtfs(self):
        """Downloads GTFS networks for all sub-regions within this region"""
        
        for id in self.sub_regions:
            id = int(id[0])
            print(f"Downloading GTFS network for region: {self.region}, sub-region: {id}")

            region_gtfs_network_dir = f"{settings.INPUT_DATA_DIR}/network_pt/{self.region}"
            if not os.path.exists(region_gtfs_network_dir):
                os.makedirs(region_gtfs_network_dir)

            self.s3_client.download_file(
                settings.AWS_BUCKET_NAME,
                f"network-pt/gtfs-regions/{self.region}/{id}.zip",
                f"{region_gtfs_network_dir}/{id}.zip"
            )
    
    
    def collect_osm(self):
        """Downloads and crops OSM data for all sub-regions within this region"""
        
        # Download OSM data for this region
        print(f"Downloading OSM data for region: {self.region}")
        region_osm_data_dir = f"{settings.INPUT_DATA_DIR}/network_pt/{self.region}"
        region_osm_data_filename = os.path.basename(self.region_osm_url)
        download_link_with_progress(url=self.region_osm_url, output_directory=region_osm_data_dir)
        
        # Split OSM data into sub-regions as per GTFS network boundaries
        print(f"Generating sub-regions for region: {self.region}")
        sub_region_output_dir = f"{settings.OUTPUT_DATA_DIR}/network_pt/{self.region}"
        if not os.path.exists(sub_region_output_dir):
            os.makedirs(sub_region_output_dir)
        
        for id in self.sub_regions:
            id = int(id[0])
            sub_region_poly_file_path = f"{settings.OUTPUT_DATA_DIR}/network_pt/{self.region}/{id}.poly"
            self.generate_polygon_file(sub_region_id=id, file_path=sub_region_poly_file_path)
            self.crop_osm_polygon(region_osm_data_path=f"{region_osm_data_dir}/{region_osm_data_filename}", sub_region_id=id)
            delete_file(file_path=sub_region_poly_file_path)
            self.s3_client.upload_file(
                f"{settings.OUTPUT_DATA_DIR}/network_pt/{self.region}/{id}.pbf",
                settings.AWS_BUCKET_NAME,
                f"network-pt/osm-regions/{self.region}/{id}.pbf"
            )
            
        print("Done!")
        
    
    def generate_polygon_file(self, sub_region_id: int, file_path: str):
        """Generates polygon filter files for cropping a region into subregions"""
        
        coordinates = self.db_rd.select(f"""SELECT ST_x(coord.geom), ST_y(coord.geom)
                                            FROM (
                                                SELECT (ST_dumppoints(buffer_geom)).geom
                                                FROM public.gtfs_regions
                                                WHERE id = {sub_region_id}
                                            ) coord;"""
                                        )
        with open(file_path, "w") as file:
            file.write(f"{sub_region_id}\n")
            file.write("polygon\n")
            file.write("\n".join([f" {i[0]} {i[1]}" for i in coordinates]))
            file.write("\nEND\nEND")
            
    
    
    def crop_osm_polygon(self, region_osm_data_path:str, sub_region_id:int):
        """Crops OSM data as per polygon file"""
        
        sub_region_osm_polygon_path = f"{settings.OUTPUT_DATA_DIR}/network_pt/{self.region}/{sub_region_id}.poly"
        sub_region_osm_data_path = f"{settings.OUTPUT_DATA_DIR}/network_pt/{self.region}/{sub_region_id}.pbf"
        subprocess.run(
            f"osmconvert {region_osm_data_path} -B={sub_region_osm_polygon_path} --complete-ways -o={sub_region_osm_data_path}",
            shell=True,
            check=True,
        )
        
    def upload_file_s3(self, filename: str, file_path: str, bucket_path: str):
        """Upload a file to an S3 bucket"""
        
        self.s3_client.upload_file(
            file_path,
            bucket_path,
            filename
        )


def collect_network_pt(region: str):
    """Main function."""
    
    db_rd = Database(settings.RAW_DATABASE_URI)
    
    config_path = os.path.join(settings.CONFIG_DIR, "data_variables", "network_pt", "network_pt" + "_" + region + ".yaml")
    config_file = open(config_path)
    region_conf = yaml.safe_load(config_file)
    
    network_pt_collection = NetworkPTCollection(db_rd=db_rd, region=region, region_conf=region_conf)
    network_pt_collection.collect_gtfs()
    network_pt_collection.collect_osm()
    
    config_file.close()
    db_rd.conn.close()


if __name__ == "__main__":
    collect_network_pt()
