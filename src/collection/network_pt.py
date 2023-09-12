import os
import subprocess
from pathlib import Path
from src.db.db import Database
from src.config.config import Config
from src.core.config import settings
from src.utils.utils import delete_file
from src.utils.utils import ensure_dir_exists
from src.utils.utils import download_link_with_progress


class NetworkPTCollection():
    """Collects GTFS network and OSM data for a specified region and its sub-regions"""
    
    def __init__(self, db_rd, config, region):
        self.db_rd = db_rd
        self.region = region
        self.region_osm_url = config.get("region_pbf")
        self.s3_sub_region_osm_dir = config.get("s3_sub_region_osm_dir")
        self.s3_sub_region_gtfs_dir = config.get("s3_sub_region_gtfs_dir")
        self.sub_regions = self.db_rd.select(config.get("sub_regions_query"))
        
        self.region_osm_filename = os.path.basename(self.region_osm_url)
        self.region_osm_input_dir = os.path.join(settings.INPUT_DATA_DIR, "network_pt", region)
        self.sub_region_gtfs_input_dir = os.path.join(settings.INPUT_DATA_DIR, "network_pt", region)
        self.sub_region_osm_output_dir = os.path.join(settings.OUTPUT_DATA_DIR, "network_pt", region)
    
    
    def collect_osm(self):
        """Downloads the latest OSM data for this region"""
        
        print(f"Downloading OSM data for region: {self.region}")
        download_link_with_progress(
            url=self.region_osm_url, 
            output_directory=self.region_osm_input_dir
        )
    
    
    def collect_gtfs(self):
        """Downloads GTFS networks for all sub-regions within this region"""
        
        for id in self.sub_regions:
            id = int(id[0])
            print(f"Downloading GTFS network for region: {self.region}, sub-region: {id}")
            ensure_dir_exists(dir_path=self.sub_region_gtfs_input_dir)
            settings.S3_CLIENT.download_file(
                settings.AWS_BUCKET_NAME,
                f"{self.s3_sub_region_gtfs_dir}/{id}.zip",
                os.path.join(self.sub_region_gtfs_input_dir, f"{id}.zip")
            )
    
    
    def process_osm(self):
        """Crops OSM data for all sub-regions within this region"""
        
        # Generate sub-region polygon filters
        print(f"Generating sub-region filters for region: {self.region}")
        ensure_dir_exists(dir_path=self.sub_region_osm_output_dir)
        for id in self.sub_regions:
            id = int(id[0])
            self.generate_polygon_file(
                sub_region_id=id,
                dest_file_path=os.path.join(self.sub_region_osm_output_dir, f"{id}.poly")
            )
        
        # Crop region OSM data as per sub-region polygon filters
        for id in self.sub_regions:
            id = int(id[0])
            print(f"Cropping OSM data for region: {self.region}, sub-region: {id}")
            self.crop_osm_polygon(
                orig_file_path=os.path.join(self.region_osm_input_dir, self.region_osm_filename),
                dest_file_path=os.path.join(self.sub_region_osm_output_dir, f"{id}.pbf"),
                poly_file_path=os.path.join(self.sub_region_osm_output_dir, f"{id}.poly")
            )
            delete_file(file_path=os.path.join(self.sub_region_osm_output_dir, f"{id}.poly"))
    
    
    def upload_osm(self):
        """Uploads cropped OSM sub-region data to S3"""
        
        for id in self.sub_regions:
            id = int(id[0])
            print(f"Uploading cropped OSM data for region: {self.region}, sub-region: {id}")
            settings.S3_CLIENT.upload_file(
                os.path.join(self.sub_region_osm_output_dir, f"{id}.pbf"),
                settings.AWS_BUCKET_NAME,
                f"{self.s3_sub_region_osm_dir}/{id}.pbf"
            )
        
    
    def generate_polygon_file(self, sub_region_id: int, dest_file_path: str):
        """Generates polygon filter files for cropping a region into subregions"""
        
        coordinates = self.db_rd.select(f"""SELECT ST_x(coord.geom), ST_y(coord.geom)
                                            FROM (
                                                SELECT (ST_dumppoints(buffer_geom)).geom
                                                FROM public.gtfs_regions
                                                WHERE id = {sub_region_id}
                                            ) coord;"""
                                        )
        with open(dest_file_path, "w") as file:
            file.write(f"{sub_region_id}\n")
            file.write("polygon\n")
            file.write("\n".join([f" {i[0]} {i[1]}" for i in coordinates]))
            file.write("\nEND\nEND")
            
    
    
    def crop_osm_polygon(self, orig_file_path: str, dest_file_path: str, poly_file_path: str):
        """Crops OSM data as per polygon file"""
        
        subprocess.run(
            f"osmconvert {orig_file_path} -B={poly_file_path} --complete-ways -o={dest_file_path}",
            shell=True,
            check=True,
        )


def collect_network_pt(region: str):
    """Main function."""
    
    db_rd = Database(settings.RAW_DATABASE_URI)
    config = Config(name="network_pt", region=region)
    network_pt_collection = NetworkPTCollection(
        db_rd=db_rd,
        config=config.config,
        region=region
    )
    network_pt_collection.collect_osm()
    network_pt_collection.collect_gtfs()
    network_pt_collection.process_osm()
    network_pt_collection.upload_osm()
    db_rd.conn.close()


if __name__ == "__main__":
    collect_network_pt()
