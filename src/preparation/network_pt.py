import os
import time

import requests

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database

R5_FRONTEND_URL_REGIONS = f"{settings.R5_FRONTEND_HOST}:{settings.R5_FRONTEND_PORT}/api/db/regions"
R5_BACKEND_URL_BUNDLE = f"{settings.R5_BACKEND_HOST}:{settings.R5_BACKEND_PORT}/api/bundle"


class NetworkPTPreparation:
    """Class to upload processed GTFS & OSM data to our R5 routing engine"""
    
    def __init__(self, db_rd, config, region):
        self.db_rd = db_rd
        self.region = region
        self.sub_regions = self.db_rd.select(config.get("sub_regions_query"))
        
        self.sub_region_gtfs_input_dir = os.path.join(settings.INPUT_DATA_DIR, "network_pt", region)
        self.sub_region_osm_output_dir = os.path.join(settings.OUTPUT_DATA_DIR, "network_pt", region)
        
        self.headers = {}
        if settings.R5_AUTHORIZATION:
            self.headers["Authorization"] = settings.R5_AUTHORIZATION
    
    
    def upload_processed_data(self):
        """Creates network bundles for every sub-region to upload OSM & GTFS data"""
        
        for id in self.sub_regions:
            id = int(id[0])
            print(f"Creating R5 region for region: {self.region}, sub-region: {id}")
            region_name = f"region-{self.region}_{id}"
            success = self.delete_region_r5(name=region_name) # Delete old region
            if not success:
                print(f"Unable to delete old R5 region: {region_name}")
                break
            region_id = self.create_region_r5( # Create new region with latest bounds for this sub-region
                name=region_name,
                description="",
                bounds=self.get_sub_region_bounds(id=id)
            )
            if region_id is None:
                print(f"Unable to create new R5 region: {region_name}")
                break
            
            print(f"Creating R5 network bundle for region: {self.region}, sub-region: {id}")
            bundle_name = f"bundle-{self.region}_{id}"
            success = self.delete_bundle_r5(name=bundle_name) # Delete old bundle
            if not success:
                print(f"Unable to delete old R5 bundle: {bundle_name}")
                break
            bundle_id = self.create_bundle_r5( # Create new bundle with latest OSM & GTFS data for this sub-region
                name=bundle_name,
                region_id=region_id,
                osm_path=os.path.join(self.sub_region_osm_output_dir, f"{id}.pbf"),
                gtfs_path=os.path.join(self.sub_region_gtfs_input_dir, f"{id}.zip")
            )
            if bundle_id is None:
                print(f"Unable to create new R5 bundle: {bundle_name}")
                break
            
            # Wait until previous bundle is processed
            bundle_status = self.get_bundle_status_r5(id=bundle_id)
            while bundle_status == "PROCESSING_OSM":
                time.sleep(10)
                bundle_status = self.get_bundle_status_r5(id=bundle_id)
            if bundle_status != "DONE":
                print(f"R5 engine failed to process bundle: {bundle_name}")
                break
    
    
    def get_sub_region_bounds(self, id):
        bounds = self.db_rd.select(
            f"""SELECT ST_XMin(buff_geom), ST_YMin(buff_geom), ST_XMax(buff_geom), ST_YMax(buff_geom)
                FROM 
                (
                    SELECT st_envelope(buffer_geom) AS buff_geom  
                    FROM public.gtfs_regions 
                    WHERE id = {id}
                ) bg;"""
        )
        return (bounds[0][0], bounds[0][1], bounds[0][2], bounds[0][3])
    
    
    def get_region_id_r5(self, name: str):
        """Get the ID of a previously created region in R5"""
        
        region_id = None
        response = requests.get(url=R5_FRONTEND_URL_REGIONS, headers=self.headers)
        if response.status_code == 200:
            for region in response.json():
                if region["name"] == name:
                    region_id = region["_id"]
                    break
        return region_id
    
    
    def delete_region_r5(self, name: str):
        """Deletes a region from R5"""
        
        region_id = self.get_region_id_r5(name=name)
        if region_id is None:
            return True
        response = requests.delete(
            url=f"{R5_FRONTEND_URL_REGIONS}/{region_id}",
            headers=self.headers
        )
        return (response.status_code == 200)
    
    
    def create_region_r5(self, name: str, description: str, bounds: tuple[float]):
        """Creates a region in R5"""
        
        request_body = {
            "name": name,
            "description": description,
            "bounds": {
                "north": bounds[3], # ymax
                "south": bounds[1], # ymin
                "east": bounds[2], # xmax
                "west": bounds[0], # xmin
            }
        }
        response = requests.post(
            url=R5_FRONTEND_URL_REGIONS,
            json=request_body,
            headers=self.headers
        )
        region_id = None
        if response.status_code == 201:
            region_id = response.json()["_id"]
        return region_id
    
    
    def get_bundle_id_r5(self, name: str):
        """Get the ID of a previously created bundle in R5"""
        
        bundle_id = None
        response = requests.get(url=R5_BACKEND_URL_BUNDLE, headers=self.headers)
        if response.status_code == 200:
            for bundle in response.json():
                if bundle["name"] == name:
                    bundle_id = bundle["_id"]
                    break
        return bundle_id
    
    
    def get_bundle_status_r5(self, id: str):
        """Get the status of a bundle in R5"""
        
        response = requests.get(url=f"{R5_BACKEND_URL_BUNDLE}/{id}", headers=self.headers)
        return response.json()["status"] if response.status_code == 200 else None
    
    
    def delete_bundle_r5(self, name: str):
        """Deletes a bundle from R5"""
        
        bundle_id = self.get_bundle_id_r5(name=name)
        if bundle_id is None:
            return True
        response = requests.delete(
            url=f"{R5_BACKEND_URL_BUNDLE}/{bundle_id}",
            headers=self.headers
        )
        return (response.status_code == 200)
    
    
    def create_bundle_r5(self, name: str, region_id: str, osm_path: str, gtfs_path: str):
        """Creates a network bundle (GTFS + OSM data) in R5"""
        
        request_body = {
            "bundleName": name,
            "regionId": region_id
        }
        data_files = {
            "osm": (os.path.basename(osm_path), open(osm_path, "rb")),
            "feedGroup": (os.path.basename(gtfs_path), open(gtfs_path, "rb"))
        }
        response = requests.post(
            url=R5_BACKEND_URL_BUNDLE,
            data=request_body,
            files=data_files,
            headers=self.headers
        )
        return response.json()["_id"] if response.status_code == 200 else None
    

def prepare_network_pt(region: str):
    """Main function"""
    
    db_rd = Database(settings.RAW_DATABASE_URI)
    try:
        config = Config(name="network_pt", region=region)
        network_pt_preparation = NetworkPTPreparation(
            db_rd=db_rd,
            config=config.config,
            region=region
        )
        network_pt_preparation.upload_processed_data()
    except Exception as e:
        print(e)
        raise e
    finally:
        db_rd.conn.close()
