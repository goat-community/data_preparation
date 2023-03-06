from osm_collection_base import OSMBaseCollection
from src.core.config import settings
from src.db.db import Database
from src.config.config import Config

class OSMPOICollection(OSMBaseCollection):
    """Collects all POIs from OSM."""
    def __init__(self, db_config, region):
        self.db_config = db_config
        super().__init__(self.db_config, dataset_type="poi", region=region)
        
    def poi_collection(self):
        """Collects all POIs from OSM."""
        # Create OSM filter for POIs
        osm_filter = " ".join([i + "=" for i in self.data_config.collection["osm_tags"].keys()])
        osm_filter = ""
        for tag in self.data_config.collection["osm_tags"]:
            osm_filter += tag
            for tag_value in self.data_config.collection["osm_tags"][tag]:
                osm_filter += "=" + tag_value + " "

        # Remove not needed osm feature categories
        if self.data_config.collection["nodes"] == False:
            osm_filter += "--drop-nodes "
        if self.data_config.collection["ways"] == False:
            osm_filter += "--drop-ways "
        if self.data_config.collection["relations"] == False:
            osm_filter += "--drop-relations "

        self.download_bulk_osm()
        self.prepare_bulk_osm(osm_filter=osm_filter)
        self.merge_osm_and_import()
        

def main():
    """Main function."""
    db = Database(settings.LOCAL_DATABASE_URI)
    osm_poi_collection = OSMPOICollection(db_config=db.db_config, region="at")

    
    osm_poi_collection.poi_collection()
    osm_poi_collection.export_osm_boundaries_db(db=db)
    osm_poi_collection.upload_raw_osm_data(boto_client=settings.S3_CLIENT)
    
    
if __name__ == "__main__":
    main()
    