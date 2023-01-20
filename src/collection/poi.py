from osm_collection_base import OSMBaseCollection
from src.config.config import Config
from src.db.config import DATABASE, DATABASE_RD

class OSMPOICollection(OSMBaseCollection):
    """Collects all POIs from OSM."""
    def __init__(self, db_config):
        super().__init__(db_config)
        self.config = Config("poi")
        
    def pois_collection(self):
        """Collects all POIs from OSM."""
        region_links = self.config.pbf_data
        # Create OSM filter for POIs
        osm_filter = " ".join([i + "=" for i in self.config.collection["osm_tags"].keys()])
        osm_filter = ""
        for tag in self.config.collection["osm_tags"]:
            osm_filter += tag
            for tag_value in self.config.collection["osm_tags"][tag]:
                osm_filter += "=" + tag_value + " "

        # Remove not needed osm feature categories
        if self.config.collection["nodes"] == False:
            osm_filter += "--drop-nodes "
        if self.config.collection["ways"] == False:
            osm_filter += "--drop-ways "
        if self.config.collection["relations"] == False:
            osm_filter += "--drop-relations "

        self.download_bulk_osm(region_links)
        self.prepare_bulk_osm(region_links, "poi", osm_filter=osm_filter)
        self.merge_osm_and_import(region_links, self.config)


def main():
    """Main function."""
    osm_poi_collection = OSMPOICollection(db_config=DATABASE)
    osm_poi_collection.pois_collection()
    
if __name__ == "__main__":
    main()
    