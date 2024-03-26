from src.collection.osm_collection_base import OSMCollection
from src.core.config import settings
from src.db.db import Database
from src.config.config import Config
import subprocess
from src.utils.utils import print_info
from src.core.config import settings

class OSMNetworkCollection(OSMCollection):
    """Collects all POIs from OSM."""
    def __init__(self, db_config, region):
        self.db_config = db_config
        self.dbname = db_config.path[1:]
        self.user = db_config.user
        self.host = db_config.host
        self.port = db_config.port
        self.password = db_config.password
        self.cache = 100000
        super().__init__(self.db_config, dataset_type="network", region=region)
    
      
    def network_collection(self, db: Database):
        """Creates and imports the network using osm2pgsql into the database"""

        self.download_bulk_osm()
        self.prepare_bulk_osm(
            osm_filter="highway= cycleway= junction=",
        )

        # Merge all osm files
        print_info("Merging files")
        file_names = [f.split("/")[-1] for f in self.region_links]
        subprocess.run(
            f'osmium merge {" ".join(file_names)} -o merged.osm.pbf',
            shell=True,
            check=True,
        )
        subprocess.run(
            f"osmconvert merged.osm.pbf -o=merged.osm", shell=True, check=True
        )

        total_cnt_links = len(self.region_links)
        cnt_link = 0

        for link in self.region_links:
            cnt_link += 1
            full_name = link.split("/")[-1]
            network_file_name = full_name.split(".")[0] + "_network.osm"
            print_info(f"Importing {full_name}")

            if cnt_link == 1 and cnt_link == total_cnt_links:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --clean --conf {settings.CONFIG_DIR}/data_variables/network/mapconfig.xml --chunk 40000",
                    shell=True,
                    check=True,
                )
            elif cnt_link == 1:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --no-index --clean --conf {settings.CONFIG_DIR}/data_variables/network/mapconfig.xml --chunk 40000",
                    shell=True,
                    check=True,
                )
            elif cnt_link != total_cnt_links:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --no-index --conf {settings.CONFIG_DIR}/data_variables/network/mapconfig.xml --chunk 40000",
                    shell=True,
                    check=True,
                )
            else:
                subprocess.run(
                    f"PGPASSFILE=~/.pgpass_{self.dbname} osm2pgrouting --dbname {self.dbname} --host {self.host} --username {self.username}  --file {network_file_name} --conf {settings.CONFIG_DIR}/data_variables/network/mapconfig.xml --chunk 40000",
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
        
def collect_network(region: str):
    """Main function."""
    db = Database(settings.LOCAL_DATABASE_URI)
    osm_poi_collection = OSMNetworkCollection(db_config=db.db_config, region=region)

    osm_poi_collection.network_collection(db=db)
    osm_poi_collection.export_osm_boundaries_db(db=db)
    osm_poi_collection.upload_raw_osm_data(boto_client=settings.S3_CLIENT)
    db.conn.close()
    
if __name__ == "__main__":
    collect_network()
    