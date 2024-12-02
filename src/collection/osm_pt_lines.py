from src.collection.osm_collection_base import OSMCollection
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import print_error, print_info, timing


class OSMPTLinesCollection(OSMCollection):
    """Collects public transport lines from OSM."""

    def __init__(self, db_config: dict, region: str):
        self.db_config = db_config
        super().__init__(self.db_config, dataset_type="osm_pt_lines", region=region)

    def validate_config(self):
        """Validate the data config."""

        # Check if modes are provided
        modes = self.data_config.collection.get("modes")
        if not modes:
            raise ValueError("Modes must be specified in the data config")
        print_info(f"Running OSM PT Lines collection for modes: {modes}")

        # Check if the osm2pgsql style is provided
        osm2pgsql_style = self.data_config.collection.get("osm2pgsql_style")
        if not osm2pgsql_style:
            raise ValueError("osm2pgsql style must be specified in the data config")
        else:
            print_info(f"Using custom osm2pgsql style: {osm2pgsql_style}")

    def run(self):
        """Run OSM public transport lines collection process."""

        # Validate the data config
        self.validate_config()

        # Build OSM filter for public transport lines
        drop_filter = '--drop="public_transport=platform"'
        keep_filter = '--keep="( type=route_master or type=route ) and public_transport:version=2"'
        keep_relations_filter = '--keep-relations="'
        for mode in self.data_config.collection.get("modes"):
            keep_relations_filter += f"route_master={mode} or route={mode} or "
        keep_relations_filter = keep_relations_filter[:-4] + '"'
        osm_filter = f'{drop_filter} {keep_filter} {keep_relations_filter}'

        # Download, prepare and import OSM data
        self.download_bulk_osm()
        self.prepare_bulk_osm(osm_filter=osm_filter)
        self.merge_osm_and_import()


@timing
def collect_osm_pt_lines(region: str):
    print_info(f"Collecting OSM PT Lines data for region: {region}")
    db = Database(settings.LOCAL_DATABASE_URI)

    try:
        OSMPTLinesCollection(
            db_config=db.db_config,
            region=region,
        ).run()
        print_info(f"Finished collecting OSM PT Lines data for region: {region}")
    except Exception as e:
        print_error(f"Failed to collect OSM PT Lines data for region: {region}")
        raise e
    finally:
        db.close()
