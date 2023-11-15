import os
import subprocess

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import (
    delete_dir,
    download_link,
    get_region_bbox_coords,
    make_dir,
    print_error,
    print_info,
)


class OvertureNetworkPreparation:

    def __init__(self, db_local: Database, db_remote: Database, region: str):
        self.db_local = db_local
        self.db_remote = db_remote
        self.region = region
        self.config = Config("network_overture", region)

        self.data_schema = self.config.collection["output_schema"]
        self.data_table_segments = self.config.collection["output_table_segments"]
        self.data_table_connectors = self.config.collection["output_table_connectors"]

        self.output_schema = self.config.preparation["output_schema"]
        self.output_table_dem = self.config.preparation["output_table_dem"]

        self.DEM_S3_URL = "https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com"


    def initialize_dem_table(self):
        """Initialize digital elevation model (DEM) raster table."""

        sql_create_dem_table = f"""
            DROP TABLE IF EXISTS {self.output_schema}.{self.output_table_dem};
            CREATE TABLE {self.output_schema}.{self.output_table_dem} (
                rid serial4 NOT NULL,
                rast public.raster NULL,
                filename text NULL,
                CONSTRAINT {self.output_table_dem}_pkey PRIMARY KEY (rid),
                CONSTRAINT enforce_nodata_values_rast CHECK ((_raster_constraint_nodata_values(rast) = '{{NULL}}'::numeric[])),
                CONSTRAINT enforce_num_bands_rast CHECK ((st_numbands(rast) = 1)),
                CONSTRAINT enforce_out_db_rast CHECK ((_raster_constraint_out_db(rast) = '{{f}}'::boolean[])),
                CONSTRAINT enforce_pixel_types_rast CHECK ((_raster_constraint_pixel_types(rast) = '{{32BF}}'::text[])),
                CONSTRAINT enforce_srid_rast CHECK ((st_srid(rast) = 4326))
            );
            CREATE INDEX {self.output_table_dem}_st_convexhull_idx ON {self.output_schema}.{self.output_table_dem} USING gist (st_convexhull(rast));
        """
        self.db_local.perform(sql_create_dem_table)


    def get_filtered_dem_tiles(self, region_bbox_coords: dict, full_tile_list: list):
        """Filter digital elevation model (DEM) tiles to our region of interest."""

        # Filter DEM tiles
        filtered_tile_list= []
        for tile_filename in full_tile_list:
            tile_x = 0.0
            tile_y = 0.0

            if "_N" in tile_filename:
                tile_y = float(tile_filename.split("_N")[1].split("_00")[0])
            elif "_S" in tile_filename:
                tile_y = float(tile_filename.split("_S")[1].split("_00")[0]) * -1.0

            if "_E" in tile_filename:
                tile_x = float(tile_filename.split("_E")[1].split("_00")[0])
            elif "_W" in tile_filename:
                tile_x = float(tile_filename.split("_W")[1].split("_00")[0]) * -1.0

            if tile_x >= int(region_bbox_coords["xmin"]) and \
                tile_x <= region_bbox_coords["xmax"] and \
                tile_y >= int(region_bbox_coords["ymin"]) and \
                tile_y <= region_bbox_coords["ymax"]:
                filtered_tile_list.append(tile_filename)

        return filtered_tile_list


    def import_dem_tiles(self):
        """Import digital elevation model (DEM) used for calculating slopes."""

        # Create directory for storing DEM related files
        dem_dir = os.path.join(settings.INPUT_DATA_DIR, "network_overture", "dem")
        delete_dir(dem_dir)
        make_dir(dem_dir)

        # Download list of DEM tile file names
        tile_list_path = os.path.join(dem_dir, "tileList.txt")
        download_link(
            link=f"{self.DEM_S3_URL}/tileList.txt",
            directory=dem_dir
        )

        # Get region bounding coords to filter DEM tiles
        region_bbox_coords = get_region_bbox_coords(
            geom_query=self.config.collection["geom_query"],
            db=self.db_remote
        )
        print_info(f"Calculated region bounding coordinates: {region_bbox_coords}.")

        # Filter tiles to region of interest
        full_tile_list = []
        full_tile_count = 0
        with open(tile_list_path, "r") as file:
            for tile_name in file:
                full_tile_list.append(tile_name.strip())
                full_tile_count += 1
        filtered_tile_list = self.get_filtered_dem_tiles(region_bbox_coords, full_tile_list)

        # Download filtered DEM tiles
        for index in range(len(filtered_tile_list)):
            tile_name = filtered_tile_list[index]
            dem_file_path = f"{self.DEM_S3_URL}/{tile_name}/{tile_name}.tif"
            download_link(
                link=dem_file_path,
                directory=dem_dir
            )
            try:
                subprocess.run(
                    f"raster2pgsql -s 4326 -M -a {os.path.join(dem_dir, tile_name)}.tif -F -t auto {self.output_schema}.{self.output_table_dem} | PGPASSWORD='{settings.POSTGRES_PASSWORD}' psql -h {settings.POSTGRES_HOST} -U {settings.POSTGRES_USER} -d {settings.POSTGRES_DB}",
                    stdout = subprocess.DEVNULL,
                    shell=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print_error(e)
                raise e

            print_info(f"Imported DEM tile: {index + 1} of {len(filtered_tile_list)}.")

        print_info(f"Total DEM tiles: {full_tile_count}, filtered DEM tiles: {len(filtered_tile_list)}.")


    def run(self):
        """Run Overture network preparation."""

        self.initialize_dem_table()
        self.import_dem_tiles()


def prepare_overture_network(region: str):
    print_info(f"Prepare Overture network data for region: {region}.")
    db_local = Database(settings.LOCAL_DATABASE_URI)
    db_remote = Database(settings.RAW_DATABASE_URI)

    try:
        OvertureNetworkPreparation(
            db_local=db_local,
            db_remote=db_remote,
            region=region
        ).run()
        db_local.close()
        db_remote.close()
        print_info("Finished Overture network preparation.")
    except Exception as e:
        print_error(e)
        raise e
    finally:
        db_local.close()
        db_remote.close()


# Run as main
if __name__ == "__main__":
    prepare_overture_network("de")
