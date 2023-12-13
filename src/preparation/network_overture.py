import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

import psycopg2

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.preparation.network_overture_parallelism import (
    ComputeImpedance,
    ProcessSegments,
)
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

        self.DEM_S3_URL = "https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com"
        self.NUM_THREADS = 16


    def initialize_dem_table(self):
        """Initialize digital elevation model (DEM) raster table."""

        sql_create_dem_table = """
            DROP TABLE IF EXISTS public.dem;
            CREATE TABLE public.dem (
                rid serial4 NOT NULL,
                rast public.raster NULL,
                filename text NULL,
                CONSTRAINT dem_pkey PRIMARY KEY (rid),
                CONSTRAINT enforce_nodata_values_rast CHECK ((_raster_constraint_nodata_values(rast) = '{{NULL}}'::numeric[])),
                CONSTRAINT enforce_num_bands_rast CHECK ((st_numbands(rast) = 1)),
                CONSTRAINT enforce_out_db_rast CHECK ((_raster_constraint_out_db(rast) = '{{f}}'::boolean[])),
                CONSTRAINT enforce_pixel_types_rast CHECK ((_raster_constraint_pixel_types(rast) = '{{32BF}}'::text[])),
                CONSTRAINT enforce_srid_rast CHECK ((st_srid(rast) = 4326))
            );
            CREATE INDEX dem_st_convexhull_idx ON public.dem USING gist (st_convexhull(rast));
        """
        self.db_local.perform(sql_create_dem_table)


    def get_filtered_dem_tiles(self, region_bbox_coords: dict, full_tile_list: list):
        """Filter digital elevation model (DEM) tiles to our region of interest."""

        filtered_tile_list= []
        for tile_filename in full_tile_list:
            tile_x = 0.0
            tile_y = 0.0

            # Extract tile latitude from filename
            if "_N" in tile_filename:
                tile_y = float(tile_filename.split("_N")[1].split("_00")[0])
            elif "_S" in tile_filename:
                tile_y = float(tile_filename.split("_S")[1].split("_00")[0]) * -1.0

            # Extract tile longitude from filename
            if "_E" in tile_filename:
                tile_x = float(tile_filename.split("_E")[1].split("_00")[0])
            elif "_W" in tile_filename:
                tile_x = float(tile_filename.split("_W")[1].split("_00")[0]) * -1.0

            # Filter tile by comparing coordinates to our region's bounding box
            if tile_x >= int(region_bbox_coords["xmin"]) and \
                tile_x <= region_bbox_coords["xmax"] and \
                tile_y >= int(region_bbox_coords["ymin"]) and \
                tile_y <= region_bbox_coords["ymax"]:
                filtered_tile_list.append(tile_filename)

        return filtered_tile_list


    def import_dem_tiles(self):
        """Import digital elevation model (DEM) used for calculating slopes."""

        print_info("Importing DEM tiles.")

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
                    f"raster2pgsql -s 4326 -M -a {os.path.join(dem_dir, tile_name)}.tif -F -t auto public.dem | PGPASSWORD='{settings.POSTGRES_PASSWORD}' psql -h {settings.POSTGRES_HOST} -U {settings.POSTGRES_USER} -d {settings.POSTGRES_DB}",
                    stdout = subprocess.DEVNULL,
                    shell=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print_error(e)
                raise e

            print_info(f"Imported DEM tile: {index + 1} of {len(filtered_tile_list)}.")

        print_info(f"Total DEM tiles: {full_tile_count}, filtered DEM tiles: {len(filtered_tile_list)}.")


    def initialize_connectors_table(self):
        """Create table for storing final processed connectors data."""

        sql_create_connectors_table = """
            DROP TABLE IF EXISTS basic.connector CASCADE;
            CREATE TABLE basic.connector (
                index serial NOT NULL,
                id text NOT NULL UNIQUE,
                osm_id int8 NULL,
                geom public.geometry(point, 4326) NOT NULL,
                h3_3 int2 NOT NULL,
                h3_5 integer NOT NULL,
                CONSTRAINT connector_pkey PRIMARY KEY (index, h3_3)
            );
            CREATE INDEX idx_connector_id on basic.connector (id);
            CREATE INDEX idx_connector_geom ON basic.connector USING gist (geom);
        """
        self.db_local.perform(sql_create_connectors_table)


    def initialize_segments_table(self):
        """Create table for storing final processed segments data."""

        sql_create_segments_table = """
            DROP TABLE IF EXISTS basic.segment;
            CREATE TABLE basic.segment (
                id serial NOT NULL,
                length_m float8 NOT NULL,
                length_3857 float8 NOT NULL,
                osm_id int8 NULL,
                bicycle text NULL,
                foot text NULL,
                class_ text NOT NULL,
                impedance_slope float8 NULL,
                impedance_slope_reverse float8 NULL,
                impedance_surface float8 NULL,
                coordinates_3857 json NOT NULL,
                maxspeed_forward int4 NULL,
                maxspeed_backward int4 NULL,
                "source" integer NOT NULL,
                target integer NOT NULL,
                tags jsonb NULL,
                geom public.geometry(linestring, 4326) NOT NULL,
                h3_3 int2 NOT NULL,
                h3_5 int4 NOT NULL,
                CONSTRAINT segment_pkey PRIMARY KEY (id, h3_3),
                CONSTRAINT segment_source_fkey FOREIGN KEY ("source") REFERENCES basic.connector(index),
                CONSTRAINT segment_target_fkey FOREIGN KEY (target) REFERENCES basic.connector(index)
            );
            CREATE INDEX idx_segment_geom ON basic.segment USING gist (geom);
            CREATE INDEX ix_basic_segment_source ON basic.segment USING btree (source);
            CREATE INDEX ix_basic_segment_target ON basic.segment USING btree (target);
        """
        self.db_local.perform(sql_create_segments_table)


    def compute_region_h3_grid(self):
        """Use the h3 grid function to create a h3 grid for our region."""

        sql_get_region_geometry = f"""
            SELECT ST_AsText(geom) AS geom
            FROM ({self.config.collection["geom_query"]}) sub
        """
        region_geom = self.db_remote.select(sql_get_region_geometry)[0][0]

        sql_create_region_h3_3_grid = f"""
            DROP TABLE IF EXISTS basic.h3_3_grid;
            CREATE TABLE basic.h3_3_grid AS
                SELECT * FROM
                public.fill_polygon_h3(ST_GeomFromText('{region_geom}', 4326), 3);
            ALTER TABLE basic.h3_3_grid ADD CONSTRAINT h3_3_grid_pkey PRIMARY KEY (h3_index);
            CREATE INDEX ON basic.h3_3_grid USING GIST (h3_boundary);
            CREATE INDEX ON basic.h3_3_grid USING GIST (h3_geom);
        """
        self.db_local.perform(sql_create_region_h3_3_grid)

        sql_create_region_h3_5_grid = f"""
            DROP TABLE IF EXISTS basic.h3_5_grid;
            CREATE TABLE basic.h3_5_grid AS
                SELECT * FROM
                public.fill_polygon_h3(ST_GeomFromText('{region_geom}', 4326), 5);
            ALTER TABLE basic.h3_5_grid ADD CONSTRAINT h3_5_grid_pkey PRIMARY KEY (h3_index);
            CREATE INDEX ON basic.h3_5_grid USING GIST (h3_boundary);
            CREATE INDEX ON basic.h3_5_grid USING GIST (h3_geom);
        """
        self.db_local.perform(sql_create_region_h3_5_grid)

        sql_compute_h3_short_index = """
            ALTER TABLE basic.h3_3_grid ADD COLUMN h3_short int2;
            UPDATE basic.h3_3_grid
            SET h3_short = to_short_h3_3(h3_index::bigint);

            ALTER TABLE basic.h3_5_grid ADD COLUMN h3_short int4;
            UPDATE basic.h3_5_grid
            SET h3_short = to_short_h3_5(h3_index::bigint);
        """
        self.db_local.perform(sql_compute_h3_short_index)

        print_info(f"Computed H3 grid for region: {self.region}.")


    def initiate_segment_processing(self):
        """Utilize multithreading to process segments in parallel."""

        # TODO Remove this
        with open("src/db/functions/classify_segment.sql", "r") as f:
            self.db_local.perform(f.read())
        with open("src/db/functions/clip_segments.sql", "r") as f:
            self.db_local.perform(f.read())

        # Load user-configured impedance coefficients for various surface types
        cycling_surfaces = json.dumps(self.config.preparation["cycling_surfaces"])

        # Create separate DB connections for each thread
        db_connections = []
        for _ in range(self.NUM_THREADS):
            connection_string = f"dbname={settings.POSTGRES_DB} user={settings.POSTGRES_USER} \
                                 password={settings.POSTGRES_PASSWORD} host={settings.POSTGRES_HOST} \
                                 port={settings.POSTGRES_PORT}"
            conn = psycopg2.connect(connection_string)
            db_connections.append(conn)

        print_info(f"Starting {self.NUM_THREADS} threads for processing segments.")
        start_time = time.time()

        # Start threads
        try:
            with ThreadPoolExecutor(max_workers=self.NUM_THREADS) as executor:
                # Process segments & connectors
                h3_3_queue = self.get_h3_3_index_queue()
                futures = [
                    executor.submit(
                        ProcessSegments(
                            thread_id=thread_id,
                            db_connection=db_connections[thread_id],
                            get_next_h3_index=lambda: h3_3_queue.get() if not h3_3_queue.empty() else None,
                            cycling_surfaces=cycling_surfaces,
                        ).run
                    )
                    for thread_id in range(self.NUM_THREADS)
                ]
                [future.result() for future in as_completed(futures)]

                # Compute segment impedance values
                h3_5_queue = self.get_h3_5_index_queue()
                futures = [
                    executor.submit(
                        ComputeImpedance(
                            thread_id=thread_id,
                            db_connection=db_connections[thread_id],
                            get_next_h3_index=lambda: h3_5_queue.get() if not h3_5_queue.empty() else None,
                        ).run
                    )
                    for thread_id in range(self.NUM_THREADS)
                ]
                [future.result() for future in as_completed(futures)]
        except Exception as e:
            print_error(e)
            raise e
        finally:
            # Clean up DB connections
            [conn.close() for conn in db_connections]

        print_info(f"Finished processing segments in {round((time.time() - start_time) / 60)} minutes.")


    def get_h3_3_index_queue(self):
        """Get queue of H3 indexes to be processed by threads."""

        sql_get_h3_indexes = """
            SELECT h3_index
            FROM basic.h3_3_grid
            ORDER BY h3_index;
        """
        h3_indexes = self.db_local.select(sql_get_h3_indexes)
        h3_index_queue = Queue()
        for h3_index in h3_indexes:
            h3_index_queue.put(h3_index[0])
        return h3_index_queue


    def get_h3_5_index_queue(self):
        """Get queue of H3 indexes to be processed by threads."""

        sql_get_h3_indexes = """
            SELECT h3_short
            FROM basic.h3_5_grid
            ORDER BY h3_index;
        """
        h3_indexes = self.db_local.select(sql_get_h3_indexes)
        h3_index_queue = Queue()
        for h3_index in h3_indexes:
            h3_index_queue.put(h3_index[0])
        return h3_index_queue


    def clean_up(self):
        """Remove unused temp columns from the connector table."""

        sql_clean_up = """
            ALTER TABLE basic.connector DROP COLUMN id;
            ALTER TABLE basic.connector RENAME COLUMN index TO id;
        """
        self.db_local.perform(sql_clean_up)


    def run(self):
        """Run Overture network preparation."""

        self.initialize_dem_table()
        self.import_dem_tiles()

        self.initialize_connectors_table()
        self.initialize_segments_table()

        self.compute_region_h3_grid()

        self.initiate_segment_processing()

        self.clean_up()


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
