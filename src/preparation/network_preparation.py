import json
import sys
import os
import subprocess
import psycopg2
import time
from src.config.config import Config
from src.db.config import DATABASE
from src.db.db import Database
from src.other.utils import print_info, print_warning, print_hashtags, create_pgpass_for_db, create_table_dump, download_link, create_table_schema
from multiprocessing.pool import Pool
from collection.osm_collection import OsmCollection
from preparation.network_islands import NetworkIslands


class NetworkPreparation:
    """Class to prepare the routing network. It processs the network in chunks and prepares the different attributes (e.g., slopes)."""

    def __init__(self, db):
        self.db = db
        self.root_dir = '/app'
        self.dbname, self.host, self.username, self.port = (
            DATABASE["dbname"],
            DATABASE["host"],
            DATABASE["user"],
            DATABASE["port"],
        )
        self.DATABASE = DATABASE
        self.config_ways_preparation = Config("ways").preparation
        self.available_cpus = os.cpu_count()

    def create_processing_units(self):
        sql_create_table = """
            DROP TABLE IF EXISTS processing_units; 
            CREATE TABLE processing_units (id serial, geom GEOMETRY(MULTIPOLYGON, 4326));
        """
        sql_fill_table = """
            INSERT INTO processing_units(geom)
            WITH boundaries AS 
            (
                SELECT ST_MakePolygon(geom) As geom, (ST_AREA(ST_MakePolygon(geom)::geography) / 100000000)::integer AS area_square_km 
                FROM 
                (
                SELECT ST_ExteriorRing((ST_Dump(st_union(geom))).geom) As geom
                FROM osm_extract_boundaries 
                ) s
            )
            SELECT ST_MULTI(create_equal_area_split_polygon(b.geom, 1))
            FROM boundaries b;
        """

        sql_create_index = """
            ALTER TABLE processing_units ADD PRIMARY KEY(id);
            CREATE INDEX ON processing_units USING GIST(geom);
        """

        sql_add_ways_status_column = """
            ALTER TABLE ways DROP COLUMN IF EXISTS preparation_status;
            ALTER TABLE ways ADD COLUMN preparation_status char(1);
        """
        self.db.perform(sql_create_table)
        self.db.perform(sql_fill_table)
        self.db.perform(sql_create_index)
        self.db.perform(sql_add_ways_status_column)
    
    def create_edge_indizes(self):
        sql_create_index = """
        ALTER TABLE basic.edge ADD PRIMARY KEY(id);
        CREATE INDEX idx_edge_geom ON basic.edge USING gist (geom);
        CREATE INDEX ix_basic_edge_bicycle ON basic.edge USING btree (bicycle);
        CREATE INDEX ix_basic_edge_edge_id ON basic.edge USING btree (edge_id);
        CREATE INDEX ix_basic_edge_foot ON basic.edge USING btree (foot);
        CREATE INDEX ix_basic_edge_source ON basic.edge USING btree (source);
        CREATE INDEX ix_basic_edge_target ON basic.edge USING btree (target);
        """
        self.db.perform(sql_create_index)

    def create_street_crossings(self):
        sql_street_crossings = """
            --Create table that stores all street crossings
            DROP TABLE IF EXISTS extra.street_crossings;
            CREATE TABLE extra.street_crossings AS 
            SELECT osm_id, NULL as key, highway,
            CASE WHEN (tags -> 'crossing_ref') IS NOT NULL THEN (tags -> 'crossing_ref') ELSE (tags -> 'crossing') END AS crossing, 
            (tags -> 'traffic_signals') AS traffic_signals, (tags -> 'kerb') AS kerb, 
            (tags -> 'segregated') AS segregated, (tags -> 'supervised') AS supervised, 
            (tags -> 'tactile_paving') AS tactile_paving, (tags -> 'wheelchair') AS wheelchair, way AS geom, 'osm' as source
            FROM planet_osm_point p
            WHERE (tags -> 'crossing') IS NOT NULL 
            OR highway IN('crossing','traffic_signals')
            OR (tags -> 'traffic_signals') = 'pedestrian_crossing';

            ALTER TABLE extra.street_crossings ADD COLUMN id serial;
            ALTER TABLE extra.street_crossings ADD PRIMARY key(id);

            UPDATE extra.street_crossings 
            SET crossing = 'traffic_signals'
            WHERE traffic_signals = 'crossing' 
            OR traffic_signals = 'pedestrian_crossing';

            UPDATE extra.street_crossings 
            SET crossing = highway 
            WHERE crossing IS NULL AND highway IS NOT NULL; 

            CREATE INDEX ON extra.street_crossings USING GIST(geom);
        """
        self.db.perform(query=sql_street_crossings)

    def dump_network(self):
        """Dump the network tables individual files."""

        create_pgpass_for_db(self.DATABASE)
        create_table_dump(self.DATABASE, 'basic.edge')
        create_table_dump(self.DATABASE, 'extra.street_crossings')


# These functions are not in the class as there where difficulaties when running it in parallel
def prepare_ways_one_core(processing_unit_id):
    connection_string = " ".join(("{}={}".format(*i) for i in DATABASE.items()))
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    config_ways_preparation = Config("ways").preparation
    impedance_surface_object = json.dumps(config_ways_preparation["cycling_surface"])
    
    sql_select_ways_ids = f"""
        SELECT ARRAY_AGG(w.gid) AS ways_ids  
        FROM ways w, processing_units p 
        WHERE ST_Intersects(ST_CENTROID(w.the_geom), p.geom) 
        AND w.the_geom && p.geom
        AND p.id = {processing_unit_id}
        AND w.preparation_status IS NULL;
    """
    ways_ids = cur.execute(sql_select_ways_ids)
    ways_ids = cur.fetchall()
    ways_ids = ways_ids[0][0]

    # Check if there are way_ids
    if ways_ids is not None:
        cnt = 0
        for way_id in ways_ids:
            cnt += 1
            sql_perform_preparation = f"""
            SELECT classify_way(
                {way_id}, 
                ARRAY{config_ways_preparation["excluded_class_id_walking"]}, 
                ARRAY{config_ways_preparation["excluded_class_id_cycling"]},
                ARRAY{config_ways_preparation["categories_no_foot"]},
                ARRAY{config_ways_preparation["categories_no_bicycle"]},
                '{impedance_surface_object}'::jsonb
            );"""

            try:
                cur.execute(sql_perform_preparation)	
                # Log success
                cur.execute(
                    f"""
                    UPDATE ways SET
                    preparation_status = 'p'
                    WHERE gid = {way_id};
                    """
                )		
                conn.commit()

            except:
                conn.rollback()
                print(f"Error in processing way {way_id}")
                # Log error
                cur.execute(
                    f"""
                UPDATE ways SET 
                preparation_status = 'e'
                WHERE gid = {way_id};
                """
                )
                conn.commit()
                continue
    
    conn.close()
    print_hashtags()
    print("Finished processing unit: ", processing_unit_id)
    print_hashtags()


def prepare_ways(db):
    sql_delete_network = """
        TRUNCATE TABLE basic.edge;
    """
    db.perform(sql_delete_network)
    
    sql_read_processing_units = f"""
        SELECT id
        FROM processing_units;		
    """
    processing_units = db.select(sql_read_processing_units)
    processing_units = [u[0] for u in processing_units]
    print_hashtags()
    print_info(f"Start processing ways.")
    print_hashtags()
    start_time = time.time()

    # Execute in parallel 100 processing units at a time
    for i in range(0, len(processing_units), 100):
        processing_unit_ids = processing_units[i:i+100]
        pool = Pool(processes=os.cpu_count())
        pool.map(prepare_ways_one_core, (processing_unit_ids))
        pool.close()
        pool.join()
    print_hashtags()
    print(f"Calculation took {time.time() - start_time} seconds ---")
    print_hashtags()


def perform_network_preparation(db):
    osm_collection = OsmCollection(DATABASE)

    # Import needed data into the database
    osm_collection.network_collection(db)
    osm_collection.create_osm_extract_boundaries(db)
    osm_collection.import_dem()
    
    #Prepare network
    Config("ways").download_db_schema()
    preparation = NetworkPreparation(db)
    preparation.create_table_schema(db, DATABASE, 'basic.edge')
    preparation.create_table_schema(db, DATABASE, 'basic.node')
    preparation.create_processing_units()
    prepare_ways(db)
    preparation.create_edge_indizes()
    NetworkIslands().find_network_islands()
    preparation.create_street_crossings()
    preparation.dump_network()
    db.conn.close()


db = Database()
perform_network_preparation(db)