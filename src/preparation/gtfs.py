from src.db.db import Database
from src.config.config import Config
from src.utils.utils import print_info, create_table_dump
from src.core.config import settings


class GTFS:
    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region
        self.config = Config("gtfs", region)
        self.small_bulk = 500
        self.large_bulk = 10000
        self.schema = self.config.preparation["target_schema"]

    def prepare_stops(self):
        """Prepare stops table."""

        sql_create_stops_optimized = f"""
        DROP TABLE IF EXISTS {self.schema}.stops_optimized;
        CREATE TABLE {self.schema}.stops_optimized (
            stop_id text NOT NULL,
            stop_code text NULL,
            stop_name text NULL,
            stop_desc text NULL,
            geom public.geometry(point, 4326) NULL,
            zone_id text NULL,
            stop_url text NULL,
            location_type text NULL,
            parent_station text NULL,
            stop_timezone text NULL,
            wheelchair_boarding text NULL,
            level_id text NULL,
            platform_code text NULL,
            h3_3 int4 NULL
        );
        """
        self.db.perform(sql_create_stops_optimized)

        # Add h3 index to stops
        sql_create_stops_h3 = f"""
            INSERT INTO {self.schema}.stops_optimized(stop_id, stop_code, stop_name, stop_desc, geom, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code, h3_3)
            SELECT stop_id, stop_code, stop_name, stop_desc, stop_loc, zone_id, stop_url,
            location_type::text, parent_station, stop_timezone, wheelchair_boarding::text, level_id, platform_code,
            public.to_short_h3_3(h3_lat_lng_to_cell(stop_loc::point, 3)::bigint) AS h3_3
            FROM {self.schema}.stops
        """
        self.db.perform(sql_create_stops_h3)

        # Create indices for stops
        sql_create_indices_stops = f"""
            CREATE INDEX ON {self.schema}.stops_optimized USING btree (parent_station);
            CREATE INDEX ON {self.schema}.stops_optimized  USING gist (geom);
            ALTER TABLE {self.schema}.stops_optimized ADD PRIMARY KEY (stop_id);
        """
        self.db.perform(sql_create_indices_stops)

    def prepare_trips(self):
        """Prepare trips table."""

        # Create result table
        sql_create_trip_optimized = f"""
        DROP TABLE IF EXISTS {self.schema}.trips_optimized;
        CREATE TABLE {self.schema}.trips_optimized (
            trip_id text NOT NULL,
            route_id text NOT NULL,
            service_id text NOT NULL,
            trip_headsign text NULL,
            trip_short_name text NULL,
            direction_id int4 NULL,
            block_id text NULL,
            shape_id text NULL,
            wheelchair_accessible text NULL,
            bikes_allowed text NULL,
            loop_id serial4 NOT NULL,
            length_m int4 NULL
        );
        """
        self.db.perform(sql_create_trip_optimized)

        sql_create_trips_columns = f"""
            ALTER TABLE {self.schema}.trips DROP COLUMN IF EXISTS loop_id;
            ALTER TABLE {self.schema}.trips ADD COLUMN loop_id serial;
            CREATE INDEX ON {self.schema}.trips (loop_id);
        """
        self.db.perform(sql_create_trips_columns)

        # Get max loop_id from routes
        sql_get_max_loop_id = f"""SELECT MAX(loop_id) FROM {self.schema}.trips;"""
        max_loop_id = self.db.select(sql_get_max_loop_id)[0][0]

        # Add length of trip to trips
        for i in range(0, max_loop_id, self.large_bulk):
            sql_create_length_m = f"""
                WITH to_insert AS
                (
                    SELECT x.*, j.*
                    FROM (
                        SELECT *
                        FROM {self.schema}.trips
                        WHERE loop_id > {i} AND loop_id <= {i+self.large_bulk}
                    )x
                    CROSS JOIN LATERAL
                    (
                        SELECT max(shape_dist_traveled) AS length_m
                        FROM {self.schema}.shapes
                        WHERE shape_id = x.shape_id
                    ) j
                )
                INSERT INTO {self.schema}.trips_optimized(trip_id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, wheelchair_accessible, bikes_allowed, loop_id, length_m)
                SELECT trip_id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id,
                wheelchair_accessible, bikes_allowed, loop_id, length_m
                FROM to_insert;
            """
            self.db.perform(sql_create_length_m)
            print_info(
                f"Finished processing trips {i} to {i+self.large_bulk} out of {max_loop_id}."
            )

    def prepare_stop_times(self):
        """Prepare stop_times table."""

        # Create result table
        sql_create_stop_times_optimized = f"""
            DROP TABLE IF EXISTS {self.schema}.stop_times_optimized;
            CREATE TABLE {self.schema}.stop_times_optimized (
                id serial4 NOT NULL,
                trip_id text NULL,
                arrival_time interval NULL,
                stop_id text NULL,
                route_type smallint NULL,
                weekdays _bool NULL,
                h3_3 integer NOT NULL
            );
        """
        self.db.perform(sql_create_stop_times_optimized)

        # Create helper columns in routes for loop
        sql_create_routes_helper = (
            f"""ALTER TABLE {self.schema}.routes ADD COLUMN IF NOT EXISTS loop_id serial;"""
        )
        self.db.perform(sql_create_routes_helper)

        # Get max loop_id from routes
        sql_get_max_loop_id = f"""SELECT MAX(loop_id) FROM {self.schema}.routes;"""
        max_loop_id = self.db.select(sql_get_max_loop_id)[0][0]

        # Run processing in batches of routes to avoid memory issues
        for i in range(0, max_loop_id, self.small_bulk):
            sql_create_trips_weekday = f"""DROP TABLE IF EXISTS {self.schema}.temp_trips_weekday;
            CREATE TABLE {self.schema}.temp_trips_weekday AS
            SELECT t.trip_id, t.route_type::text::smallint, ARRAY[
            (('{'{'}"available": "true", "not_available": "false"{'}'}'::jsonb) ->> c.monday::text)::boolean,
            (('{'{'}"available": "true", "not_available": "false"{'}'}'::jsonb) ->> c.tuesday::text)::boolean,
            (('{'{'}"available": "true", "not_available": "false"{'}'}'::jsonb) ->> c.wednesday::text)::boolean,
            (('{'{'}"available": "true", "not_available": "false"{'}'}'::jsonb) ->> c.thursday::text)::boolean,
            (('{'{'}"available": "true", "not_available": "false"{'}'}'::jsonb) ->> c.friday::text)::boolean,
            (('{'{'}"available": "true", "not_available": "false"{'}'}'::jsonb) ->> c.saturday::text)::boolean,
            (('{'{'}"available": "true", "not_available": "false"{'}'}'::jsonb) ->> c.sunday::text)::boolean
            ] AS weekdays
            FROM
            (
                SELECT t.trip_id, t.service_id, r.route_type
                FROM {self.schema}.trips t, {self.schema}.routes r
                WHERE t.route_id = r.route_id
                AND r.loop_id > {i} AND r.loop_id <= {i+self.small_bulk}
            ) t, {self.schema}.calendar c
            WHERE t.service_id = c.service_id
            AND '{self.config.preparation["start_date"]}' >= start_date
            AND '{self.config.preparation["end_date"]}' <= end_date;
            ALTER TABLE {self.schema}.temp_trips_weekday ADD COLUMN id serial;
            ALTER TABLE {self.schema}.temp_trips_weekday ADD PRIMARY KEY (id);
            CREATE INDEX ON {self.schema}.temp_trips_weekday (trip_id);"""
            self.db.perform(sql_create_trips_weekday)

            # Creates table with optimized structure for counting services on the station level
            sql_create_stop_times_optimized = f"""
            DROP TABLE IF EXISTS {self.schema}.temp_stop_times_optimized;
            CREATE TABLE {self.schema}.temp_stop_times_optimized AS
            SELECT st.trip_id, st.arrival_time, stop_id, route_type::text::smallint, weekdays
            FROM {self.schema}.stop_times st, {self.schema}.temp_trips_weekday w
            WHERE st.trip_id = w.trip_id;
            CREATE INDEX ON {self.schema}.temp_stop_times_optimized (stop_id);
            """
            self.db.perform(sql_create_stop_times_optimized)

            # Insert data into the results table
            sql_insert_stop_times_optimized = f"""
            INSERT INTO {self.schema}.stop_times_optimized(trip_id, arrival_time, stop_id, route_type, weekdays, h3_3)
            SELECT t.trip_id, t.arrival_time, t.stop_id, t.route_type, t.weekdays, s.h3_3
            FROM {self.schema}.temp_stop_times_optimized t, {self.schema}.stops_optimized s
            WHERE t.stop_id = s.stop_id;
            """
            self.db.perform(sql_insert_stop_times_optimized)

            print_info(
                f"Finished processing routes {i} to {i+self.small_bulk} out of {max_loop_id}."
            )

    # TODO: Citus needs to be finished and tested further, currently not used.
    def make_citus(self):
        """Make tables distributed."""

        # Drop primary key from stops
        sql_drop_primary_key = f"""
            ALTER TABLE {self.schema}.stops_optimized DROP CONSTRAINT stops_optimized_pkey;
        """
        self.db.perform(sql_drop_primary_key)

        # Make tables distributed
        sql_make_citus = """
            SELECT create_distributed_table('gtfs.stops_optimized', 'h3_3');
            SELECT create_distributed_table('gtfs.stop_times_optimized', 'h3_3');
        """
        self.db.perform(sql_make_citus)

        # Create indices for stops
        sql_create_indices_stops = f"""
            ALTER TABLE {self.schema}.stops_optimized ADD PRIMARY KEY (h3_3, stop_id);
            ALTER TABLE {self.schema}.stops_optimized ADD FOREIGN KEY (h3_3, parent_station)
            REFERENCES {self.schema}.stops_optimized(h3_3, stop_id);
        """
        self.db.perform(sql_create_indices_stops)

        # Create indices for stop_times
        sql_create_indices_stop_times = f"""
            ALTER TABLE {self.schema}.stop_times_optimized ADD PRIMARY KEY (h3_3, id);
            CREATE INDEX ON {self.schema}.stop_times_optimized (h3_3, stop_id, arrival_time);
        """
        self.db.perform(sql_create_indices_stop_times)

    def add_indices(self):
        sql_create_indices_stops = f"""
            ALTER TABLE {self.schema}.stops_optimized ADD FOREIGN KEY (parent_station)
            REFERENCES {self.schema}.stops_optimized(stop_id);
        """
        self.db.perform(sql_create_indices_stops)

        sql_create_indices_stop_times = f"""
            ALTER TABLE {self.schema}.stop_times_optimized ADD PRIMARY KEY (id);
            CREATE INDEX ON {self.schema}.stop_times_optimized (stop_id, arrival_time);
            CREATE INDEX ON {self.schema}.stop_times_optimized (trip_id);
            CREATE INDEX ON {self.schema}.trips_optimized (trip_id);
        """
        self.db.perform(sql_create_indices_stop_times)

    def run(self):
        """Run the gtfs preparation."""

        self.prepare_stops()
        self.prepare_trips()
        self.prepare_stop_times()
        self.add_indices()


def prepare_gtfs(region: str):
    print_info(f"Prepare GTFS data for the region {region}.")
    #db = Database(settings.LOCAL_DATABASE_URI)
    db_rd = Database(settings.RAW_DATABASE_URI)

    try:
        GTFS(db=db_rd, region=region).run()
        db_rd.close()
        print_info("Finished GTFS preparation.")
    except Exception as e:
        print(e)
        raise e
    finally:
        db_rd.close()


def export_gtfs():
    db = Database(settings.RAW_DATABASE_URI)
    create_table_dump(db.db_config, "gtfs", "stops_optimized")
    create_table_dump(db.db_config, "gtfs", "stop_times_optimized")
    create_table_dump(db.db_config, "gtfs", "trips")
