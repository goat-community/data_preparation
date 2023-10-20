import csv
import os

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import create_table_dump, print_info, timing


class GTFS:
    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region
        self.config = Config("gtfs", region)
        self.small_bulk = 100
        self.large_bulk = 10000
        self.schema = self.config.preparation["target_schema"]


    @timing
    def implement_data_corrections(self):
        """Implement corrections as defined in CSV format correction files listed in config"""

        # Return if no correction files are listed in config
        if "network_corrections" not in self.config.preparation:
            print_info("No corrections to be made.")
            return

        # Get list of tables in GTFS data schema
        sql_get_tables = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{self.schema}';
        """
        schema_tables = self.db.select(sql_get_tables)
        schema_tables = [item for tuple in schema_tables for item in tuple]

        # Implement corrections by table
        corr_tables = self.config.preparation["network_corrections"]
        for table_name in corr_tables.keys():
            if table_name not in schema_tables:
                print_info(f"Table {table_name} doesn't exist, skipping.")
                continue

            corr_filename = os.path.join(
                settings.INPUT_DATA_DIR, "gtfs",
                self.config.preparation["network_dir"],
                corr_tables[table_name]
            )
            with open(corr_filename) as csv_file:
                reader = csv.DictReader(csv_file)
                header = reader.fieldnames
                for line in reader:
                    corrections = ""
                    for column in header[1:]:
                        # Correction not required if value is empty
                        if not line[column]:
                            continue
                        corrections += f"{column} = '{line[column]}', "
                    corrections = corrections.rstrip(", ")
                    # Update relevant row in table assuming first column in header is the primary key
                    sql_perform_correction = f"""
                        UPDATE {self.schema}.{table_name}
                        SET {corrections}
                        WHERE {header[0]} = '{line[header[0]]}';
                    """
                    self.db.perform(sql_perform_correction)


    @timing
    def prepare_shape_dist_region(self):
        """Prepare distance travelled of the gtfs shapes per specified region."""

        # Create result table
        sql_create_shape_dist_region = f"""
        DROP TABLE IF EXISTS {self.schema}.shape_dist_region;
        CREATE TABLE {self.schema}.shape_dist_region
        (
            region_id TEXT,
            shape_id TEXT,
            shape_dist_traveled float,
            h3_3 integer
        );
        SELECT create_distributed_table('{self.schema}.shape_dist_region', 'h3_3');
        """
        self.db.perform(sql_create_shape_dist_region)

        # Get region ids and names
        regions = self.db.select(self.config.preparation["regions"])
        cnt_regions = len(regions)
        cnt = 0

        # Loop through regions and calculate shape_dist_traveled
        for region in regions:
            cnt += 1
            region[0]
            name = region[1]

            # Create temporary table with region
            sql_create_table_region = f"""DROP TABLE IF EXISTS region_subdivided;
            CREATE TABLE region_subdivided
            (
                id TEXT,
                name TEXT,
                geom geometry,
                h3_3 integer
            );
            SELECT create_distributed_table('public.region_subdivided', 'h3_3');

            INSERT INTO region_subdivided
            WITH region AS
            (
                {self.config.preparation["regions"]}
                WHERE name = '{name}'
            )
            ,border_points AS
            (
                SELECT ((ST_DUMPPOINTS(geom)).geom)::point AS geom
                FROM region
            ),
            h3_ids AS
            (
                SELECT DISTINCT to_short_h3_3(h3_lat_lng_to_cell(geom, 3)::bigint) AS h3_3,
                ST_SETSRID(h3_cell_to_boundary(h3_lat_lng_to_cell(geom, 3))::geometry, 4326) AS geom
                FROM border_points
            )
            SELECT r.id, r.name, ST_SUBDIVIDE(ST_Intersection(h.geom, r.geom), 20) AS geom, h.h3_3
            FROM h3_ids h, region r;
            CREATE INDEX ON region_subdivided USING GIST(h3_3, geom);
            """
            self.db.perform(sql_create_table_region)

            sql_get_shape_dist_traveled = f"""INSERT INTO {self.schema}.shape_dist_region(region_id, shape_id, shape_dist_traveled, h3_3)
            SELECT n.id, s.shape_id, max(shape_dist_traveled) - min(shape_dist_traveled) AS shape_dist_traveled, n.h3_3
            FROM {self.schema}.shapes s, public.region_subdivided n
            WHERE ST_Intersects(s.geom, n.geom)
            AND s.h3_3 = n.h3_3
            GROUP BY n.h3_3, n.id, shape_id;
            DROP TABLE IF EXISTS region_subdivided; """
            self.db.perform(sql_get_shape_dist_traveled)

            print_info(f"Finished processing region {name}. There are {cnt} out of {cnt_regions} regions processed.")

        # Create index
        self.db.perform(f"CREATE INDEX ON {self.schema}.shape_dist_region (h3_3, shape_id);")
        self.db.perform(f"CREATE INDEX ON {self.schema}.shape_dist_region (h3_3, region_id);")

    @timing
    def prepare_stop_times(self):
        """Prepare stop_times table."""

        # Create undistributed shape_dist_region table
        self.db.perform(
            f"""
            DROP TABLE IF EXISTS {self.schema}.undistributed_shape_dist_region;
            CREATE TABLE {self.schema}.undistributed_shape_dist_region AS
            SELECT *
            FROM {self.schema}.shape_dist_region;
            CREATE INDEX ON {self.schema}.undistributed_shape_dist_region (shape_id);
            """
        )

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
            SELECT create_distributed_table('{self.schema}.stop_times_optimized', 'h3_3');
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
            # This is currently optimized to fetch only the monday therefore the interval is just one day
            sql_get_date_with_max_trips = f"""
                DROP TABLE IF EXISTS {self.schema}.dates_max_trips;
                CREATE TABLE {self.schema}.dates_max_trips AS
                WITH date_series AS 
                (
                    SELECT 
                    TO_CHAR(DATE '{self.config.preparation["start_date"]}' + (7 * s.a), 'YYYY-MM-DD')::date AS start_date,
                    TO_CHAR(DATE '{self.config.preparation["start_date"]}' + (7 * s.a) + INTERVAL '1 day', 'YYYY-MM-DD')::date AS end_date
                    FROM generate_series(0, {self.config.preparation["num_weeks"]}) as s(a)
                ),
                trip_cnt AS 
                (
                    SELECT s.*, j.*
                    FROM date_series s 
                    CROSS JOIN LATERAL 
                    (
                        SELECT jj.*
                        FROM (SELECT route_id FROM {self.schema}.routes r WHERE r.loop_id > {i} AND r.loop_id <= {i+self.small_bulk}) r 
                        CROSS JOIN LATERAL 
                        (
                            SELECT
                                sum(c.monday::integer) cnt_trips_mon,
                                sum(c.saturday::integer) cnt_trips_sat,
                                sum(c.sunday::integer) cnt_trips_sun,
                                t.route_id
                            FROM {self.schema}.trips t, {self.schema}.calendar c
                            WHERE t.route_id = r.route_id 
                            AND t.service_id = c.service_id 
                            AND s.start_date >= start_date
                            AND s.end_date <= end_date
                            GROUP BY t.route_id
                        ) jj
                    ) j
                ),
                dates_max_trips AS
                (
                    SELECT
                        r.route_id,
                        j_mon.start_date_mon[1] AS start_date_mon,
                        j_mon.end_date_mon[1] AS end_date_mon,
                        j_sat.start_date_sat[1] AS start_date_sat,
                        j_sat.end_date_sat[1] AS end_date_sat,
                        j_sun.start_date_sun[1] AS start_date_sun,
                        j_sun.end_date_sun[1] AS end_date_sun
                    FROM (SELECT DISTINCT route_id FROM trip_cnt r) r
                    CROSS JOIN LATERAL 
                    (
                        SELECT cnt_trips_mon, max(cnt_trips_mon), ARRAY_AGG(start_date) AS start_date_mon, ARRAY_AGG(end_date) AS end_date_mon
                        FROM trip_cnt t 
                        WHERE r.route_id = t.route_id  
                        GROUP BY cnt_trips_mon
                        ORDER BY max(cnt_trips_mon)
                        DESC
                        LIMIT 1 
                    ) j_mon
                    CROSS JOIN LATERAL 
                    (
                        SELECT cnt_trips_sat, max(cnt_trips_sat), ARRAY_AGG(start_date) AS start_date_sat, ARRAY_AGG(end_date) AS end_date_sat
                        FROM trip_cnt t 
                        WHERE r.route_id = t.route_id  
                        GROUP BY cnt_trips_sat
                        ORDER BY max(cnt_trips_sat)
                        DESC
                        LIMIT 1 
                    ) j_sat
                    CROSS JOIN LATERAL 
                    (
                        SELECT cnt_trips_sun, max(cnt_trips_sun), ARRAY_AGG(start_date) AS start_date_sun, ARRAY_AGG(end_date) AS end_date_sun
                        FROM trip_cnt t 
                        WHERE r.route_id = t.route_id  
                        GROUP BY cnt_trips_sun
                        ORDER BY max(cnt_trips_sun)
                        DESC
                        LIMIT 1 
                    ) j_sun
                )
                SELECT
                    r.*,
                    d.start_date_mon, d.end_date_mon,
                    d.start_date_sat, d.end_date_sat,
                    d.start_date_sun, d.end_date_sun
                FROM dates_max_trips d, {self.schema}.routes r
                WHERE d.route_id = r.route_id;
            """ 
                
            self.db.perform(sql_get_date_with_max_trips)
            self.db.perform(f"CREATE INDEX ON {self.schema}.dates_max_trips (route_id);")
            
            # Select relevant trips with relevant route information and save them into a new table
            sql_create_trips_weekday = f"""DROP TABLE IF EXISTS {self.schema}.temp_trips_weekday;
            CREATE TABLE {self.schema}.temp_trips_weekday AS
            WITH t AS (
                SELECT t.trip_id, t.service_id, t.shape_id, t.trip_headsign, r.*
                FROM {self.schema}.trips t
                INNER JOIN {self.schema}.dates_max_trips r ON t.route_id = r.route_id
            )
            SELECT trip_id, route_id, route_type, trip_headsign, shape_id,
                ARRAY[CASE WHEN 'true' = ANY(array_agg(weekday)) THEN 'true'::boolean ELSE 'false'::boolean END,
                        CASE WHEN 'true' = ANY(array_agg(sat)) THEN 'true'::boolean ELSE 'false'::boolean END,
                        CASE WHEN 'true' = ANY(array_agg(sun)) THEN 'true'::boolean ELSE 'false'::boolean END] AS weekdays
            FROM (
                SELECT t.*,
                    'true' as weekday, 
                    'false' as sat,
                    'false' as sun
                FROM t
                INNER JOIN {self.schema}.calendar c ON
                    t.service_id = c.service_id
                    AND t.start_date_mon >= c.start_date
                    AND t.end_date_mon <= c.end_date
                    AND c.monday = '1'
                UNION
                SELECT t.*,
                    'false' as weekday, 
                    'true' as sat,
                    'false' as sun
                FROM t
                INNER JOIN {self.schema}.calendar c ON
                    t.service_id = c.service_id
                    AND t.start_date_sat >= c.start_date
                    AND t.end_date_sat <= c.end_date
                    AND c.saturday = '1'
                UNION
                SELECT t.*,
                    'false' as weekday,
                    'false' as sat,
                    'true' as sun
                FROM t
                INNER JOIN {self.schema}.calendar c ON
                    t.service_id = c.service_id
                    AND t.start_date_sun >= c.start_date
                    AND t.end_date_sun <= c.end_date
                    AND c.sunday = '1'
            ) trips_combined
            GROUP BY trip_id, route_id, route_type, trip_headsign, shape_id;
            ALTER TABLE {self.schema}.temp_trips_weekday ADD COLUMN id serial;
            ALTER TABLE {self.schema}.temp_trips_weekday ADD PRIMARY KEY (id);
            CREATE INDEX ON {self.schema}.temp_trips_weekday (trip_id);
            CREATE INDEX ON {self.schema}.temp_trips_weekday (shape_id);"""
            self.db.perform(sql_create_trips_weekday)
            
            
            # # Select relevant trips with relevant route information and save them into a new table
            # sql_create_trips_weekday = f"""DROP TABLE IF EXISTS {self.schema}.temp_trips_weekday;
            # CREATE TABLE {self.schema}.temp_trips_weekday AS
            # SELECT t.trip_id, t.route_type::text::smallint, t.shape_id, ARRAY[
            # (('{'{'}"1": "true", "0": "false"{'}'}'::jsonb) ->> c.monday::text)::boolean,
            # (('{'{'}"1": "true", "0": "false"{'}'}'::jsonb) ->> c.tuesday::text)::boolean,
            # (('{'{'}"1": "true", "0": "false"{'}'}'::jsonb) ->> c.wednesday::text)::boolean,
            # (('{'{'}"1": "true", "0": "false"{'}'}'::jsonb) ->> c.thursday::text)::boolean,
            # (('{'{'}"1": "true", "0": "false"{'}'}'::jsonb) ->> c.friday::text)::boolean,
            # (('{'{'}"1": "true", "0": "false"{'}'}'::jsonb) ->> c.saturday::text)::boolean,
            # (('{'{'}"1": "true", "0": "false"{'}'}'::jsonb) ->> c.sunday::text)::boolean
            # ] AS weekdays
            # FROM
            # (
            #     SELECT t.trip_id, t.service_id, r.route_type, t.shape_id
            #     FROM {self.schema}.trips t, {self.schema}.routes r
            #     WHERE t.route_id = r.route_id
            #     AND r.loop_id > {i} AND r.loop_id <= {i+self.small_bulk}
            # ) t, {self.schema}.calendar c
            # WHERE t.service_id = c.service_id
            # AND '{self.config.preparation["start_date"]}' >= start_date
            # AND '{self.config.preparation["end_date"]}' <= end_date;
            # ALTER TABLE {self.schema}.temp_trips_weekday ADD COLUMN id serial;
            # ALTER TABLE {self.schema}.temp_trips_weekday ADD PRIMARY KEY (id);
            # CREATE INDEX ON {self.schema}.temp_trips_weekday (trip_id);
            # CREATE INDEX ON {self.schema}.temp_trips_weekday (shape_id);"""
            # self.db.perform(sql_create_trips_weekday)

            # Create distributed table for temp_trips_weekday
            sql_create_temp_trips_weekday_distributed = f"""
                DROP TABLE IF EXISTS {self.schema}.temp_trips_weekday_distributed;
                CREATE TABLE {self.schema}.temp_trips_weekday_distributed
                (
                    trip_id TEXT,
                    route_id TEXT, 
                    route_type SMALLINT,
                    trip_headsign TEXT, 
                    shape_id TEXT,
                    weekdays bool[],
                    h3_3 integer
                );
                SELECT create_distributed_table('{self.schema}.temp_trips_weekday_distributed', 'h3_3');
            """
            self.db.perform(sql_create_temp_trips_weekday_distributed)

            # Insert data into the distributed table
            sql_insert_temp_trips_weekday_distributed = f"""
                INSERT INTO {self.schema}.temp_trips_weekday_distributed
                SELECT t.trip_id, t.route_id, t.route_type::text::smallint, t.trip_headsign, t.shape_id, t.weekdays, j.h3_3
                FROM {self.schema}.temp_trips_weekday t
                CROSS JOIN LATERAL
                (
                    SELECT DISTINCT s.h3_3
                    FROM {self.schema}.undistributed_shape_dist_region s
                    WHERE t.shape_id = s.shape_id
                ) j;
                CREATE INDEX ON {self.schema}.temp_trips_weekday_distributed (h3_3, trip_id);
            """
            self.db.perform(sql_insert_temp_trips_weekday_distributed)

            # Create temporary table to be cleaned
            sql_create_stop_times_to_clean = f"""
                DROP TABLE IF EXISTS {self.schema}.stop_times_to_clean;
                CREATE TABLE {self.schema}.stop_times_to_clean AS 
                SELECT st.trip_id, st.arrival_time, stop_id, route_type::text::smallint, weekdays, w.route_id, w.trip_headsign, st.h3_3
                FROM {self.schema}.stop_times st
                LEFT JOIN {self.schema}.temp_trips_weekday_distributed w
                ON st.trip_id = w.trip_id
                WHERE st.h3_3 = w.h3_3;
            """
            self.db.perform(sql_create_stop_times_to_clean)
                            
            # Join stop_times with temp_trips_weekday_distributed and insert into stop_times_optimized
            sql_insert_stop_times_optimized = f"""
                INSERT INTO {self.schema}.stop_times_optimized(trip_id, stop_id, arrival_time, weekdays, route_type,  h3_3)
                SELECT (ARRAY_AGG(trip_id))[1], stop_id, arrival_time, weekdays, (ARRAY_AGG(route_type))[1],  h3_3
                FROM {self.schema}.stop_times_to_clean
                GROUP BY stop_id, arrival_time, weekdays, h3_3; 
            """
            self.db.perform(sql_insert_stop_times_optimized)

            print_info(
                f"Finished processing routes {i} to {i+self.small_bulk} out of {max_loop_id}."
            )

        # Clean up temporary tables
        self.db.perform(f"DROP TABLE IF EXISTS {self.schema}.temp_trips_weekday;")
        self.db.perform(f"DROP TABLE IF EXISTS {self.schema}.temp_trips_weekday_distributed;")
        self.db.perform(f"DROP TABLE IF EXISTS {self.schema}.undistributed_shape_dist_region;")

    @timing
    def add_indices(self):
        """Add indices to the stop_times_optimized table."""

        # Creating indices one my one to monitor progress
        self.db.perform(f"""ALTER TABLE {self.schema}.stop_times_optimized ADD PRIMARY KEY (h3_3, id);""")
        print_info("Added primary key to stop_times_optimized.")
        self.db.perform(f"""CREATE INDEX ON {self.schema}.stop_times_optimized (h3_3, stop_id, arrival_time);""")
        print_info("Added index to stop_times_optimized (h3_3, stop_id, arrival_time).")
        self.db.perform(f"""CREATE INDEX ON {self.schema}.stop_times_optimized (h3_3, trip_id);""")
        print_info("Added index to stop_times_optimized (h3_3, trip_id).")

    def run(self):
        """Run the gtfs preparation."""

        self.implement_data_corrections()
        self.prepare_shape_dist_region()
        self.prepare_stop_times()
        self.add_indices()


def prepare_gtfs(region: str):
    print_info(f"Prepare GTFS data for the region {region}.")
    db = Database(settings.LOCAL_DATABASE_URI)
    #db_rd = Database(settings.RAW_DATABASE_URI)

    try:
        GTFS(db=db, region=region).run()
        db.close()
        print_info("Finished GTFS preparation.")
    except Exception as e:
        print(e)
        raise e
    finally:
        db.close()


def export_gtfs():
    db = Database(settings.LOCAL_DATABASE_URI)
    create_table_dump(db.db_config, "gtfs", "stops")
    create_table_dump(db.db_config, "gtfs", "stop_times_optimized")
    create_table_dump(db.db_config, "gtfs", "shape_dist_region")
    db.close()