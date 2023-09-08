from src.db.db import Database
from src.config.config import Config
from src.utils.utils import print_info, create_table_dump, restore_table_dump
from src.core.config import settings

class GTFS():
    def __init__(self, db: Database, db_rd: Database, region: str):
        self.db = db
        self.region = region
        # Get config for population
        self.config = Config("gtfs", region)

    def prepare_optimized_tables(self):
        """Prepare GTFS data for the region."""

        print_info("Prepare GTFS data for the region.")
        
        # Create results tables 
        sql_create_trips_weekday = f"""DROP TABLE IF EXISTS gtfs.trips_weekday;  
        CREATE TABLE gtfs.trips_weekday (id serial, trip_id TEXT, route_type smallint, weekdays boolean[]);"""
        self.db.perform(sql_create_trips_weekday)
        sql_create_stop_times_optimized = """
            DROP TABLE IF EXISTS gtfs.stop_times_optimized;
            CREATE TABLE gtfs.stop_times_optimized (
                id serial4 NOT NULL,
                trip_id text NULL,
                arrival_time interval NULL,
                stop_id text NULL,
                route_type smallint NULL,
                weekdays _bool NULL
            );
        """
        self.db.perform(sql_create_stop_times_optimized)
        
        # Create helper columns in routes for loop 
        sql_create_routes_helper = f"""ALTER TABLE gtfs.routes ADD COLUMN loop_id serial;""" 
        self.db.perform(sql_create_routes_helper)
        
        # Get max loop_id from routes
        sql_get_max_loop_id = f"""SELECT MAX(loop_id) FROM gtfs.routes;"""
        max_loop_id = self.db.select(sql_get_max_loop_id)[0][0]
        
        # Run processing in batches of 500 routes to avoid memory issues
        for i in range(0, max_loop_id, 500):
            f"""DROP TABLE IF EXISTS gtfs.temp_trips_weekday;  
            CREATE TABLE gtfs.temp_trips_weekday AS
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
                FROM gtfs.trips t, gtfs.routes r  
                WHERE t.route_id = r.route_id 
                AND r.loop_id > {i}
            ) t, gtfs.calendar c
            WHERE t.service_id = c.service_id
            AND '{self.config.preparation["start_date"]}' >= start_date 
            AND '{self.config.preparation["end_date"]}' <= end_date; 
            ALTER TABLE gtfs.temp_trips_weekday ADD COLUMN id serial;
            ALTER TABLE gtfs.temp_trips_weekday ADD PRIMARY KEY (id);
            CREATE INDEX ON gtfs.temp_trips_weekday (trip_id);"""
            self.db.perform(sql_create_trips_weekday)
        

            #Creates table with optimized structure for counting services on the station level
            sql_create_stop_times_optimized = """
            CREATE TABLE gtfs.temp_stop_times_optimized AS
            SELECT st.trip_id, st.arrival_time, stop_id, route_type::text::smallint, weekdays  
            FROM gtfs.stop_times st, gtfs.trips_weekday w 
            WHERE st.trip_id = w.trip_id;
            """
            self.db.perform(sql_create_stop_times_optimized)
            
            
            #INSERT INTO gtfs.stop_times_optimized(trip_id, arrival_time, stop_id, route_type, weekdays)
        self.db.perform(sql_create_stop_times_optimized)

    def create_table_partitions(self):
        """Create table partitions for the gtfs data."""
        
        # Move the gtfs data to db_rd
        
        # Create schema gtfs if not exists in db_rd
        sql_create_schema_gtfs = "CREATE SCHEMA IF NOT EXISTS gtfs;"
        self.db_rd.perform(sql_create_schema_gtfs)
        
        create_table_dump(self.db.db_config, "gtfs", "stops", data_only=True)
        create_table_dump(self.db.db_config, "gtfs", "stop_times_optimized", data_only=True)
        create_table_dump(self.db.db_config, "gtfs", "stop_times_optimized", data_only=True)
        restore_table_dump(self.db_rd.db_config, "gtfs", "stops", data_only=True)
        restore_table_dump(self.db_rd.db_config, "gtfs", "stop_times_optimized", data_only=True)
         
    
    def run(self):
        """Run the gtfs preparation."""
        self.prepare_optimized_tables()

def prepare_gtfs(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    db_rd = Database(settings.RAW_DATABASE_URI)

    GTFS(db=db, db_rd=db_rd, region=region).run()
    db.close()
    db_rd.close()
    print_info("Finished GTFS preparation.")


"""
    ALTER TABLE gtfs.stop_times_optimized ADD PRIMARY KEY(id); 
    CREATE INDEX ON gtfs.stop_times_optimized(stop_id, arrival_time);
"""
# TODO: 
# Refactor preparation 
# Test logic with tables partitions using Citus and h3 logic
# Run queries against the database and check results