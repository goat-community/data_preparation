from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import print_info


class TripCountAnalysis:
    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region
        self.config = Config("gtfs", region)
        self.schema = self.config.analysis["data_schema"]
        self.start_time = self.config.analysis["start_time"]
        self.end_time = self.config.analysis["end_time"]
        self.weekdays = self.config.analysis["weekdays"]
        self.sub_regions = self.config.analysis["sub_regions"]


    def create_trip_count_function(self):
        """Create SQL function for calculating trip counts"""

        sql_create_trip_count_func = f"""
            DROP FUNCTION IF EXISTS basic.count_public_transport_services_station;
            CREATE OR REPLACE FUNCTION basic.count_public_transport_services_station(
                start_time interval,
                end_time interval,
                weekday integer,
                reference_table_name TEXT
            )
            RETURNS TABLE(stop_id text, stop_name text, trip_cnt jsonb, geom geometry, trip_ids jsonb, h3_3 integer)
            LANGUAGE plpgsql
            AS $function$
            DECLARE
                table_region_stops TEXT;
            BEGIN
                -- Create the temp table name based on a uuid
                table_region_stops = 'temporal.' || '"' || REPLACE(uuid_generate_v4()::TEXT, '-', '') || '"';

                -- Create temporary table and execute dynamic SQL
                EXECUTE format(
                    'DROP TABLE IF EXISTS %s;
                    CREATE TABLE %s
                    (
                        stop_id TEXT,
                        h3_3 integer
                    );', table_region_stops, table_region_stops
                );
                -- Distribute the table with stops
                PERFORM create_distributed_table(table_region_stops, 'h3_3');

                -- Get relevant stations
                EXECUTE format(
                    'INSERT INTO %s
                    SELECT st.stop_id, st.h3_3
                    FROM {self.schema}.stops st, %s b
                    WHERE ST_Intersects(st.geom, b.geom)
                    AND st.location_type IS NULL OR st.location_type = ''0''
                    AND st.geom && b.geom',
                    table_region_stops, reference_table_name
                );

                -- Count trips per station and transport mode in respective time interval
                RETURN QUERY EXECUTE format(
                    'WITH trip_cnt AS
                    (
                        SELECT c.stop_id, j.route_type, cnt AS cnt, j.trip_ids, c.h3_3
                        FROM %s c
                        CROSS JOIN LATERAL (
                            SELECT t.route_type, SUM(weekdays[$1]::integer) cnt, ARRAY_AGG(trip_id) AS trip_ids
                            FROM {self.schema}.stop_times_optimized t, {self.schema}.stops s
                            WHERE t.stop_id = s.stop_id
                            AND s.stop_id = c.stop_id
                            AND s.h3_3 = c.h3_3
                            AND t.h3_3 = s.h3_3
                            AND t.arrival_time BETWEEN $2 AND $3
                            AND weekdays[$4] = True
                            GROUP BY t.route_type
                        ) j
                    ),
                    o AS (
                        SELECT stop_id, jsonb_object_agg(route_type, cnt) AS trip_cnt, jsonb_object_agg(route_type, g.trip_ids) AS trip_ids, h3_3
                        FROM trip_cnt g
                        WHERE cnt <> 0
                        GROUP BY stop_id, h3_3
                    )
                    SELECT s.stop_id, s.stop_name, o.trip_cnt, s.geom, o.trip_ids, o.h3_3
                    FROM o, {self.schema}.stops s
                    WHERE o.stop_id = s.stop_id
                    AND s.h3_3 = o.h3_3', table_region_stops) USING weekday, start_time, end_time, weekday;

                -- Drop the temporary table
                EXECUTE format(
                    'DROP TABLE IF EXISTS %s;',
                    table_region_stops
                );
            END;
            $function$;
        """
        self.db.perform(sql_create_trip_count_func)


    def run_trip_count_function(self, sub_region: str):
        """Run trip count function for a specific sub-region"""

        print_info(f"Computing trip count for sub-region: {sub_region}.")

        # Initialize temp trip count table for trip counts at a stop level
        trip_cnt_table_name = f"temporal.trip_cnt_{sub_region}"
        sql_create_trip_cnt_table = f"""
            DROP TABLE IF EXISTS {trip_cnt_table_name};
            CREATE TABLE {trip_cnt_table_name} (
                stop_id TEXT,
                stop_name TEXT,
                trip_cnt_week INTEGER DEFAULT 0,
                trip_cnt_mon JSONB,
                trip_cnt_mon_total INTEGER DEFAULT 0,
                trip_cnt_sat JSONB,
                trip_cnt_sat_total INTEGER DEFAULT 0,
                trip_cnt_sun JSONB,
                trip_cnt_sun_total INTEGER DEFAULT 0,
                geom GEOMETRY,
                h3_3 INTEGER
            );

            ALTER TABLE {trip_cnt_table_name}
            ADD CONSTRAINT pk_trip_cnt_{sub_region} PRIMARY KEY (stop_id, h3_3);

            CREATE INDEX ON {trip_cnt_table_name} USING GIST (geom);
        """
        self.db.perform(sql_create_trip_cnt_table)

        # Run trip count function for each weekday, computing trip counts at a stop level
        for index in range(len(self.weekdays)):
            day = self.weekdays[index]
            sql_run_trip_count_func = f"""
                WITH trip_cnt AS (
                    SELECT *
                    FROM basic.count_public_transport_services_station(
                        '{self.start_time}','{self.end_time}',
                        {index + 1},
                        '(select geom from public.nuts where levl_code = 0 and cntr_code = ''{sub_region}'')'
                    )
                )
                INSERT INTO {trip_cnt_table_name} (stop_id, stop_name, trip_cnt_{day}, trip_cnt_{day}_total, geom, h3_3)
                    SELECT tc.stop_id, tc.stop_name, tc.trip_cnt,
                        (SELECT SUM(value::numeric) FROM jsonb_each_text(tc.trip_cnt) AS j(key, value)) AS trip_cnt_{day}_total,
                        tc.geom, tc.h3_3
                    FROM trip_cnt tc
                ON CONFLICT (stop_id, h3_3)
                DO UPDATE SET
                    trip_cnt_{day} = EXCLUDED.trip_cnt_{day},
                    trip_cnt_{day}_total = EXCLUDED.trip_cnt_{day}_total;
            """
            self.db.perform(sql_run_trip_count_func)

        # Update the full week trip count
        sql_update_week_trip_count = f"""
            with week_cnt as (
                select stop_id,
                    ((trip_cnt_mon_total * 5) +
                    trip_cnt_sat_total +
                    trip_cnt_sun_total) as trip_cnt_week
                from {trip_cnt_table_name}
            )
            update {trip_cnt_table_name} as tc
            set trip_cnt_week = week_cnt.trip_cnt_week
            from week_cnt
            where tc.stop_id = week_cnt.stop_id;
        """
        self.db.perform(sql_update_week_trip_count)


    def perform_regional_aggregation(self, sub_region: str):
        """Perform regional aggregation of trip counts"""

        # Initialize final aggregated trip count table for trip counts at a LAU/NUTS-3 region level
        agg_table_name = f"temporal.trip_cnt_agg_{sub_region}"
        sql_create_agg_table = f"""
            DROP TABLE IF EXISTS {agg_table_name};
            CREATE TABLE {agg_table_name} (
                lau_id TEXT,
                lau_name TEXT,
                trip_cnt_week INT DEFAULT 0,
                trip_cnt_mon JSONB,
                trip_cnt_total_mon INT DEFAULT 0,
                trip_cnt_sat JSONB,
                trip_cnt_total_sat INT DEFAULT 0,
                trip_cnt_sun JSONB,
                trip_cnt_total_sun INT DEFAULT 0,
                geom GEOMETRY
            );

            ALTER TABLE {agg_table_name}
            ADD CONSTRAINT pk_trip_cnt_agg_{sub_region} PRIMARY KEY (lau_id, lau_name);

            CREATE INDEX ON {agg_table_name} USING GIST (geom);
        """
        self.db.perform(sql_create_agg_table)

        for day in self.weekdays:
            sql_get_route_types = f"""
                SELECT DISTINCT jsonb_object_keys(trip_cnt_{day}) FROM temporal.trip_cnt_{sub_region};
            """
            route_types = self.db.select(sql_get_route_types)

            route_types_agg = ""
            for route_type in route_types:
                route_types_agg += f"'{route_type[0]}', COALESCE(SUM((stops.trip_cnt_{day}->>'{route_type[0]}')::int), 0), "
            route_types_agg = route_types_agg.strip(", ")

            route_types_agg2 = ""
            for route_type in route_types:
                route_types_agg2 += f"(trip_cnt->>'{route_type[0]}')::int + "
            route_types_agg2 = route_types_agg2.strip(" +")

            sql_aggregate_trip_counts = f"""
                WITH final_trip_counts AS (
                    WITH trip_counts AS (
                        SELECT region.lau_id, region.lau_name, region.geom, jsonb_build_object({route_types_agg}) AS trip_cnt
                        FROM
                        (SELECT lau_id, lau_name, geom FROM public.lau WHERE cntr_code = '{sub_region}') region
                        INNER JOIN
                        temporal.trip_cnt_{sub_region} stops
                        ON region.geom && stops.geom
                        WHERE ST_Intersects(region.geom, stops.geom)
                        GROUP BY region.lau_id, region.lau_name, region.geom
                    )
                    SELECT *, ({route_types_agg2}) AS trip_cnt_total FROM trip_counts
                )
                INSERT INTO {agg_table_name} (lau_id, lau_name, trip_cnt_{day}, trip_cnt_total_{day}, geom)
                    SELECT ftc.lau_id, ftc.lau_name, ftc.trip_cnt, ftc.trip_cnt_total, ftc.geom
                    FROM final_trip_counts ftc
                ON CONFLICT (lau_id, lau_name)
                DO UPDATE SET
                    trip_cnt_{day} = EXCLUDED.trip_cnt_{day},
                    trip_cnt_total_{day} = EXCLUDED.trip_cnt_total_{day};
            """
            self.db.perform(sql_aggregate_trip_counts)

        sql_update_full_week_count = f"""
            UPDATE {agg_table_name}
            SET trip_cnt_week = (trip_cnt_total_mon * 5) + trip_cnt_total_sat + trip_cnt_total_sun;
        """
        self.db.perform(sql_update_full_week_count)


    def run(self):
        """Run the trip count analysis."""

        # Initialize the trip count function
        self.create_trip_count_function()

        # Run trip count function for all sub-regions
        for sub_region in self.sub_regions:
            self.run_trip_count_function(sub_region)
            self.perform_regional_aggregation(sub_region)


def perform_analysis_trip_count(region: str):
    print_info(f"Running Trip Count analysis for region: {region}.")
    db = Database(settings.LOCAL_DATABASE_URI)

    try:
        TripCountAnalysis(db=db, region=region).run()
        db.close()
        print_info("Finished Trip Count analysis.")
    except Exception as e:
        print(e)
        raise e
    finally:
        db.close()


if __name__ == "__main__":
    perform_analysis_trip_count("eu")
