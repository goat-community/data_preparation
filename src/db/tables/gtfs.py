class GtfsTables:
    def __init__(self, schema):
        self.schema = schema

    def sql_create_table(self) -> dict:
        sql_create_table_agency = f"""
            CREATE TABLE {self.schema}.agency (
                agency_id text NOT NULL,
                agency_name text NOT NULL,
                agency_url text NOT NULL,
                agency_timezone text NOT NULL,
                agency_lang text NULL,
                agency_phone text NULL,
                agency_fare_url text NULL,
                agency_email text NULL
            );
        """
        sql_create_table_stops = f"""
            CREATE TABLE {self.schema}.stops (
                stop_id text NOT NULL,
                stop_code text NULL,
                stop_name text NULL,
                stop_desc text NULL,
                stop_lat float4 NOT NULL,
                stop_lon float4 NOT NULL,
                zone_id text NULL,
                stop_url text NULL,
                location_type text NULL,
                parent_station text NULL,
                stop_timezone text NULL,
                wheelchair_boarding text NULL,
                level_id text NULL,
                platform_code text NULL,
                geom public.geometry(point, 4326) NULL,
                h3_3 int4 NULL
            );
        """

        sql_create_table_routes = f"""
            CREATE TABLE {self.schema}.routes (
                route_id text NOT NULL,
                agency_id text NULL,
                route_short_name text NULL,
                route_long_name text NULL,
                route_desc text NULL,
                route_type text NOT NULL,
                route_url text NULL,
                route_color text NULL,
                route_text_color text NULL,
                route_sort_order int4 NULL,
                continuous_drop_off text NULL,
                continuous_pickup text NULL
            );
        """

        sql_create_table_trips = f"""
            CREATE TABLE {self.schema}.trips (
                trip_id text NOT NULL,
                route_id text NOT NULL,
                service_id text NOT NULL,
                trip_headsign text NULL,
                trip_short_name text NULL,
                direction_id int4 NULL,
                block_id text NULL,
                shape_id text NULL,
                wheelchair_accessible text NULL,
                bikes_allowed text NULL
            );"""

        sql_create_table_stop_times = f"""
            CREATE TABLE {self.schema}.stop_times (
                trip_id text NOT NULL,
                arrival_time interval NULL,
                departure_time interval NULL,
                stop_id text NOT NULL,
                stop_sequence int4 NOT NULL,
                stop_sequence_consec int4 NULL,
                stop_headsign text NULL,
                pickup_type text NULL,
                drop_off_type text NULL,
                shape_dist_traveled float4 NULL,
                timepoint text NULL,
                h3_3 int4 NULL
            );"""

        sql_create_table_calendar = f"""
            CREATE TABLE {self.schema}.calendar (
                service_id text NOT NULL,
                monday text NOT NULL,
                tuesday text NOT NULL,
                wednesday text NOT NULL,
                thursday text NOT NULL,
                friday text NOT NULL,
                saturday text NOT NULL,
                sunday text NOT NULL,
                start_date date NOT NULL,
                end_date date NOT NULL
            );"""

        sql_create_table_shapes = f"""
            CREATE TABLE {self.schema}.shapes (
                shape_id text NULL,
                shape_pt_lat float4 NOT NULL,
                shape_pt_lon float4 NOT NULL,
                shape_pt_sequence int4 NULL,
                geom public.geometry(point, 4326) NULL,
                shape_dist_traveled float4 NULL,
                h3_3 int4 NULL
            );
        """

        return {
            "agency": sql_create_table_agency,
            "stops": sql_create_table_stops,
            "routes": sql_create_table_routes,
            "trips": sql_create_table_trips,
            "stop_times": sql_create_table_stop_times,
            "calendar": sql_create_table_calendar,
            "shapes": sql_create_table_shapes
        }
