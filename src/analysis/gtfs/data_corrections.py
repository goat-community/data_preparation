import csv
import os

from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import print_info, timing


class DataCorrections:

    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region
        self.config = Config("gtfs", region)
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
    def fix_nuremberg_routes(self):
        """Fix issues with routes and trips specific to Nuremberg

            Not sure what's up in Nuremburg (agency 654)?, but they're randomly adding incorrect
            U-Bahn (route type 402) & Tram (route type 900) trips to almost every route in the city :(

            NOTE: This fix currently only cleans up data in the stop_times_optimized table, not routes or trips.
            Funnily enough, actual U-Bahn routes are labelled type 400, so we can just delete all 402 routes."""

        sql_fix_nuremberg_stop_times = """
            with extra_trips as
            (
                with route_types as (
                    select route_short_name, jsonb_object_agg(route_type, route_id) as route_types
                    from gtfs.routes
                    where agency_id = '654'
                    group by agency_id, route_short_name
                ),
                extra_metro_routes as (
                    select route_types->>'402' as route_id
                    from route_types
                    where route_types->>'402' is not null
                ),
                extra_tram_routes as (
                    select route_types->>'900' as route_id
                    from route_types
                    where route_types->>'700' is not null
                        and route_types->>'900' is not null
                )
                select trip_id from
                    (
                        select * from extra_metro_routes
                        union
                        select * from extra_tram_routes
                    ) r
                inner join
                    gtfs.trips t
                on
                    t.route_id = r.route_id
            )
            delete from gtfs.stop_times_optimized
            where trip_id in (select trip_id from extra_trips);
        """
        self.db.perform(sql_fix_nuremberg_stop_times)

        print_info("Finished fixing Nuremberg routes.")


    def run(self):
        """Run the correction process."""

        # self.implement_data_corrections()
        # self.fix_nuremberg_routes()


def perform_corrections(region: str):
    print_info(f"Perform corrections on GTFS data for the region {region}.")
    db = Database(settings.LOCAL_DATABASE_URI)

    try:
        DataCorrections(db=db, region=region).run()
        db.close()
        print_info("Finished GTFS preparation.")
    except Exception as e:
        print(e)
        raise e
    finally:
        db.close()


if __name__ == "__main__":
    perform_corrections("eu")
