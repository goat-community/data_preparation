from src.utils.utils import create_table_dump, restore_table_dump
from src.core.config import settings
from src.db.db import Database


def migrate_gtfs():
    db = Database(settings.RAW_DATABASE_URI)
    db_goat = Database(settings.GOAT_DATABASE_URI)
    
    # Dump tables
    create_table_dump(db.db_config, "gtfs", "stops_optimized")
    create_table_dump(db.db_config, "gtfs", "stop_times_optimized")
    create_table_dump(db.db_config, "gtfs", "trips_optimized")

    # Create schema if not exists and restore tables
    db_goat.perform("""
        DROP SCHEMA IF EXISTS gtfs CASCADE;
        CREATE SCHEMA IF NOT EXISTS gtfs;
    """)
    restore_table_dump(db_goat.db_config, "gtfs", "stops_optimized")
    restore_table_dump(db_goat.db_config, "gtfs", "stop_times_optimized")
    restore_table_dump(db_goat.db_config, "gtfs", "trips_optimized")
