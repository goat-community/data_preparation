from src.utils.utils import create_table_dump, restore_table_dump
from src.core.config import settings
from src.db.db import Database


def migrate_gtfs():
    db = Database(settings.RAW_DATABASE_URI)
    db_goat = Database(settings.GOAT_DATABASE_URI)

    # Migrate table by table
    tables = ["stops_optimized", "stop_times_optimized", "trips_optimized"]
    for table in tables:
        create_table_dump(db.db_config, "gtfs", table)
        db_goat.perform(f"DROP TABLE IF EXISTS gtfs.{table} CASCADE;")
        restore_table_dump(db_goat.db_config, "gtfs", table)

