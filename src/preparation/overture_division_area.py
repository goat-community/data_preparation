from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import (
    print_error,
    print_info,
    timing,
)


class OvertureDivisionAreaPreparation:

    BATCH_SIZE = 10_000

    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region
        self.config = Config("overture", region)

    def extract_relevant_columns(self):
        """Extract relevant columns from the raw collected dataset."""

        # Create separate output layer for each subtype
        subtypes = self.db.select(f"SELECT DISTINCT subtype FROM {self.config.collection['local_result_table']};")

        for subtype in subtypes:
            subtype = subtype[0]

            # Create local result table with relevant columns
            sql_create_table = f"""
                DROP TABLE IF EXISTS {self.config.collection['local_result_table']}_{subtype};
                CREATE TABLE {self.config.collection['local_result_table']}_{subtype} (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    subtype TEXT,
                    class TEXT,
                    country TEXT,
                    source TEXT,
                    geom GEOMETRY(GEOMETRY, 4326)
                );
            """
            self.db.perform(sql_create_table)

            # Iterate over data in batches
            num_rows = self.db.select(f"""
                SELECT COUNT(*)
                FROM {self.config.collection['local_result_table']}
                WHERE subtype = '{subtype}';
            """)[0][0]
            for i in range(0, num_rows, self.BATCH_SIZE):
                sql_extract_columns = f"""
                    INSERT INTO {self.config.collection['local_result_table']}_{subtype} (
                        SELECT
                            id,
                            (names::JSONB)->>'primary' AS name,
                            subtype,
                            class,
                            country,
                            string_agg(DISTINCT source->>'dataset', ', ') AS source,
                            geometry AS geom
                        FROM temporal.division_area_all,
                        LATERAL jsonb_array_elements(REPLACE(sources, '\\u0000', '')::JSONB) AS source
                        WHERE subtype = '{subtype}'
                        GROUP BY id, names, subtype, class, country
                        ORDER BY id
                        LIMIT {self.BATCH_SIZE}
                        OFFSET {i}
                    );
                """
                self.db.perform(sql_extract_columns)

                print_info(f"Processed data for {subtype}: {i + self.BATCH_SIZE}/{num_rows}")

    def run(self):
        """Run Overture data preparation process."""

        # Process and extract relevant columns
        self.extract_relevant_columns()


@timing
def prepare_overture_division_area(region: str):
    print_info(f"Preparing Overture data for region: {region}")
    db = Database(settings.LOCAL_DATABASE_URI)

    try:
        OvertureDivisionAreaPreparation(
            db=db,
            region=region,
        ).run()
        print_info(f"Finished preparing Overture data for region: {region}")
    except Exception as e:
        print_error(f"Failed to prepare Overture data for region: {region}")
        raise e
    finally:
        db.close()
