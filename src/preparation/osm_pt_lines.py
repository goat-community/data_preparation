from src.config.config import Config
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import print_error, print_info, timing


class OSMPTLinesPreparation():
    """Prepares public transport lines from OSM."""

    def __init__(self, db: Database, region: str):
        self.db = db
        self.region = region
        self.config = Config("osm_pt_lines", region)

    def validate_config(self):
        """Validate the data config."""

        # Check if at least one output dataset is specified
        datasets = self.config.preparation.get("datasets")
        if not datasets or not len(datasets):
            raise ValueError("At least one output dataset must be specified in the data config")

        # Validate each output dataset
        for _, config in datasets.items():
            local_result_table = config.get("local_result_table")
            if not local_result_table:
                raise ValueError("Local result table must be specified in each dataset config")

            modes = config.get("modes")
            if not modes:
                raise ValueError("Modes must be specified in the dataset config")

            for mode in modes:
                mode_config = config.get(mode)
                if mode_config and (mode_config.get("exclude_operators") and mode_config.get("include_operators")):
                    raise ValueError("Only one of exclude_operators or include_operators may be specified")

    def initialize_osm_tables(self):
        """Create indexes and columns on the collected OSM data tables for further processing."""

        # Create index on line table to speed up lookups by operator
        self.db.perform(f"""
            CREATE INDEX IF NOT EXISTS osm_pt_lines_preparation ON
            osm_{self.config.name}_{self.region}_line (route, operator);
        """)

        # Add route_master column to line table used for grouping routes into lines
        self.db.perform(f"""
            ALTER TABLE osm_{self.config.name}_{self.region}_line
            ADD COLUMN IF NOT EXISTS route_master INT NULL;
        """)

        print_info("Initialized collected OSM tables for further processing.")

    def perform_route_master_linkage(self, operators: list[str]):
        """Link routes to route masters if available."""

        print_info("Linking routes to route masters.")

        # Do this one operator at a time as we are joining large tables
        for i in range(len(operators)):
            operator = operators[i]
            self.db.perform(f"""
                UPDATE osm_{self.config.name}_{self.region}_line l
                SET route_master = sub.route_master
                FROM (
                    SELECT l.osm_id, r.id route_master
                    FROM osm_{self.config.name}_{self.region}_line l,
                    osm_{self.config.name}_{self.region}_rels r
                    WHERE l.operator = %s
                    AND 'route_master' = ANY(r.tags)
                    AND ABS(l.osm_id) = ANY(r.parts)
                ) sub
                WHERE l.osm_id = sub.osm_id;
            """, (operator,))

            if i % 50 == 0:
                print_info(f"Processed {i} of {len(operators)} operators.")

        print_info("Finished linking routes to route masters.")

    def produce_pt_lines(self, operators: list[str]):
        """Process collected OSM data to produce public transport line geometries."""

        # Produce configured datasets
        datasets = self.config.preparation.get("datasets")
        for name, config in datasets.items():

            print_info(f"Producing OSM PT Lines for dataset: {name}")

            # Initialize result table
            local_result_table = config.get("local_result_table")
            self.db.perform(f"DROP TABLE IF EXISTS {local_result_table};")
            self.db.perform(f"""
                CREATE TABLE {local_result_table} (
                    id SERIAL PRIMARY KEY,
                    mode TEXT,
                    line TEXT,
                    operator TEXT,
                    routes TEXT[],
                    geom GEOMETRY
                );
            """)

            modes = config.get("modes")
            for mode in modes:
                print_info(f"Processing mode: {mode}")

                mode_config = config.get(mode, {})
                exclude_operators = mode_config.get("exclude_operators", [])
                exclude_operators: list[str] = [o.lower() for o in exclude_operators]
                include_operators = mode_config.get("include_operators", [])
                include_operators: list[str] = [o.lower() for o in include_operators]
                for operator in operators:
                    # Handle exclusion / inclusion criteria
                    if exclude_operators and operator.lower() in exclude_operators:
                        continue
                    elif include_operators and operator.lower() not in include_operators:
                        continue

                    # Produce grouped public transport lines by mode and operator
                    self.db.perform(f"""
                        INSERT INTO {local_result_table} (mode, line, operator, routes, geom)
                        SELECT route AS mode, ref AS line, operator, ARRAY_AGG(DISTINCT "from" || ' → ' || "to") AS routes, ST_Simplify(ST_Union(way), 0.0001) AS geom
                        FROM osm_{self.config.name}_{self.region}_line
                        WHERE route = %s
                        AND operator = %s
                        AND ref IS NOT NULL
                        AND route_master IS NOT NULL
                        GROUP BY route, ref, operator, route_master
                        UNION
                        SELECT route AS mode, ref AS line, operator, ARRAY_AGG(DISTINCT "from" || ' → ' || "to") AS routes, ST_Simplify(ST_Union(way), 0.0001) AS geom
                        FROM osm_{self.config.name}_{self.region}_line
                        WHERE route = %s
                        AND operator = %s
                        AND ref IS NOT NULL
                        AND route_master IS NULL
                        GROUP BY route, ref, operator;
                    """, (mode, operator, mode, operator))

            # Create gist index on result table
            self.db.perform(f"CREATE INDEX ON {local_result_table} USING GIST (geom);")

    def cleanup(self):
        """Convert all produced feature geometries to a Multi-Linestring for consistency."""

        datasets = self.config.preparation.get("datasets")
        for _, config in datasets.items():
            local_result_table = config.get("local_result_table")
            self.db.perform(f"""
                UPDATE {local_result_table}
                SET geom = ST_Multi(geom)
                WHERE ST_GeometryType(geom) = 'ST_LineString';
            """)

    def run(self):
        """Run OSM public transport lines preparation process."""

        # Get a list of all operators
        operators = self.db.select(f"""
            SELECT DISTINCT operator
            FROM osm_{self.config.name}_{self.region}_line
            WHERE operator IS NOT NULL;
        """)
        operators: list[str] = [operator[0] for operator in operators]

        # Validate the data config
        self.validate_config()

        # Initialize OSM tables
        self.initialize_osm_tables()

        # Perform route master linkage
        self.perform_route_master_linkage(operators)

        # Produce public transport lines
        self.produce_pt_lines(operators)

        # Cleanup
        self.cleanup()


@timing
def prepare_osm_pt_lines(region: str):
    print_info(f"Preparing OSM PT Lines data for region: {region}")
    db = Database(settings.LOCAL_DATABASE_URI)

    try:
        OSMPTLinesPreparation(
            db=db,
            region=region,
        ).run()
        print_info(f"Finished preparing OSM PT Lines data for region: {region}")
    except Exception as e:
        print_error(f"Failed to prepare OSM PT Lines data for region: {region}")
        raise e
    finally:
        db.close()
