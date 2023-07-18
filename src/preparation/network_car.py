import xml.etree.ElementTree as ET
from src.core.config import settings
from src.db.db import Database
from src.utils.utils import print_info, delete_file
from src.core.enums import Weekday
import os, shutil
import subprocess
import polars as pl


class NetworkCar:
    """Class to process the network for cars"""

    def __init__(self, db: Database, time_of_the_day: str):
        """This is the constructor of the class.

        Args:
            db (Database): Database object
            time_of_the_day (str): Time of the day in the format "hh:mm"
        """
        self.db = db
        self.db_config = db.db_config
        self.db_uri = f"postgresql://{self.db_config.user}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}{self.db_config.path}"

        self.bulk_size = 100000

        sql_query_cnt = "SELECT COUNT(*) FROM dds_street_with_speed_subset;"
        cnt_network = self.db.select(sql_query_cnt)
        self.cnt_network = cnt_network[0][0]

        self.results_dir = settings.OUTPUT_DATA_DIR
        # Convert the time to the column name
        self.time_of_the_day = "h" + time_of_the_day.replace(":", "_")

    # TODO: We can try to make this a bit flexible and allow the user to pass different weekdays

    def create_serial_for_loop(self):
        # Create serial for the loop
        sql_create_serial_for_loop = f"""
            ALTER TABLE dds_street ADD COLUMN IF NOT EXISTS loop_serial SERIAL;
            CREATE INDEX ON dds_street (loop_serial);
        """
        self.db.perform(sql_create_serial_for_loop)

    def create_streets_with_speed(self, weekday: Weekday):
        if isinstance(weekday, Weekday) is False:
            raise ValueError("Invalid weekday")

        # Get the max loop serial
        max_loop_serial = self.db.select("SELECT MAX(loop_serial) FROM dds_street")
        max_loop_serial = max_loop_serial[0][0]

        # Create table for the streets with speed
        print_info(f"Creating table dds_street_with_speed for {weekday}...")
        sql_create_dds_street_with_speed = f"""
            DROP TABLE IF EXISTS dds_street_with_speed;
            CREATE TABLE public.dds_street_with_speed (
                gid integer NOT NULL,
                prim_name text NULL,
                sek_name text NULL,
                kat integer NULL,
                von integer NULL,
                nach integer NULL,
                id integer NULL,
                stil integer NULL,
                fussweg integer NULL,
                fuss_zone integer NULL,
                "level" integer NULL,
                kat_pre integer NULL,
                geom public.geometry(LineString,4326) NOT NULL,
                hin_speed_{weekday} jsonb NULL,
                rueck_speed_{weekday} jsonb NULL, 
                loop_serial integer NULL
            );
        """
        self.db.perform(sql_create_dds_street_with_speed)

        # Insert data in batches
        for offset in range(0, max_loop_serial, self.bulk_size):
            # Create selected streets for the bulk
            sql_create_selected_streets = f"""
                DROP TABLE IF EXISTS selected_dds_street;
                CREATE /*TEMP*/ TABLE selected_dds_street AS
                SELECT * 
                FROM dds_street
                WHERE loop_serial >= {offset} AND loop_serial < {offset + self.bulk_size};
                ALTER TABLE selected_dds_street ADD PRIMARY KEY (id);
            """
            self.db.perform(sql_create_selected_streets)

            # Create temp table with speed for the repective bulk
            sql_create_temp_streets_with_speed_table = f"""
                DROP TABLE IF EXISTS tmp_dds_street_with_speed;
                CREATE /*TEMP*/ TABLE tmp_dds_street_with_speed AS
                SELECT d.gid, d.prim_name, d.sek_name, d.kat, d.von, d.nach, d.id, d.stil, d.fussweg, d.fuss_zone, d."level", d.kat_pre, d.geom, j.hin_speed_{weekday}, j.rueck_speed_{weekday}, d.loop_serial
                FROM selected_dds_street d
                CROSS JOIN LATERAL 
                (
                    SELECT j.*
                    FROM traffic_patterns t
                    CROSS JOIN LATERAL
                    (
                        SELECT hin_speed_{weekday} , rueck_speed_{weekday} 
                        FROM
                        (
                            SELECT row_to_json(s.*)::jsonb - 'pattern_id' hin_speed_{weekday}
                            FROM speed_patterns s 
                            WHERE t.t_hin = s.pattern_id
                        ) x,
                        (
                            SELECT row_to_json(s.*)::jsonb - 'pattern_id' rueck_speed_{weekday}
                            FROM speed_patterns s 
                            WHERE t.t_rueck = s.pattern_id 
                        ) y
                    ) j
                    WHERE t.id = d.id
                ) j;
                ALTER TABLE tmp_dds_street_with_speed ADD PRIMARY KEY (id);
            """
            self.db.perform(sql_create_temp_streets_with_speed_table)

            # Insert data into the table that had matching speed patterns
            sql_insert_dds_street_with_speed = f"""
            INSERT INTO dds_street_with_speed (gid, prim_name, sek_name, kat, von, nach, id, stil, fussweg, fuss_zone, "level", kat_pre, geom, hin_speed_{weekday}, rueck_speed_{weekday}, loop_serial)
            SELECT DISTINCT * FROM tmp_dds_street_with_speed;
            """
            self.db.perform(sql_insert_dds_street_with_speed)

            sql_create_temp_street_unclassified = f"""
                DROP TABLE IF EXISTS tmp_dds_street_unclassified;
                CREATE /*TEMP*/ TABLE tmp_dds_street_unclassified AS
                SELECT DISTINCT d.gid, d.prim_name, d.sek_name, d.kat, d.von, d.nach, d.id, d.stil, d.fussweg, d.fuss_zone, d."level", d.kat_pre, d.geom, d.loop_serial
                FROM selected_dds_street d
                LEFT JOIN tmp_dds_street_with_speed s
                ON d.id = s.id
                WHERE s.id IS NULL;
            """
            self.db.perform(sql_create_temp_street_unclassified)

            # Insert data into the table that had no matching speed patterns
            sql_insert_dds_street_no_speed = f"""
            INSERT INTO dds_street_with_speed (gid, prim_name, sek_name, kat, von, nach, id, stil, fussweg, fuss_zone, "level", kat_pre, geom, loop_serial)
            SELECT * FROM tmp_dds_street_unclassified;
            """
            self.db.perform(sql_insert_dds_street_no_speed)

            sql_check_duplicated = f"""
                SELECT count(*), id 
                FROM dds_street_with_speed
                GROUP BY id 
                HAVING count(*) > 1
            """
            duplicated = self.db.select(sql_check_duplicated)

            if len(duplicated) > 0:
                raise Exception("Duplicated ids found")

            print_info(
                f"Inserted {offset + self.bulk_size} of {self.cnt_network} streets with speed"
            )

        # Create primary key and index on the geometry column
        sql_create_pk_and_index = """
            CREATE INDEX ON dds_street_with_speed (id);
            CREATE INDEX ON dds_street_with_speed USING GIST (geom);
            CLUSTER dds_street_with_speed USING dds_street_with_speed_id_idx;
            CLUSTER dds_street_with_speed;
        """
        self.db.perform(sql_create_pk_and_index)
        print_info("Finished inserting streets with speed")

    def create_network_nodes(self):
        """This function creates the nodes of the network and saves them into the database."""

        # Create empty dataframe for the nodes
        df = pl.DataFrame()
        # Create nodes of the network in batches
        for offset in range(0, self.cnt_network, self.bulk_size):
            sql_get_nodes = f"""
                SELECT DISTINCT (coords -> 1)::float lat, (coords -> 0)::float lon
                FROM (
                    SELECT geom 
                    FROM dds_street_with_speed_subset
                    WHERE stil IN (0, 1, 2, 3, 4, 5, 6, 9, 11)
                    LIMIT {self.bulk_size}
                    OFFSET {offset}
                ) s,  
                LATERAL jsonb_array_elements(
                    (st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates')
                ) coords
            """
            df = pl.concat([df, pl.read_database(sql_get_nodes, self.db_uri)])

            print_info(
                f"Reading nodes for {offset + self.bulk_size} of {self.cnt_network} ways"
            )
        # Get distinct nodes
        df = df.unique()

        # Write nodes to database
        df.write_database(
            table_name="dds_street_nodes",
            connection_uri=self.db_uri,
            if_exists="replace",
        )

        # Create id and primary key on id column
        self.db.perform(
            """
            ALTER TABLE dds_street_nodes ADD COLUMN id serial;
            ALTER TABLE dds_street_nodes ADD PRIMARY KEY (id);
            ALTER TABLE dds_street_nodes ADD COLUMN coords float[]; 
            UPDATE dds_street_nodes 
            SET coords = ARRAY[lat, lon];
            CREATE INDEX ON dds_street_nodes (coords); 
        """
        )
        print_info("Finished inserting nodes")

    def nodes_to_xml(self):
        """Creates the nodes of the network from database and converts them into XML."""

        # Create table storing the xml nodes
        sql_create_xml_table = """
            DROP TABLE IF EXISTS dds_street_nodes_xml;
            CREATE TABLE dds_street_nodes_xml (
                id bigint,
                xml_obj text
            );
        """
        self.db.perform(sql_create_xml_table)
        # Count nodes
        sql_cnt_nodes = """SELECT count(*) FROM public.dds_street_nodes;"""
        cnt_nodes = self.db.select(sql_cnt_nodes)

        # Read nodes in batches and convert them into XML
        for offset in range(0, cnt_nodes[0][0], self.bulk_size):
            sql_read_node = f"""
            INSERT INTO dds_street_nodes_xml (id, xml_obj)
            SELECT id, xmlelement(
                name node, 
                xmlattributes(
                    id as id,
                    lat,
                    lon
                )
            )
            FROM public.dds_street_nodes
            LIMIT {self.bulk_size}
            OFFSET {offset}
            ;"""
            self.db.perform(sql_read_node)
            print_info(
                f"Saved {offset + self.bulk_size} of {cnt_nodes[0][0]} nodes into xml"
            )

        sql_index_sort = """
            CREATE INDEX ON dds_street_nodes_xml (id);
            CLUSTER dds_street_nodes_xml USING dds_street_nodes_xml_id_idx;
            CLUSTER dds_street_nodes_xml;
        """
        self.db.perform(sql_index_sort)

    def ways_to_xml(self):
        """Reads the network of the car from database and converts it into XML."""

        # XML Table streets
        sql_create_xml_table = """
            DROP TABLE IF EXISTS dds_street_with_speed_xml;
            CREATE TABLE dds_street_with_speed_xml (
                id bigint,
                xml_obj text
            );
        """
        self.db.perform(sql_create_xml_table)

        max_id = self.db.select(
            """SELECT max(loop_serial) FROM public.dds_street_with_speed_subset;"""
        )[0][0]

        # Read network in batches
        for offset in range(0, max_id, self.bulk_size):
            sql_query_read_network_car = f"""
                INSERT INTO dds_street_with_speed_xml
                WITH batch_ways AS 
                (
                    SELECT id::bigint,
                        '{'{'}"0": "road",
                        "1": "motorway",
                        "2": "primary",
                        "3": "secondary", 
                        "4": "secondary", 
                        "5": "tertiary", 
                        "6": "road",
                        "9": "unclassified",
                        "11": "unclassified"
                        {'}'}'::jsonb ->> stil::TEXT AS highway, 
                    ((hin_speed_tuesday ->> '{self.time_of_the_day}')::integer + (rueck_speed_tuesday ->> '{self.time_of_the_day}')::integer) / 2 maxspeed, 
                    (st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates') AS coordinates 
                    FROM dds_street_with_speed_subset
                    WHERE loop_serial BETWEEN {offset} AND {offset + self.bulk_size}
                    AND stil IN (0, 1, 2, 3, 4, 5, 6, 9, 11)
                )
                SELECT DISTINCT w.id, xmlelement(name way, xmlattributes(w.id AS id),
                    XMLCONCAT(
                        j.nodes,
                        CASE WHEN w.highway IS NOT NULL THEN xmlelement(name tag, xmlattributes('highway' as k, w.highway as v)) END,
                        CASE WHEN w.maxspeed IS NOT NULL 
                        THEN xmlelement(name tag, xmlattributes('maxspeed' as k, w.maxspeed as v)) 
                        ELSE xmlelement(name tag, xmlattributes('maxspeed' as k, 
                        ('{'{'}
                            "road": 40,
                            "motorway": 130,
                            "primary": 90,
                            "secondary": 50,  
                            "tertiary": 40, 
                            "road": 40,
                            "unclassified": 40
                        {'}'}'::jsonb -> w.highway)::integer as v))
                        END
                    )
                )::TEXT AS way 
                FROM batch_ways w
                CROSS JOIN LATERAL 
                (
                    SELECT xmlagg(xmlelement(name nd, xmlattributes(n.id as ref))) AS nodes 
                    FROM public.dds_street_nodes n, 
                    (
                        SELECT ARRAY[(coords -> 1)::float, (coords -> 0)::float]::float[] AS coords 
                        FROM jsonb_array_elements(coordinates) coords 
                    ) x
                    WHERE n.coords = x.coords 
                ) j;
                """
        
            self.db.perform(sql_query_read_network_car)
            print_info(
                f"Saved {offset + self.bulk_size} of {self.cnt_network} ways into xml"
            )

        sql_index_sort = """
            CREATE INDEX ON dds_street_with_speed_xml (id);
            CLUSTER dds_street_with_speed_xml USING dds_street_with_speed_xml_id_idx;
            CLUSTER dds_street_with_speed_xml;
        """
        self.db.perform(sql_index_sort)

    def write_xml_to_file(self):
        """Reads the XML from database and writes it into a file."""

        print_info("Writing XML from DB to file")

        # Read Ways from database and write it into file
        sql_read_ways = """
            SELECT xml_obj FROM dds_street_with_speed_xml;
        """
        ways = self.db.select(sql_read_ways)
        ways = [x[0] for x in ways]
        ways = "".join(ways)

        # Read Nodes from database and write it into file
        sql_read_nodes = """
            SELECT xml_obj FROM dds_street_nodes_xml;
        """
        nodes = self.db.select(sql_read_nodes)
        nodes = [x[0] for x in nodes]
        nodes = "".join(nodes)

        osm_header = """<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" generator="Overpass API 0.7.59 e21c39fe">"""
        osm_footer = "</osm>"
        with open(
            os.path.join(
                self.results_dir,
                f"network.osm",
            ),
            "w",
        ) as f:
            f.write(osm_header + ways + nodes + osm_footer)

        # Fix file
        subprocess.run(
            f"""osmium sort {self.results_dir}/network.osm -o {self.results_dir}/network.osm --overwrite""",
            shell=True,
        )

        # Convert to pbf
        subprocess.run(
            f"""osmconvert {self.results_dir}/network.osm -o={self.results_dir}/network.osm.pbf""",
            shell=True,
        )

    def export_to_osm(self):

        # Create osm file with header
        with open(os.path.join(self.results_dir, f"network.osm"), "w") as f:
            f.write(
                """<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" generator="Overpass API 0.7.59 e21c39fe">\n"""
            )

        # Create empty file for nodes 
        with open(os.path.join(self.results_dir, f"network_node.osm"), "w") as f:
            f.write("") 

        # Create empty file for ways
        with open(os.path.join(self.results_dir, f"network_way.osm"), "w") as f:
            f.write("")

        # Max id of ways 
        sql_max_id_ways =  """
            SELECT MAX(loop_serial) FROM dds_street_with_speed_subset;
        """
        max_id_ways = self.db.select(sql_max_id_ways)[0][0]
        max_node_id = 1 
        for offset in range(0, max_id_ways, self.bulk_size):

            # Get node is for respective ways
            sql_create_table_node_cnt = f"""
            DROP TABLE IF EXISTS node_cnt;
            CREATE TABLE node_cnt AS 
            WITH node_counts AS (
                SELECT id,
                    array_length(ARRAY(
                        SELECT ARRAY[(coords -> 1)::float, (coords -> 0)::float]        
                        FROM jsonb_array_elements(st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates') coords
                    ), 1) AS count_nodes 
                FROM dds_street_with_speed_subset
                WHERE loop_serial >= {offset} AND loop_serial < {offset + self.bulk_size}
            ),
            node_cum_count AS 
            (
                SELECT id, SUM(count_nodes) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cumulative_count
                FROM node_counts
            )
            SELECT id, ({max_node_id} + COALESCE(cumulative_count, 0))::integer AS cumulative_count
            FROM node_cum_count c; 
            ALTER TABLE node_cnt ADD PRIMARY KEY(id);"""
            self.db.perform(sql_create_table_node_cnt)

            max_node_id = self.db.select("SELECT MAX(cumulative_count) FROM node_cnt")[0][0]

            # Read converted nodes and ways from the DB
            sql_export_to_osm = f"""
                WITH ways_to_export AS 
                (
                    SELECT w.id, geom, jsonb_strip_nulls(jsonb_build_object('highway', highway, 'maxspeed', maxspeed)) AS tags, w.cumulative_count
                    FROM 
                    (
                        SELECT w.id, geom, '{'{'}"0": "road",
                        "1": "motorway",
                        "2": "primary",
                        "3": "secondary", 
                        "4": "secondary", 
                        "5": "tertiary", 
                        "6": "road",
                        "9": "unclassified",
                        "11": "unclassified"{'}'}'::jsonb 
                        ->> stil::TEXT AS highway, 
                        ((hin_speed_tuesday ->> 'h08_00')::integer + (rueck_speed_tuesday ->> 'h08_00')::integer) / 2 AS maxspeed                    
                        , n.cumulative_count
                        FROM dds_street_with_speed_subset w, node_cnt n
                        WHERE w.id = n.id 
                        AND loop_serial >= {offset} AND loop_serial < {offset + self.bulk_size}
                    ) w
                )
                SELECT o.nodes, o.way
                FROM ways_to_export x, LATERAL basic.convert_way_to_osm(cumulative_count, x.id, geom, tags)  o
            """
            data = self.db.select(sql_export_to_osm)
    
            nodes_osm = [x[0] for x in data]
            nodes_osm = [item for sublist in nodes_osm for item in sublist]
            ways_osm = [x[1] for x in data]
            nodes_osm = "\n".join(nodes_osm)
            ways_osm = "\n".join(ways_osm)

            # Write ways to file
            with open(os.path.join(self.results_dir, f"network_node.osm"), "a") as f:
                f.write(nodes_osm)
         
            with open(os.path.join(self.results_dir, f"network_way.osm"), "a") as f:
                f.write(ways_osm)
         
            print_info(f"Exported {offset + self.bulk_size} of {max_id_ways}")

        # Add nodes to osm files using command line for perfmance reasons
        subprocess.run(
            f"""cat {self.results_dir}/network_node.osm {self.results_dir}/network_way.osm >> {self.results_dir}/network.osm""",
            shell=True,
        )

        # Add footer to osm file
        with open(os.path.join(self.results_dir, f"network.osm"), "a") as f:
            f.write("</osm>")

        # Fix file
        subprocess.run(
            f"""osmium sort {self.results_dir}/network.osm -o {self.results_dir}/network.osm --overwrite""",
            shell=True,
        )

        # Convert to pbf
        subprocess.run(
            f"""osmconvert {self.results_dir}/network.osm -o={self.results_dir}/network.osm.pbf""",
            shell=True,
        )


def main():
    """Main function."""
    db = Database(settings.RAW_DATABASE_URI)
    network_car = NetworkCar(db=db, time_of_the_day="08:00")

    # network_car.create_serial_for_loop()
    # network_car.create_streets_with_speed(weekday=Weekday["tuesday"])
    # network_car.export_to_osm()
    network_car.create_network_nodes()
    network_car.nodes_to_xml()
    network_car.ways_to_xml()
    network_car.write_xml_to_file()
    # network_car.merge_nodes_and_ways()

    # print("Creating the files...")
    # network_car.read_network_car()
    # print("Files have been created")
    # print("Started colliding all the files into one...")
    # network_car.collide_all_data()


if __name__ == "__main__":
    main()
