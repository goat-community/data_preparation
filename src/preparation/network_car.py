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

        sql_query_cnt = "SELECT COUNT(*) FROM dds_street_with_speed;"
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
                f"Inserted {offset + self.bulk_size} of {max_loop_serial} streets with speed"
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

        # Create empty table for nodes 
        sql_create_nodes_table = """
            DROP TABLE IF EXISTS dds_street_nodes;
            CREATE TABLE dds_street_nodes (
                lat float,
                lon float,
                coords float[]
            );
            CREATE INDEX ON dds_street_nodes (coords); 
        """
        self.db.perform(sql_create_nodes_table)
        
        max_id = self.db.select(
            """SELECT max(loop_serial) FROM public.dds_street_with_speed_subset;"""
        )[0][0]
        
        # Create nodes of the network in batches
        for offset in range(0, max_id, self.bulk_size):
            sql_get_nodes = f"""
                DROP TABLE IF EXISTS tmp_dds_street_nodes;
                CREATE /*TEMP*/ TABLE tmp_dds_street_nodes AS 
                SELECT DISTINCT (coords -> 1)::float lat, (coords -> 0)::float lon, 
                ARRAY[(coords -> 1)::float, (coords -> 0)::float] coords
                FROM (
                    SELECT geom 
                    FROM dds_street_with_speed_subset
                    WHERE stil IN (0, 1, 2, 3, 4, 5, 6, 9, 11)
                    LIMIT {self.bulk_size}
                    OFFSET {offset}
                ) s,  
                LATERAL jsonb_array_elements(
                    (st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates')
                ) coords;
                ALTER TABLE tmp_dds_street_nodes ADD PRIMARY KEY (coords);
            """
            self.db.perform(sql_get_nodes)
            
            # Insert using left join to avoid adding duplicates
            sql_insert_nodes = f"""
                INSERT INTO dds_street_nodes (lat, lon, coords)
                SELECT DISTINCT x.lat, x.lon, x.coords 
                FROM tmp_dds_street_nodes x
                LEFT JOIN dds_street_nodes n
                ON x.coords = n.coords
                WHERE n.coords IS NULL;
            """
            self.db.perform(sql_insert_nodes)
            print_info(
                f"Reading nodes for {offset + self.bulk_size} of {max_id} ways"
            )

        # Create id and primary key on id column
        self.db.perform(
            """
            ALTER TABLE dds_street_nodes ADD COLUMN id serial;
            ALTER TABLE dds_street_nodes ADD PRIMARY KEY (id);
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
                f"Saved {offset + self.bulk_size} of {max_id} ways into xml"
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

def main():
    """Main function."""
    db = Database(settings.RAW_DATABASE_URI)
    network_car = NetworkCar(db=db, time_of_the_day="08:00")

    # network_car.create_serial_for_loop()
    # network_car.create_streets_with_speed(weekday=Weekday["tuesday"])
    network_car.create_network_nodes()
    network_car.nodes_to_xml()
    network_car.ways_to_xml()
    network_car.write_xml_to_file()

if __name__ == "__main__":
    main()
