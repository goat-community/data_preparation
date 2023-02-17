import fileinput
import glob
import xml.etree.ElementTree as ET

from src.core.config import settings
from src.db.db import Database
from src.utils.utils import file_merger, print_info, write_into_file


class NetworkCar:
    """Class to process the network for cars"""

    def __init__(self, db: Database, time_of_the_day: str):
        """This is the constructor of the class.

        Args:
            db (Database): Database object
            time_of_the_day (str): Time of the day in the format "hh:mm"
        """

        self.db = db
        self.bulk_size = 100000

        sql_query_cnt = "SELECT COUNT(*) FROM dds_street_with_speed"
        cnt_network = self.db.select(sql_query_cnt)
        self.cnt_network = cnt_network[0][0]

        # Convert the time to the column name
        self.time_of_the_day = "h" + time_of_the_day.replace(":", "_")

    # TODO: We can try to make this a bit flexible and allow the user to pass different weekdays
    def create_network_nodes(self):
        """This function creates the nodes of the network and saves them into the database."""
        # Create table for the nodes of the network
        sql_create_node_table = """
            DROP TABLE IF EXISTS dds_street_nodes;
            CREATE TABLE dds_street_nodes (
                id serial,
                coords float[]
            ); 
            ALTER TABLE dds_street_nodes ADD PRIMARY KEY (id);
            CREATE INDEX ON dds_street_nodes (coords);
        """
        self.db.perform(sql_create_node_table)

        # Create nodes of the network in batches
        for offset in range(0, self.cnt_network, self.bulk_size):
            sql_get_nodes = f"""
                INSERT INTO dds_street_nodes (coords)
                WITH new_nodes AS 
                (
                    SELECT DISTINCT ARRAY[(coords -> 1)::float, (coords -> 0)::float] AS coords
                    FROM (
                        SELECT geom 
                        FROM dds_street_with_speed 
                        LIMIT {self.bulk_size}
                        OFFSET {offset}
                    ) s,  
                    LATERAL jsonb_array_elements(
                        (st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates')
                    ) coords
                )
                SELECT n.* 
                FROM new_nodes n
                LEFT JOIN dds_street_nodes o
                ON n.coords = o.coords 
                WHERE o.id IS NULL;
            """
            self.db.perform(sql_get_nodes)

    def read_ways_car_xml(self):
        """Reads the network of the car from database and converts it into XML.
        and saves it into a file.
        """
        # Read network in batches
        for offset in range(0, self.cnt_network, self.bulk_size):

            sql_query_read_network_car = f"""
                WITH batch_ways AS 
                (
                    SELECT id::bigint, '{'{'}"0": "road",
                        "1": "motorway",
                        "2": "primary",
                        "3": "secondary", 
                        "4": "secondary", 
                        "5": "tertiary", 
                        "6": "road",
                        "9": "unclassified",
                        "11": "unclassified"
                    {'}'}'::jsonb 
                    ->> stil::TEXT AS highway, 
                    ((hin_speed_tuesday ->> '{self.time_of_the_day}')::integer + (rueck_speed_tuesday ->> '{self.time_of_the_day}')::integer) / 2 maxspeed, 
                    (st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates') AS coordinates 
                    FROM dds_street_with_speed 
                    LIMIT {self.bulk_size}
                    OFFSET {offset}
                )
                SELECT xmlelement(name way, xmlattributes(w.id AS id),
                    XMLCONCAT(
                        j.nodes,
                        xmlelement(name tag, 
                            xmlattributes('highway' as k, highway as v)
                        ), 
                        xmlelement(name tag, 
                            xmlattributes('maxspeed' as k, maxspeed as v)
                        )
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
            result = self.db.select(sql_query_read_network_car)
            result = [x[0] for x in result]
            properties = "".join(result)

            write_into_file(properties, f"{offset}offset.osm")

            cnt_completed = offset + self.bulk_size
            print_info(f"Calculated {cnt_completed} of {self.cnt_network} ways")

    def read_nodes_car_xml(self):
        """
        Reads all the nodes and saves it in files containing 1000 each
        """

        sql_cnt_nodes = """SELECT count(*) FROM public.dds_street_nodes;"""
        cnt_nodes = self.db.select(sql_cnt_nodes)

        for offset in range(0, cnt_nodes[0][0], self.bulk_size):
            sql_read_node = f"""
            SELECT xmlelement(
                name node, 
                xmlattributes(
                    id as id,
                    coords[1] as lat,
                    coords[2] as lon
                )
            )
            FROM public.dds_street_nodes
            LIMIT {self.bulk_size}
            OFFSET {offset}
            ;"""
            result = self.db.select(sql_read_node)
            result = "".join([x[0] for x in result])

            # write into file
            write_into_file(result, f"{offset}offset_nodes.osm")
            cnt_completed = offset + self.bulk_size
            print_info(f"Calculated {cnt_completed} of {self.cnt_network} ways")

    def file_merger(self, header: str, footer: str, file_output_name: str):
        """This function merges all the files into a single one osm traffic data file. The files have to be global though

        Args:
            header (str): header of the file <osm>...
            footer (str): footer is the closing tag of the header
            file_output_name (str): name of the file that we want to output everything to
        """

        file_list = glob.glob("*.osm")

        with open(file_output_name, "w") as file:
            input_lines = fileinput.input(file_list)
            file.writelines(header + input_lines + footer)

    # TODO: Use lower case for function names
    def putAllTogether(self):

        """This function creates the header and footer and passes it to file_merger"""

        print_info("Outputing the data")
        header = """<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" generator="Overpass API 0.7.59 e21c39fe">"""

        footer = """</osm>"""

        file_merger(
            header=header,
            footer=footer,
            file_output_name="traffic_data.osm",
            directory="",
        )


def main():
    """Main function."""
    db = Database(settings.REMOTE_DATABASE_URI)
    network_car = NetworkCar(db=db, time_of_the_day="08:00")
    # TODO: Use print_info from src.utils.utils for printing
    print_info("Creating the files...")
    network_car.read_ways_car_xml()
    network_car.read_nodes_car_xml()
    print_info("Files have been created")
    print_info("Started colliding all the files into one...")
    network_car.putAllTogether()


if __name__ == "__main__":
    main()
