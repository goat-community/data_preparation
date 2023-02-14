import xml.etree.ElementTree as ET
from src.core.config import settings
from src.db.db import Database


class NetworkCar:
    """Class to process the network for cars
    """    
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
        """This function creates the nodes of the network and saves them into the database.
        """        
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

    def read_network_car(self):
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

            header = """<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" generator="Overpass API 0.7.59 e21c39fe">"""
            footer = """</osm>"""
            properties = ""

            # result_data = (properties + way for way in result);
            for indx, resultItem in enumerate(result):
                print(f"count: {indx}")
                properties = properties + resultItem

            xmlFileContent = header + properties + footer

            with open(f"{offset}offset.osm", "w") as f:
                f.write(xmlFileContent)

    # TODO: Make a function that is called write_into_osm_file
    def collide_all_data(self):
        """
        Here we get all the nodes and create them into xml tags in order to add them to a common file
        """
        print("Creating the nodes...")
        # TODO: I would put this into a seperate function
        # TODO: I would stay consistent and also read the nodes as XML already from DB
        nodes = ""
        sql_get_nodes = """
            SELECT *
            FROM public.dds_street_nodes;
        """
        results = self.db.select(sql_get_nodes)
        for result in results:
            nodes = (
                nodes
                + f'<node id="{result[0]}" lat="{result[1][0]}" lon="{result[1][1]}"/>'
            )
        print("Created all the nodes")
        ######################
        
        
        """
           Here we collide all the nodes into one file since their geometries will refer to the nodes that we go above.
           They need to be in the same file
        """

        ways = ""
        print("Merging all the ways...")
        for offset in range(0, self.cnt_network, self.bulk_size):
            node = None
            fileinfo = ET.parse(f"{offset}offset.osm")
            root = fileinfo.getroot()
            if node is None:
                node = root
            elements = root.find("./way")
            for element in elements.iter():
                node[1].append(element)

            ways = ways + str(ET.tostring(node), "UTF-8")
        print("Finished merging the ways")
        self.putAllTogether(nodes=nodes, ways=ways)

        """
            Here we put together all the things
            header - Info about the file and file format
            footer - closing tag for header
            ways - includes all the roads
            nodes - includes all the references to geopoints in the ways
        """
    #TODO: Use lower case for function names
    def putAllTogether(self, nodes, ways):
        print("Outputing the data")
        header = """<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" generator="Overpass API 0.7.59 e21c39fe">"""

        footer = """</osm>"""

        allData = header + ways + nodes + footer

        with open("traffic_data.osm", "w") as f:
            f.write(allData)
            print("Data Outputed")
            print("Finished")


def main():
    """Main function."""
    db = Database(settings.REMOTE_DATABASE_URI)
    network_car = NetworkCar(db=db, time_of_the_day="08:00")
    #TODO: Use print_info from src.utils.utils for printing 
    print("Creating the files...")
    network_car.read_network_car()
    print("Files have been created")
    print("Started colliding all the files into one...")
    network_car.collide_all_data()


if __name__ == "__main__":
    main()
