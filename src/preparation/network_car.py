import glob
import xml.etree.ElementTree as ET

from src.core.config import settings
from src.db.db import Database


class NetworkCar:
    def __init__(self, db):
        self.db = db
        self.bulk_size = 100000
<<<<<<< HEAD

        sql_query_cnt = "SELECT COUNT(*) FROM dds_street_with_speed"
        cnt_network = self.db.select(sql_query_cnt)
        self.cnt_network = cnt_network[0][0]
=======
>>>>>>> a83eaa02854c51bc7ae2faf39bcc989af7767d5f

        # Convert the time to the column name
        self.time_of_the_day = "h" + time_of_the_day.replace(":", "_")

    # TODO: We can try to make this a bit flexible and allow the user to pass different times of the day
    # Weekdays we are not having currently though in the database
<<<<<<< HEAD

    def create_network_nodes(self):
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
        """Export the car network from the database for certain times of the day."""

        for offset in range(0, self.cnt_network, self.bulk_size):
=======
    def read_network_car(self, weekday: str, time_of_the_day: str):
        """Export the car network from the database for certain times of the day.
        """       
        sql_query_cnt = "SELECT COUNT(*) FROM dds_street_with_speed"
        cnt_network = self.db.select(sql_query_cnt)
        cnt_network = cnt_network[0][0]
        
        # Convert the time to the column name 
        time_of_the_day = "h"+time_of_the_day.replace(":", "_")
        
        for offset in range(0, cnt_network, self.bulk_size):
>>>>>>> a83eaa02854c51bc7ae2faf39bcc989af7767d5f
            sql_query_read_network_car = f"""
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
                ((hin_speed_tuesday ->> '{time_of_the_day}')::integer + (rueck_speed_tuesday ->> '{time_of_the_day}')::integer) / 2 maxspeed, 
                (st_asgeojson(ST_SETSRID(geom, 4326))::jsonb -> 'coordinates') AS coordinates 
                FROM dds_street_with_speed 
                LIMIT {self.bulk_size}
                OFFSET {offset};
            """
            result = self.db.select(sql_query_read_network_car)
<<<<<<< HEAD
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

            # TODO: Convert to OSM format

    def collide_all_data(self):
        """
        Here we get all the nodes and create them into xml tags in order to add them to a common file
        """
        print("Creating the nodes...")
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
    print("Creating the files...")
    network_car.read_network_car()
    print("Files have been created")
    print("Started colliding all the files into one...")
    network_car.collide_all_data()


=======
            print()
            #TODO: Convert to OSM format
    
def main():
    """Main function."""
    db = Database(settings.REMOTE_DATABASE_URI)
    network_car = NetworkCar(db=db)   
    network_car.read_network_car(1, "08:00")
    
>>>>>>> a83eaa02854c51bc7ae2faf39bcc989af7767d5f
if __name__ == "__main__":
    main()
