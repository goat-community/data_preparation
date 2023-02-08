from src.db.db import Database
from src.core.config import settings

class NetworkCar:
    def __init__(self, db, time_of_the_day):
        self.db = db
        self.bulk_size = 100000
        
        sql_query_cnt = "SELECT COUNT(*) FROM dds_street_with_speed"
        cnt_network = self.db.select(sql_query_cnt)
        self.cnt_network = cnt_network[0][0]
        
        # Convert the time to the column name 
        self.time_of_the_day = "h"+time_of_the_day.replace(":", "_")

    #TODO: We can try to make this a bit flexible and allow the user to pass different times of the day
    # Weekdays we are not having currently though in the database
    
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
        """Export the car network from the database for certain times of the day.
        """       
        
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
            print()
            #TODO: Convert to OSM format
    
def main():
    """Main function."""
    db = Database(settings.REMOTE_DATABASE_URI)
    network_car = NetworkCar(db=db, time_of_the_day="08:00")   
    network_car.read_network_car()
    
if __name__ == "__main__":
    main()