from src.db.db import Database
from src.core.config import settings

class NetworkCar:
    def __init__(self, db):
        self.db = db
        self.bulk_size = 100000

    #TODO: We can try to make this a bit flexible and allow the user to pass different times of the day
    # Weekdays we are not having currently though in the database
    def read_network_car(self, weekday: str, time_of_the_day: str):
        """Export the car network from the database for certain times of the day.
        """       
        sql_query_cnt = "SELECT COUNT(*) FROM dds_street_with_speed"
        cnt_network = self.db.select(sql_query_cnt)
        cnt_network = cnt_network[0][0]
        
        # Convert the time to the column name 
        time_of_the_day = "h"+time_of_the_day.replace(":", "_")
        
        for offset in range(0, cnt_network, self.bulk_size):
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
            print()
            #TODO: Convert to OSM format
    
def main():
    """Main function."""
    db = Database(settings.REMOTE_DATABASE_URI)
    network_car = NetworkCar(db=db)   
    network_car.read_network_car(1, "08:00")
    
if __name__ == "__main__":
    main()