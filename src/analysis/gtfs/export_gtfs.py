import csv

import psycopg2

from src.core.config import settings

SCHEMA_NAME = "gtfs"
TABLE_NAME = "agency"

# Connect to the database
conn = psycopg2.connect(
    dbname=settings.POSTGRES_DB,
    user=settings.POSTGRES_USER,
    password=settings.POSTGRES_PASSWORD,
    host=settings.POSTGRES_HOST,
    port=settings.POSTGRES_PORT,
)

# Create a cursor
cur = conn.cursor()

print("Fetching data...")

if TABLE_NAME == "agency":
    # Export agency
    cur.execute(f"""
        SELECT agency_id, agency_name, agency_url, agency_timezone, 
            agency_lang, agency_phone, agency_fare_url 
        FROM {SCHEMA_NAME}.agency;
    """)
elif TABLE_NAME == "routes":
    # Export routes
    cur.execute(f"""
        SELECT route_long_name, route_short_name, agency_id, route_desc, 
            route_type, route_id, route_color, route_text_color, route_sort_order 
        FROM {SCHEMA_NAME}.routes;
    """)
elif TABLE_NAME == "stops":
    # Export routes
    cur.execute(f"""
        SELECT stop_name, parent_station, stop_code, zone_id, stop_id, stop_desc, 
            stop_lat, stop_lon,stop_url, location_type, stop_timezone, wheelchair_boarding, 
            level_id, platform_code 
        FROM {SCHEMA_NAME}.stops;
    """)
elif TABLE_NAME == "calendar":
    # Export calendar
    cur.execute(f"""
        SELECT monday, tuesday, wednesday, thursday, friday, saturday, sunday, 
            REPLACE(start_date::text, '-', '') as start_date, REPLACE(end_date::text, '-', '') as end_date, 
            service_id 
        FROM {SCHEMA_NAME}.calendar;
    """)
elif TABLE_NAME == "calendar_dates":
    # Export calendar_dates
    cur.execute(f"""
        SELECT service_id, exception_type, REPLACE(date::text, '-', '') as date 
        FROM {SCHEMA_NAME}.calendar_dates;
    """)
elif TABLE_NAME == "trips":
    # Export trips
    cur.execute(f"""
        SELECT route_id, service_id, trip_headsign, trip_short_name, direction_id, 
            block_id, shape_id, trip_id, wheelchair_accessible, bikes_allowed 
        FROM {SCHEMA_NAME}.trips;
    """)
elif TABLE_NAME == "shapes":
    # Export shapes
    cur.execute(f"""
        SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence 
        FROM {SCHEMA_NAME}.shapes;
    """)
elif TABLE_NAME == "stop_times":
    h3_3_list = []
    cur.execute("SELECT h3_short FROM basic.h3_3_grid;")
    result = cur.fetchall()
    h3_3_list = [row[0] for row in result]

    file = open(f"{settings.OUTPUT_DATA_DIR}/gtfs/export/stop_times.txt", 'w', newline='')

    writer = csv.writer(file)

    # Export stop_times
    for i in range(len(h3_3_list)):
        cur.execute(f"""
            SELECT trip_id, arrival_time::text, departure_time::text, stop_id, stop_sequence, 
                stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, timepoint 
            FROM {SCHEMA_NAME}.stop_times
            WHERE h3_3 = {h3_3_list[i]};
        """)

        rows = cur.fetchall()

        if i == 0:
            column_names = [desc[0] for desc in cur.description]
            writer.writerow(column_names)

        if len(rows) > 0:
            print(f"Writing H3_3 region: {h3_3_list[i]}")

        writer.writerows(rows)

    file.close()
    cur.close()
    conn.close()
    exit()

# Get the results
rows = cur.fetchall()

# Get the column names
column_names = [desc[0] for desc in cur.description]

print("Writing to file...")

# Write the results to a .txt file
with open(f"{settings.OUTPUT_DATA_DIR}/gtfs/export/{TABLE_NAME}.txt", 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(column_names)
    writer.writerows(rows)

# Close the cursor and the connection
cur.close()
conn.close()
