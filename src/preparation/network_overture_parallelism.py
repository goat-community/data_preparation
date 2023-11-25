import time
from threading import Thread

import psycopg2
from tqdm import tqdm

from src.core.config import settings
from src.db.db import Database
from src.utils.utils import print_error


class ProcessSegments(Thread):

    def __init__(
            self,
            thread_id: int,
            db_local: Database,
            get_next_h3_index,
            cycling_surfaces
        ):
        super().__init__(group=None, target=self)

        self.thread_id = thread_id
        self.db_local = db_local
        self.get_next_h3_index = get_next_h3_index
        self.cycling_surfaces = cycling_surfaces


    def run(self):
        """Process segment data for this H3 index region"""

        # Create new DB connection for this thread
        connection_string = f"dbname={settings.POSTGRES_DB} user={settings.POSTGRES_USER} \
                            password={settings.POSTGRES_PASSWORD} host={settings.POSTGRES_HOST} \
                            port={settings.POSTGRES_PORT}"
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()

        h3_index = self.get_next_h3_index()
        while h3_index is not None:
            # Get all segment IDs for this H3 index
            sql_get_segment_ids = f"""
                SELECT s.id, ST_AsText(g.h3_boundary) FROM
                temporal.segments s, basic.h3_3_grid g
                WHERE
                ST_Intersects(ST_Centroid(s.geometry), g.h3_geom)
                AND g.h3_index = '{h3_index}';
            """
            segment_ids = cur.execute(sql_get_segment_ids)
            segment_ids = cur.fetchall()

            # Process each segment
            for index in tqdm(
                    range(len(segment_ids)),
                    desc=f"Thread {self.thread_id} - H3 index [{h3_index}]",
                    unit=" segments", mininterval=1, smoothing=0.0
                ):
                id = segment_ids[index]
                sql_classify_segment = f"""
                    SELECT classify_segment(
                        '{id[0]}',
                        '{self.cycling_surfaces}'::jsonb,
                        '{id[1]}'
                    );
                """
                try:
                    cur.execute(sql_classify_segment)
                    
                    # Commit changes to DB once every 1000 segments
                    # This significantly improves performance
                    if index % 1000 == 0:
                        conn.commit()
                except Exception as e:
                    print_error(f"Thread {self.thread_id} failed to process segment {h3_index}, error: {e}.")
                    break

            h3_index = self.get_next_h3_index()

        conn.close()
