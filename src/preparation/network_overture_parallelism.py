import time
from threading import Thread

from tqdm import tqdm

from src.utils.utils import print_error


class ProcessSegments(Thread):

    def __init__(
            self,
            thread_id: int,
            db_connection,
            get_next_h3_index,
            cycling_surfaces
        ):
        super().__init__(group=None, target=self)

        self.thread_id = thread_id
        self.db_connection = db_connection
        self.db_cursor = db_connection.cursor()
        self.get_next_h3_index = get_next_h3_index
        self.cycling_surfaces = cycling_surfaces


    def run(self):
        """Process segment data for this H3 index region"""

        h3_index = self.get_next_h3_index()
        while h3_index is not None:
            # Get all segment IDs for this H3_3 index
            # Ensure segments are within valid H3_6 cells as well
            sql_get_segment_ids = f"""
                SELECT s.id
                FROM temporal.segments s, basic.h3_3_grid g1, basic.h3_6_grid g2
                WHERE
                    ST_Intersects(ST_Centroid(s.geometry), g1.h3_geom)
                    AND ST_Intersects(ST_Centroid(s.geometry), g2.h3_geom)
                    AND g1.h3_index = '{h3_index}';
            """
            segment_ids = self.db_cursor.execute(sql_get_segment_ids)
            segment_ids = self.db_cursor.fetchall()

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
                        '{self.cycling_surfaces}'::jsonb
                    );
                """
                try:
                    self.db_cursor.execute(sql_classify_segment)

                    # Commit changes to DB once every 1000 segments
                    # This significantly improves performance
                    if index % 1000 == 0:
                        self.db_connection.commit()
                except Exception as e:
                    print_error(f"Thread {self.thread_id} failed to process segment {h3_index}, error: {e}.")
                    break

            h3_index = self.get_next_h3_index()


class ComputeImpedance(Thread):

    def __init__(
            self,
            thread_id: int,
            db_connection,
            get_next_h3_index,
        ):
        super().__init__(group=None, target=self)

        self.thread_id = thread_id
        self.db_connection = db_connection
        self.db_cursor = db_connection.cursor()
        self.get_next_h3_index = get_next_h3_index


    def run(self):
        """Update slope impedance data for this H3 index region"""

        h3_short = self.get_next_h3_index()
        while h3_short is not None:
            sql_update_impedance = f"""
                WITH segment AS (
                    SELECT id, length_m, geom
                    FROM basic.segment
                    WHERE h3_6 = {h3_short}
                )
                UPDATE basic.segment AS sp
                SET impedance_slope = c.imp, impedance_slope_reverse = c.rs_imp
                FROM segment,
                LATERAL get_slope_profile(segment.geom, segment.length_m, ST_LENGTH(segment.geom)) s,
                LATERAL compute_impedances(s.elevs, s.linklength, s.lengthinterval) c
                WHERE sp.id = segment.id;
            """
            try:
                start_time = time.time()
                self.db_cursor.execute(sql_update_impedance)
                self.db_connection.commit()
                print(f"Thread {self.thread_id} updated impedance for H3 index {h3_short}. Time: {round(time.time() - start_time)} seconds.")
            except Exception as e:
                print_error(f"Thread {self.thread_id} failed to update impedances for H3 index {h3_short}, error: {e}.")
                break

            h3_short = self.get_next_h3_index()
