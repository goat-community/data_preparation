import json
import sys
import os
import subprocess
from src.config.config import Config
from src.db.config import DATABASE
from src.db.db import Database


config_ways = Config("ways")
variable_container_ways = config_ways.preparation


class FirstNetworkPreparation:
	"""Class to compute to identify network islands from the ways table."""
	def __init__(self, db):
		self.db = db  
		self.root_dir = os.path.abspath(os.curdir)
		self.dbname, self.host, self.username, self.port = DATABASE['dbname'], DATABASE['host'], DATABASE['user'], DATABASE['port']


	def create_table_schemas(self):
		self.db.perform(query = "CREATE SCHEMA IF NOT EXISTS basic;")
		# Restore node table
		self.db.perform(query = "DROP TABLE IF EXISTS basic.node;")
		subprocess.run(f'PGPASSFILE=~/.pgpass_{self.dbname} pg_restore -U {self.username} --schema-only -h {self.host} -n basic -d {self.dbname} -t node {self.root_dir + "/src/data/input/dump.tar"}', shell=True, check=True)
		# Restore node table
		self.db.perform(query = "DROP TABLE IF EXISTS basic.edge;")
		subprocess.run(f'PGPASSFILE=~/.pgpass_{self.dbname} pg_restore -U {self.username} --schema-only -h {self.host} -n basic -d {self.dbname} -t edge {self.root_dir + "/src/data/input/dump.tar"}', shell=True, check=True)

	def create_street_crossings(self):
		sql_street_crossings = '''
			--Create table that stores all street crossings
			DROP TABLE IF EXISTS street_crossings;
			CREATE TABLE street_crossings AS 
			SELECT osm_id, NULL as key, highway,
			CASE WHEN (tags -> 'crossing_ref') IS NOT NULL THEN (tags -> 'crossing_ref') ELSE (tags -> 'crossing') END AS crossing, 
			(tags -> 'traffic_signals') AS traffic_signals, (tags -> 'kerb') AS kerb, 
			(tags -> 'segregated') AS segregated, (tags -> 'supervised') AS supervised, 
			(tags -> 'tactile_paving') AS tactile_paving, (tags -> 'wheelchair') AS wheelchair, way AS geom, 'osm' as source
			FROM planet_osm_point p
			WHERE (tags -> 'crossing') IS NOT NULL 
			OR highway IN('crossing','traffic_signals')
			OR (tags -> 'traffic_signals') = 'pedestrian_crossing';

			ALTER TABLE street_crossings ADD COLUMN gid serial;
			ALTER TABLE street_crossings ADD PRIMARY key(gid);

			UPDATE street_crossings 
			SET crossing = 'traffic_signals'
			WHERE traffic_signals = 'crossing' 
			OR traffic_signals = 'pedestrian_crossing';

			UPDATE street_crossings 
			SET crossing = highway 
			WHERE crossing IS NULL AND highway IS NOT NULL; 

			CREATE INDEX ON street_crossings USING GIST(geom);
		'''
		self.db.perform(query = sql_street_crossings)

db = Database()
preparation = FirstNetworkPreparation(db)
preparation.create_table_schemas()
db.conn.close()


network_preparation1 = f'''

UPDATE ways_vertices_pgr v SET cnt = y.cnt
FROM (
	SELECT SOURCE, count(source) cnt 
	FROM 
	(
		SELECT SOURCE FROM ways 
		UNION ALL
		SELECT target FROM ways 
	) x 
	GROUP BY SOURCE 
) y
WHERE v.id = y.SOURCE;



WITH ways_attributes AS (
	SELECT vv.id, array_remove(array_agg(DISTINCT x.tag_id),NULL) tag_ids,
	array_remove(array_agg(DISTINCT x.foot),NULL) AS foot,
	array_remove(array_agg(DISTINCT x.bicycle),NULL) bicycle,
	array_remove(array_agg(DISTINCT x.lit_classified),NULL) lit_classified,
	array_remove(array_agg(DISTINCT x.wheelchair_classified),NULL) wheelchair_classified
	FROM ways_vertices_pgr vv
	LEFT JOIN
	(	SELECT v.id, w.tag_id, w.foot, w.bicycle, w.lit_classified, w.wheelchair_classified 
		FROM ways_vertices_pgr v, ways w 
		WHERE st_intersects(v.geom,w.geom)
	) x
	ON vv.id = x.id
	GROUP BY vv.id
)
UPDATE ways_vertices_pgr v
SET tag_ids = w.tag_ids, 
foot = w.foot,
bicycle = w.bicycle,
lit_classified = w.lit_classified,
wheelchair_classified  = w.wheelchair_classified
FROM ways_attributes w
WHERE v.id = w.id;

-- Mark dedicated lanes as foot = 'no'
UPDATE ways w
SET foot = 'no'  FROM ( 
	SELECT osm_id 
	FROM planet_osm_line 
	WHERE highway = 'service' 
	AND (tags->'psv' IS NOT NULL OR tags->'bus' = 'yes') 
) x 
WHERE w.osm_id = x.osm_id;

'''