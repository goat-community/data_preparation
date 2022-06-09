import json
import sys
sys.path.insert(0,"..")
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from config.config import Config
from db.db import Database

config_ways = Config("ways")
variable_container_ways = config_ways.preparation

class NetworkIslands:
	"""Class to compute to identify network islands from the ways table."""
	def __init__(self):
		self.db = Database()  

	def prepare_table(self):

		sql_create_table_no_islands = '''
			DROP TABLE IF EXISTS ways_no_islands; 
			CREATE TABLE ways_no_islands 
			(
				id bigint
			);
			ALTER TABLE ways_no_islands ADD PRIMARY KEY(id);
		'''
		self.db.perform(query = sql_create_table_no_islands)

	def find_connected_network(self):
		"""It finds connected network parts."""
		
		sql_first_starting_link = f'''
		DROP TABLE IF EXISTS starting_links; 
		CREATE TABLE starting_links AS
		SELECT w.id, w.source, w.target  
		FROM basic.edge w
		LEFT JOIN ways_no_islands n
		ON w.id = n.id
		WHERE w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_walking"]}))
		AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_cycling"]}))
		AND (
			(w.foot NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_foot"]})) OR foot IS NULL)
			OR
			(w.bicycle NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_bicycle"]})) OR bicycle IS NULL)
		)
		AND n.id IS NULL 
		LIMIT 1; 
		'''
		self.db.perform(query = sql_first_starting_link)

		cnt = 0 
		no_islands_empty = False
		while no_islands_empty == False: 
			cnt = cnt + 1 
			print(cnt)
			sql_get_neighbors = f''' 
			CREATE TABLE new_starting_links AS
			WITH to_filter AS 
			(
				SELECT DISTINCT w.id, w.source, w.target 
				FROM starting_links s, basic.edge w
				WHERE (s.source = w.target OR s.target = w.SOURCE OR s.source = w.source OR s.target = w.target)
				AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_walking"]}))
				AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_cycling"]}))
				AND (
					(w.foot NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_foot"]})) OR foot IS NULL)
					OR
					(w.bicycle NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_bicycle"]})) OR bicycle IS NULL)
				)
			)
			SELECT f.*
			FROM to_filter f
			LEFT JOIN ways_no_islands s 
			ON f.id = s.id
			WHERE s.id IS NULL;
			DROP TABLE IF EXISTS starting_links;
			ALTER TABLE new_starting_links RENAME TO starting_links; 
			ALTER TABLE starting_links ADD PRIMARY KEY(id);
			CREATE INDEX ON starting_links (source);
			CREATE INDEX ON starting_links (target);
			'''
			self.db.perform(query = sql_get_neighbors)

			sql_create_no_islands_insert = ''' 
			DROP TABLE IF EXISTS no_islands_insert;
			CREATE TABLE no_islands_insert AS
			SELECT s.id 
			FROM starting_links s
			LEFT JOIN ways_no_islands w
			ON w.id = s.id 
			WHERE w.id IS NULL; 
			'''
			self.db.perform(query = sql_create_no_islands_insert)

			sql_insert_no_islands = '''
			INSERT INTO ways_no_islands 
			SELECT id FROM no_islands_insert;
			'''
			self.db.perform(query = sql_insert_no_islands)

			sql_check_no_islands_empty = '''
			SELECT CASE 
			WHEN EXISTS (SELECT * FROM no_islands_insert LIMIT 1) THEN FALSE
			ELSE TRUE
			END
			'''
			
			no_islands_empty = self.db.select(sql_check_no_islands_empty)
			no_islands_empty = no_islands_empty[0][0]

	def find_network_islands(self):
		"""Will loop through the potential subnetwork and find all network islands."""
		self.prepare_table()
		self.find_connected_network()
		sql_classify_network_islands =  f"""
			DROP TABLE IF EXISTS network_islands;
			CREATE TABLE network_islands AS
			SELECT w.id
			FROM basic.edge w
			LEFT JOIN ways_no_islands n
			ON w.id = n.id
			WHERE n.id IS NULL
			AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_walking"]}))
			AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_cycling"]}))
			AND (w.foot NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_foot"]})) OR foot IS NULL); 
		
			ALTER TABLE network_islands ADD PRIMARY KEY(id);
			UPDATE basic.edge w SET class_id = 701
			FROM network_islands n
			WHERE w.id = n.id; 
		"""
		self.db.perform(sql_classify_network_islands)
