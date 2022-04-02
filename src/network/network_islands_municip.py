import json
import sys
sys.path.insert(0,"..")
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
from config.config import Config

config_ways = Config("ways")
variable_container_ways = config_ways.preparation


network_islands = f'''

DROP TABLE study_area_buffer; 
CREATE TABLE study_area_buffer AS
SELECT st_buffer(geom::geography, 2500)::geometry AS geom, id
FROM basic.study_area sa
WHERE id = {municip}; --91620000
CREATE INDEX ON study_area_buffer USING GIST(geom);

--Mark network islands in the network
--Update  "configuration" set class_id = 701, class_value = 'network_island' WHERE NOT EXISTS (Select class_id, class_value From "configuration" where class_id = 701 and class_value = 'network_island');
INSERT INTO "configuration" (class_id, class_value) VALUES (701, 'network_island');

DROP TABLE IF EXISTS network_islands; 
CREATE TABLE network_islands AS 
WITH RECURSIVE ways_no_islands AS (
	SELECT id, geom FROM 
	(SELECT w.id,w.geom
	FROM ways w, study_area_buffer b
	WHERE w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_walking"]}))
	AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_cycling"]}))
	AND ST_Intersects(w.geom, b.geom)
	AND (
	(w.foot NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_foot"]})) OR foot IS NULL)
	OR
	(w.bicycle NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_bicycle"]})) OR bicycle IS NULL))
	LIMIT 1) x
	UNION 
	SELECT w.id,w.geom
	FROM ways w, ways_no_islands n, study_area_buffer b
	WHERE ST_Intersects(n.geom,w.geom)
	AND ST_Intersects(w.geom, b.geom)
	AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_walking"]}))
	AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_cycling"]}))
	AND (w.foot NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_foot"]})) OR foot IS NULL)
) 
SELECT w.id  
FROM (
	SELECT w.id
	FROM 
	(
		SELECT wa.id 
		FROM study_area_buffer b, ways wa
		WHERE ST_Intersects(wa.geom, b.geom)	
	) w
	LEFT JOIN ways_no_islands n
	ON w.id = n.id
	WHERE n.id IS NULL
) x, ways w
WHERE w.id = x.id
AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_walking"]}))
AND w.class_id NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["excluded_class_id_cycling"]}))
AND (w.foot NOT IN (SELECT UNNEST(ARRAY{variable_container_ways["categories_no_foot"]})) OR foot IS NULL); 

ALTER TABLE network_islands ADD PRIMARY KEY(id);

UPDATE ways w SET class_id = 701
FROM network_islands n
WHERE w.id = n.id; 

'''