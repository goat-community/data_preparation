-- WIP this can be the basis for some new function

EXPLAIN ANALYZE 
WITH comparison AS 
(
	SELECT n.id, (ARRAY_AGG(old_id))[1] AS old_id, (ARRAY_AGG(similarity))[1] AS similarity
	FROM temporal.new_scraped_gps n  
	LEFT JOIN LATERAL 
	(	
		SELECT n2.id AS old_id, n1.names AS new_name, n2.names AS old_name, similarity(n1.names, n2.names)
		FROM 
		(
			SELECT string_agg(lower(name.name), '' ORDER BY lower(name.name)) AS names 
			FROM UNNEST(string_to_array(REPLACE(n.name, ' ', ''), '&')) name 
		) n1,
		(
			SELECT id, string_agg(lower(name), '' ORDER BY lower(name)) AS names
			FROM 
			(
				SELECT p.id, UNNEST(string_to_array(REPLACE(name, ' ', ''), '&')) AS name 
				FROM basic.poi p 
				WHERE ST_DWithin(n.geom, p.geom, 0.001)
				AND n.category = p.category 
			) y 
			GROUP BY id 
		) n2
		ORDER BY similarity(n1.names, n2.names)
		DESC
	) j ON TRUE 
	WHERE n.category = 'general_practitioner'
	GROUP BY n.id
)
SELECT n.*, c.similarity, c.old_id 
FROM temporal.new_scraped_gps n
LEFT JOIN comparison c 
ON c.id = n.id;



CREATE EXTENSION pg_trgm 

EXPLAIN ANALYZE 


DROP TABLE IF EXISTS temporal.poi_new;
CREATE TABLE temporal.poi_new AS 
SELECT *
FROM temporal.apotheke_de p; 
CREATE INDEX ON temporal.poi_new USING GIST(geom); 

DROP TABLE IF EXISTS temporal.poi_old;
CREATE TABLE temporal.poi_old AS 
SELECT *
FROM kart_poi_goat.poi p  
WHERE p.category = 'pharmacy'
AND p.SOURCE = 'Apotheken Umschau'; 
CREATE INDEX ON temporal.poi_old USING GIST(geom); 

DROP TABLE IF EXISTS temporal.comparison_poi; 
CREATE TABLE temporal.comparison_poi AS 
SELECT n.id, (ARRAY_AGG(old_id))[1] AS old_id, (ARRAY_AGG(similarity))[1] AS similarity
FROM temporal.poi_new n  
LEFT JOIN LATERAL 
(	
	SELECT y.id AS old_id, similarity(y.name, n.name)
	FROM 
	(
		SELECT p.auto_pk AS id, lower(name) AS name 
		FROM temporal.poi_old p 
		WHERE ST_DWithin(n.geom, p.geom, 0.0015)
	) y 
	ORDER BY similarity(y.name, n.name)
	DESC
) j ON TRUE 
GROUP BY n.id;
CREATE INDEX ON temporal.comparison_poi (id); 

DROP TABLE IF EXISTS temporal.poi_to_add;
CREATE TABLE temporal.poi_to_add AS 
SELECT n.*, c.similarity, c.old_id 
FROM temporal.poi_new n, temporal.comparison_poi c 
WHERE n.id = c.id 
AND c.similarity IS NULL;
CREATE INDEX ON temporal.poi_to_add (id); 

DROP TABLE IF EXISTS temporal.poi_to_update;
CREATE TABLE temporal.poi_to_update AS 
SELECT n.*, c.similarity, c.old_id 
FROM temporal.poi_new n, temporal.comparison_poi c 
WHERE n.id = c.id 
AND c.similarity > 0.2; 
CREATE INDEX ON temporal.poi_to_update (id); 

DROP TABLE IF EXISTS temporal.poi_to_delete;
CREATE TABLE temporal.poi_to_delete AS 

DROP TABLE temporal.test;
CREATE TABLE temporal.test AS 
WITH poi_matched AS 
(
	SELECT old_id FROM temporal.poi_to_update
	UNION ALL 
	SELECT old_id FROM temporal.poi_to_add
)
SELECT x.*
FROM temporal.poi_old x
WHERE x.auto_pk NOT IN (
	SELECT old_id FROM poi_matched WHERE old_id IS NOT NULL 
); 


SELECT count(*)
FROM temporal.poi_old 
WHERE auto_pk NOT IN (
	SELECT auto_pk
	FROM temporal.test 
)

SELECT count(*) FROM poi_matched 


SELECT count(*)
FROM temporal.poi_old x


SELECT count(x.*)
FROM temporal.poi_old x
WHERE x.auto_pk NOT IN (
	SELECT old_id FROM poi_matched
)

SELECT COUNT(*)
FROM temporal.poi_old 
GROUP BY auto_pk 
HAVING COUNT(*) > 1



LEFT JOIN (
	SELECT old_id FROM temporal.poi_to_update
	UNION ALL 
	SELECT old_id FROM temporal.poi_to_add
) j 
ON x.auto_pk = j.old_id 
WHERE j.old_id IS NULL; 



CREATE INDEX ON temporal.poi_to_delete (auto_pk); 
CREATE INDEX ON temporal.poi_to_delete USING GIST(geom);


WITH x AS 
(
	SELECT DISTINCT zipcode 
	FROM temporal.poi_to_delete
)
DELETE FROM temporal.apotheke_de a
USING x 
WHERE x.zipcode = a."addr:postcode"

ALTER TABLE temporal.pharmacy_rescraped DROP COLUMN id_0

SELECT max(id_0)
FROM temporal.apotheke_de


SELECT *
FROM kart_poi_goat.poi 
WHERE category = 'pharmacy'


SELECT n.*, c.similarity, c.old_id 
FROM temporal.apotheke_de n
LEFT JOIN comparison c 
ON c.id = n.id;




SELECT *
FROM temporal.apotheke_de ad 

ALTER TABLE temporal.apotheke_de ADD category TEXT; 


UPDATE temporal.apotheke_de
SET category = 'pharmacy'

