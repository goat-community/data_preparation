# This script is classifying the buildings using a maximum of three landuse layers. The default landuse layer is OSM.
# Furthermore the user can apply to additional landuse filters using the layers landuse and landuse_additional.
# In this current implementation priority is given to the landuse_osm and landuse layers, while landuse_additional is only applied if 
# none of the other landuse layers intersect.

import sys, os
# sys.path.insert(0,"..")
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,parentdir)
from config.config import Config

variable_container_population = Config("population").preparation

classify_buildings = f'''

UPDATE buildings 
SET residential_status = 'potential_residents'
WHERE residential_status IS NULL; 


DROP TABLE IF EXISTS buildings_classification;
CREATE TABLE buildings_classification 
(
	gid integer, 
	landuse_residential_status integer[],
	landuse_gids integer[],
	landuse_additional_residential_status integer[],
	landuse_additional_gids integer[],
	landuse_osm_residential_status integer[],
	landuse_osm_gids integer[]
);

UPDATE buildings 
SET area = ST_AREA(geom::geography);

UPDATE buildings
SET residential_status = 'no_residents'
WHERE buildings.area < (SELECT {variable_container_population['minimum_building_size_residential']}::integer)
AND residential_status = 'potential_residents';

INSERT INTO buildings_classification 
SELECT gid 
FROM buildings
WHERE residential_status = 'potential_residents'; 

ALTER TABLE buildings_classification ADD PRIMARY KEY(gid);
'''

intersect_landuse = f'''
DO $$
	DECLARE 
    	categories_no_residents TEXT[] := ARRAY{variable_container_population['custom_landuse_no_residents']};
		categories_potential_residents TEXT[] := ARRAY{variable_container_population['custom_landuse_potential_residents']};
    BEGIN 
        IF EXISTS
            ( SELECT 1
              FROM   information_schema.tables 
              WHERE  table_schema = 'temporal'
              AND    table_name = 'landuse'
            )
        THEN
        	
        	DROP TABLE IF EXISTS landuse_subdivide; 
        	CREATE TABLE landuse_subdivide AS 
        	SELECT landuse::text, ST_SUBDIVIDE(geom, 200) AS geom
        	FROM temporal.landuse; 
        
        	ALTER TABLE landuse_subdivide ADD COLUMN gid serial;
        	ALTER TABLE landuse_subdivide ADD PRIMARY KEY(gid);
    		CREATE INDEX ON landuse_subdivide USING GIST(geom);
     		
    		DROP TABLE IF EXISTS buildings_landuse;
        	CREATE TABLE buildings_landuse AS 
        	WITH buildings_intersects AS 
        	(
				SELECT b.gid, CASE WHEN l.landuse IS NULL THEN 2 
				WHEN (categories_potential_residents && ARRAY[l.landuse]) IS TRUE THEN 2 
				ELSE (categories_no_residents && ARRAY[l.landuse])::integer END AS residential_status, l.gid landuse_gid
				FROM (
					SELECT * 
					FROM buildings 
					WHERE residential_status = 'potential_residents'
				) b
				LEFT JOIN landuse_subdivide l
				ON ST_INTERSECTS(l.geom, b.geom)
			) 
			SELECT gid, ARRAY_AGG(residential_status) AS landuse_residential_status, ARRAY_AGG(landuse_gid) AS landuse_gids			
			FROM buildings_intersects
			GROUP BY gid; 
			
			ALTER TABLE buildings_landuse ADD PRIMARY KEY(gid);

			UPDATE buildings_classification c
			SET landuse_residential_status = l.landuse_residential_status,
			landuse_gids = l.landuse_gids
			FROM buildings_landuse l
			WHERE c.gid = l.gid; 
		
        END IF ;
    END
$$ ;
'''

intersect_landuse_additional = f'''
DO $$                  
    DECLARE 
    	categories_no_residents TEXT[] := ARRAY{variable_container_population['custom_landuse_additional_no_residents']};
		categories_potential_residents TEXT[] := ARRAY{variable_container_population['custom_landuse_additional_potential_residents']};
	BEGIN 
        IF EXISTS
            ( SELECT 1
              FROM   information_schema.tables 
              WHERE  table_schema = 'temporal'
              AND    table_name = 'landuse_additional'
            )
        THEN     
        	DROP TABLE IF EXISTS landuse_additional_subdivide ; 
        	CREATE TABLE landuse_additional_subdivide  AS 
        	SELECT landuse::text, ST_SUBDIVIDE(geom, 200) AS geom
        	FROM temporal.landuse_additional; 
        
        	ALTER TABLE landuse_additional_subdivide ADD COLUMN gid serial;
        	ALTER TABLE landuse_additional_subdivide  ADD PRIMARY KEY(gid);
    		CREATE INDEX ON landuse_additional_subdivide  USING GIST(geom);
     		
    		DROP TABLE IF EXISTS buildings_landuse;
        	CREATE TABLE buildings_landuse AS 
        	WITH buildings_intersects AS 
        	(
				SELECT b.gid, CASE WHEN l.landuse IS NULL THEN 2 
				WHEN (categories_potential_residents && ARRAY[l.landuse]) IS TRUE THEN 2 
				ELSE (categories_no_residents && ARRAY[l.landuse])::integer END AS residential_status, l.gid landuse_gid
				FROM (
					SELECT * 
					FROM buildings 
					WHERE residential_status = 'potential_residents'
				) b 
				LEFT JOIN landuse_additional_subdivide l
				ON ST_INTERSECTS(l.geom, b.geom)
			) 
			SELECT gid, ARRAY_AGG(residential_status) AS landuse_residential_status, ARRAY_AGG(landuse_gid) AS landuse_gids		
			FROM buildings_intersects
			GROUP BY gid; 
			
			ALTER TABLE buildings_landuse ADD PRIMARY KEY(gid);
		
			UPDATE buildings_classification c
			SET landuse_additional_residential_status = l.landuse_residential_status,
			landuse_additional_gids = l.landuse_gids
			FROM buildings_landuse l
			WHERE c.gid = l.gid; 
        END IF ;
    END
$$ ;
'''

intersect_osm_landuse = f'''
DO $$
	DECLARE 
    	categories_no_residents TEXT[] := ARRAY{variable_container_population['osm_landuse_no_residents']}
    		|| ARRAY{variable_container_population['tourism_no_residents']}
    		|| ARRAY{variable_container_population['amenity_no_residents']};
    BEGIN 
        IF EXISTS
            ( SELECT 1
              FROM   information_schema.tables 
              WHERE  table_schema = 'temporal'
              AND    table_name = 'landuse_osm'
            )
        THEN
			
			DROP TABLE IF EXISTS landuse_osm_subdivide ; 
        	CREATE TABLE landuse_osm_subdivide AS 
			SELECT landuse::text, ST_SUBDIVIDE(geom, 200) AS geom
			FROM temporal.landuse_osm l
			WHERE landuse IS NOT NULL;
	
        	ALTER TABLE landuse_osm_subdivide ADD COLUMN gid serial;
        	ALTER TABLE landuse_osm_subdivide  ADD PRIMARY KEY(gid);
    		CREATE INDEX ON landuse_osm_subdivide  USING GIST(geom);
     		
    		DROP TABLE IF EXISTS buildings_landuse;
        	CREATE TABLE buildings_landuse AS 
        	WITH buildings_intersects AS 
        	(
				SELECT b.gid, CASE WHEN l.landuse IS NULL THEN 2 
				ELSE (categories_no_residents && ARRAY[l.landuse])::integer END AS residential_status, l.gid landuse_gid
				FROM (
					SELECT * 
					FROM buildings 
					WHERE residential_status = 'potential_residents'
				) b
				LEFT JOIN landuse_osm_subdivide l
				ON ST_INTERSECTS(l.geom, b.geom)
			) 
			SELECT gid, ARRAY_AGG(residential_status) AS landuse_residential_status, ARRAY_AGG(landuse_gid) AS landuse_gids		
			FROM buildings_intersects
			GROUP BY gid; 
			
			ALTER TABLE buildings_landuse ADD PRIMARY KEY(gid);

			UPDATE buildings_classification c
			SET landuse_osm_residential_status = l.landuse_residential_status,
			landuse_osm_gids = l.landuse_gids
			FROM buildings_landuse l
			WHERE c.gid = l.gid; 
		
		
        END IF ;
    END
$$ ;
'''
finalized_classification = f'''
CREATE INDEX ON buildings_classification USING gin (landuse_residential_status gin__int_ops);
CREATE INDEX ON buildings_classification USING gin (landuse_additional_residential_status gin__int_ops);
CREATE INDEX ON buildings_classification USING gin (landuse_osm_residential_status gin__int_ops);

DROP TABLE IF EXISTS buildings_to_update;
CREATE TABLE buildings_to_update (gid integer, residential_status text);
ALTER TABLE buildings_to_update ADD PRIMARY KEY(gid);

INSERT INTO buildings_to_update
SELECT b.gid, 'with_residents'
FROM buildings_classification c, buildings b
WHERE c.landuse_residential_status = ARRAY[0]
AND (c.landuse_additional_residential_status = ARRAY[0] OR c.landuse_additional_residential_status IS NULL)
AND (c.landuse_osm_residential_status = ARRAY[0] OR c.landuse_osm_residential_status IS NULL)
AND c.gid = b.gid; 

INSERT INTO buildings_to_update 
SELECT b.gid, 'no_residents'
FROM buildings_classification c, buildings b
WHERE c.landuse_residential_status = ARRAY[1]
AND (c.landuse_additional_residential_status = ARRAY[1] OR c.landuse_additional_residential_status IS NULL)
AND (c.landuse_osm_residential_status = ARRAY[1] OR c.landuse_osm_residential_status IS NULL)
AND c.gid = b.gid; 

INSERT INTO buildings_to_update 
SELECT b.gid, 'no_residents'
FROM buildings_classification c, buildings b
WHERE c.landuse_residential_status = ARRAY[2]
AND (c.landuse_additional_residential_status = ARRAY[2] OR c.landuse_additional_residential_status IS NULL)
AND (c.landuse_osm_residential_status = ARRAY[2] OR c.landuse_additional_residential_status IS NULL)
AND c.gid = b.gid; 

INSERT INTO buildings_to_update
WITH classification AS 
(
	SELECT c.gid, jsonb_build_object('categorization',c.landuse_residential_status,'landuse_gid', c.landuse_gids, 'table','landuse') AS landuse
	FROM buildings_classification c
	LEFT JOIN buildings_to_update u 
	ON c.gid = u.gid 
	WHERE u.gid IS NULL 
	AND c.landuse_residential_status IS NOT NULL 
	UNION ALL 
	SELECT c.gid, jsonb_build_object('categorization',c.landuse_osm_residential_status,'landuse_osm_gid', c.landuse_osm_gids, 'table','landuse_osm') landuse_osm
	FROM buildings_classification c
	LEFT JOIN buildings_to_update u 
	ON c.gid = u.gid 
	WHERE u.gid IS NULL 
	AND c.landuse_osm_residential_status IS NOT NULL 
	UNION ALL 
	SELECT c.gid, jsonb_build_object('categorization',c.landuse_additional_residential_status,'landuse_additional_gid', c.landuse_additional_gids, 'table','landuse_additional') landuse_additional
	FROM buildings_classification c
	LEFT JOIN buildings_to_update u 
	ON c.gid = u.gid 
	WHERE u.gid IS NULL 
	AND c.landuse_additional_residential_status IS NOT NULL 
)
SELECT c.gid, CASE WHEN classify_building(c.gid, jsonb_agg(c.landuse)) = 0 THEN 'with_residents' 
WHEN classify_building(c.gid, jsonb_agg(c.landuse)) = 2 THEN 'potential_residents'
ELSE 'no_residents' END AS residential_status 
FROM classification c
GROUP BY c.gid; 
'''

buildings_update = f'''
UPDATE buildings b 
SET residential_status = u.residential_status
FROM buildings_to_update u 
WHERE b.gid = u.gid; 

--Substract one level when POI on building (more classification has to be done in the future)

WITH x AS (
    SELECT DISTINCT b.gid
    FROM buildings b, poi_prepared_goat p 
    WHERE st_intersects(b.geom,p.geom)
)
UPDATE buildings b
SET building_levels_residential = building_levels - 1
FROM x
WHERE b.gid = x.gid;

UPDATE buildings 
set building_levels_residential = building_levels
WHERE building_levels_residential IS NULL;

--UPDATE buildings 
--SET residential_status = 'with_residents'
--WHERE residential_status = 'potential_residents';

UPDATE buildings 
SET gross_floor_area_residential = (building_levels_residential * area) + (roof_levels/2) * area 
WHERE residential_status = 'with_residents';

'''
