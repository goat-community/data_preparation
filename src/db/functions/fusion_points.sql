DROP FUNCTION IF EXISTS fusion_points;
/*
Table 1&2 input columns: name, geom, category, id
*/

/*
run prior to the code the poi table creation function -> to create two temp tables

*/

CREATE OR REPLACE FUNCTION fusion_points(table_1 text, table_2 text, fusion_radius float, fusion_threshold float, comparison_column_table_1 text, comparison_column_table_2 text)
RETURNS TABLE () /* add column to separate the table afterwards (matched, only in first, only in second) */
AS $function$
BEGIN
    DROP TABLE IF EXISTS temporal.comparison_poi; 
    EXECUTE
        'CREATE TABLE temporal.comparison_poi AS 
        SELECT n.id, (ARRAY_AGG(old_id))[1] AS old_id, (ARRAY_AGG(similarity))[1] AS similarity
        FROM ' || table_1 || ' n
        LEFT JOIN LATERAL 
        (	
            SELECT y.id AS old_id, similarity(y.name, n.'|| comparison_column_table_1 ||')
            FROM 
            (
                SELECT p.auto_pk AS id, lower('|| comparison_column_table_2 ||') AS name 
                FROM temporal.poi_old p 
                WHERE ST_DWithin(n.geom, p.geom, ' || fusion_radius || ')
            ) y 
            ORDER BY similarity(y.name, n.'|| comparison_column_table_1 ||')
            DESC
        ) j ON TRUE 
        GROUP BY n.id';

        CREATE INDEX ON temporal.comparison_poi (id); 
END;
$function$  LANGUAGE plpgsql;

/* 

Existing code
- creates copies of table_1 and table_2
- new table -> does map matching + string similarity based on name column (could be others id needed -> generic) of table_1 and table_2
- find pois that are only in table_2 and not in table_3 -> same id, but similarity IS NULL
- find pois that should be updated in table_1 -> same id, but similarity > 0.2 (or other threshold maybe based on input)
- find pois that are only in table_1 and not in table_3 -> based on id -> option if dropped or kept

Option1: should be to update table_1 (existing data) based on table_2 (new data) 
e.g. update our POIs
    -> drop data only in table_1
    -> keep data that is in both tables (without duplicates)
    -> add new data from table_2

Option2: enrich table_1 with table_2 
e.g. Fusion OSM and Overture where we have more confindence in OSM -> could add attributes or confidence
    -> keep data only in table_1
    -> keep data that is in both tables (without duplicates)
    -> drop new data from table_2

Option3: combine two tables without duplicates
e.g. combine OSM and Overture where we have the same confidence in both sources
    -> keep data only in table_1
    -> keep data that is in both tables (without duplicates)
    -> keep new data from table_2

*/ 