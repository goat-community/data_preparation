DROP FUNCTION IF EXISTS fusion_points();

CREATE OR REPLACE FUNCTION fusion_points(
    table_1 text,
    table_2 text,
    fusion_radius float,
    fusion_threshold float,
    comparison_column_table_1 text,
    comparison_column_table_2 TEXT,
    decision_table_1 TEXT,
    decision_fusion TEXT,
    decision_table_2 TEXT
)
RETURNS void
AS $function$
BEGIN
    DROP TABLE IF EXISTS temporal.comparison_poi; 
    EXECUTE '
        CREATE TABLE temporal.comparison_poi AS 
        SELECT n.*, (ARRAY_AGG(id_table_2))[1] AS id_table_2, (ARRAY_AGG(similarity))[1] AS similarity
        FROM ' || table_1 || ' n
        LEFT JOIN LATERAL 
        (   
            SELECT y.id AS id_table_2, similarity(lower(y.name), lower(n.'|| comparison_column_table_1 ||')) AS similarity
            FROM 
            (
                SELECT p.id AS id, lower('|| comparison_column_table_2 ||') AS name 
                FROM ' || table_2 || ' p 
                WHERE ST_DWithin(n.geom, p.geom, ' || fusion_radius || ')
            ) y 
			WHERE similarity(y.name, n.'|| comparison_column_table_1 ||') >= ' || fusion_threshold || '
            ORDER BY similarity(y.name, n.'|| comparison_column_table_1 ||')
            DESC
            LIMIT 1
        ) j ON TRUE 
        GROUP BY n.id
	';

    CREATE INDEX ON temporal.comparison_poi (id); 
   
	ALTER TABLE temporal.comparison_poi 
	ADD COLUMN decision varchar;

    -- Use separate IF and ELSIF statements for different decisions
    IF decision_table_1 = 'keep' THEN
        EXECUTE '
            UPDATE temporal.comparison_poi 
            SET decision = ''keep''
            WHERE id_table_2 IS NULL;
        ';
    ELSIF decision_table_1 = 'drop' THEN
        EXECUTE '
            DELETE FROM temporal.comparison_poi 
            WHERE id_table_2 IS NULL;
        ';
    END IF;

    IF decision_fusion = 'keep' THEN
        EXECUTE '
            UPDATE temporal.comparison_poi 
            SET decision = ''keep''
            WHERE similarity >= ' || fusion_threshold || ';
        ';
    ELSIF decision_fusion = 'combine' THEN
        EXECUTE '
	        UPDATE temporal.comparison_poi AS c
	        SET 
	            category_1 = COALESCE(t2.category_1, c.category_1),
	            category_2 = COALESCE(t2.category_2, c.category_2),
	            category_3 = COALESCE(t2.category_3, c.category_3),
	            category_4 = COALESCE(t2.category_4, c.category_4),
	            category_5 = COALESCE(t2.category_5, c.category_5),
	            "name" = COALESCE(t2."name", c."name"),
	            street = COALESCE(t2.street, c.street),
	            housenumber = COALESCE(t2.housenumber, c.housenumber),
	            zipcode = COALESCE(t2.zipcode, c.zipcode),
	            opening_hours = COALESCE(t2.opening_hours, c.opening_hours),
	            wheelchair = COALESCE(t2.wheelchair, c.wheelchair),
	            tags = t2.tags || c.tags, -- Merge JSONB tags
	            geom = CASE WHEN ST_X(t2.geom) IS NULL OR ST_Y(t2.geom) IS NULL THEN c.geom ELSE t2.geom END,
	            decision = ''combined''
	        FROM ' || table_2 || ' AS t2
	        WHERE c.id_table_2 = t2.id
			AND similarity >= ' || fusion_threshold || ';
        ';
    ELSIF decision_fusion = 'replace' THEN
        EXECUTE '
            UPDATE temporal.comparison_poi AS c
	        SET 
	            category_1 = t2.category_1,
	            category_2 = t2.category_2,
	            category_3 = t2.category_3,
	            category_4 = t2.category_4,
	            category_5 = t2.category_5,
	            "name" = t2."name",
	            street = t2.street,
	            housenumber = t2.housenumber,
	            zipcode = t2.zipcode,
	            opening_hours = t2.opening_hours,
	            wheelchair = t2.wheelchair,
	            tags = t2.tags,
	            geom = t2.geom,
	            decision = ''replaced''
	        FROM ' || table_2 || ' AS t2
	        WHERE c.id_table_2 = t2.id
			AND similarity >= ' || fusion_threshold || ';
        ';
    END IF;

    IF decision_table_2 = 'add' THEN
        EXECUTE '
            INSERT INTO temporal.comparison_poi
            SELECT *, id as id_table_2, NULL as similarity ,''add''::varchar AS decision
            FROM ' || table_2 || '
            WHERE id NOT IN (SELECT DISTINCT id_table_2 FROM temporal.comparison_poi WHERE id_table_2 IS NOT NULL);
        ';
    ELSIF decision_table_2 = 'drop' THEN
        -- Do nothing or add any necessary logic
        NULL;
    END IF;

END;
$function$ LANGUAGE plpgsql;
