sql_queries = {
    "accidents": '''
        DROP TABLE IF EXISTS accidents;
        CREATE TABLE accidents as 
        SELECT a.* 
        FROM germany_accidents a, study_area s
        WHERE ST_Intersects(a.geom,s.geom)
    '''
    ,
    "landuse": '''
        DROP TABLE IF EXISTS landuse; 
        DO $$                  
        BEGIN 
                IF  ( SELECT count(*) 
                FROM landuse_atkis l, (SELECT ST_UNION(geom) AS geom FROM study_area) s
                WHERE ST_Intersects(l.geom, s.geom)
                ) = 0    
                THEN
                        CREATE TABLE landuse AS 
                        SELECT l.objart_txt::text AS landuse, l.geom 
                        FROM dlm250_polygon l, (SELECT ST_UNION(geom) AS geom FROM study_area) s
                        WHERE ST_Intersects(l.geom, s.geom)
                        AND objart_txt IN ('AX_Siedlungsflaeche','AX_FlaecheBesondererFunktionalerPraegung','AX_Friedhof','AX_IndustrieUndGewerbeflaeche','AX_Landwirtschaft',
                        'AX_Siedlungsflaeche','AX_SportFreizeitUndErholungsflaeche');
                ELSE
                        CREATE TABLE landuse AS 
                        SELECT l.objart_txt::text AS landuse, l.geom 
                        FROM landuse_atkis l, (SELECT ST_UNION(geom) AS geom FROM study_area) s
                        WHERE ST_Intersects(l.geom, s.geom);
                END IF;
        END
        $$ ;
        ALTER TABLE landuse ADD COLUMN gid serial;
        CREATE INDEX ON landuse(gid);
        CREATE INDEX ON landuse USING GIST(geom);''',
    "landuse_additional": '''DROP TABLE IF EXISTS landuse_additional;
            CREATE TABLE landuse_additional AS 
            SELECT u.class_2018::text AS landuse, u.geom  
            FROM urbanatlas u, (SELECT ST_UNION(geom) AS geom FROM study_area) s
            WHERE ST_Intersects(u.geom, s.geom)
            AND u.class_2018 NOT IN ('Fast transit roads and associated land', 'Other roads and associated land');
            ALTER TABLE landuse_additional ADD COLUMN gid serial;
            CREATE INDEX ON landuse_additional(gid);
            CREATE INDEX ON landuse_additional USING GIST(geom);''',
    "pois_custom_no_fusion": '''DROP TABLE IF EXISTS buffer_study_area;
        CREATE TEMP TABLE buffer_study_area AS 
        SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
        FROM study_area;
        
        DROP TABLE IF EXISTS pois_custom_no_fusion;
        CREATE TABLE pois_custom_no_fusion AS
        SELECT lower(j.school_t_1) as amenity, j.name, j.geom 
        FROM jedeschule j, buffer_study_area s
        WHERE j.school_t_1 IN ('Grundschule','Gymnasium','Realschule','Werkrealschule')
        AND ST_Intersects(j.geom,s.geom)
        AND NOT lower(name) ~~ ANY (ARRAY['%privat%','%priv.%','%montessori%','%waldorf%']); 

        INSERT INTO pois_custom_no_fusion
        SELECT 'grundschule' AS amenity, j.name, j.geom AS geom 
        FROM jedeschule j, buffer_study_area s 
        WHERE school_t_1 IN('Grund- und Teilhauptschule','Grund- und Hauptschule','Grund- und Mittelschule')
        AND ST_Intersects(j.geom,s.geom)
        AND NOT lower(name) ~~ ANY (ARRAY['%privat%','%priv.%','%montessori%','%waldorf%']);

        INSERT INTO pois_custom_no_fusion
        SELECT 'hauptschule_mittelschule' AS amenity, j.name, j.geom AS geom 
        FROM jedeschule j, buffer_study_area s  
        WHERE school_t_1 IN('Grund- und Teilhauptschule','Grund- und Hauptschule','Hauptschule','Mittelschule','Grund- und Mittelschule')
        AND ST_Intersects(j.geom,s.geom)
        AND NOT lower(name) ~~ ANY (ARRAY['%privat%','%priv.%','%montessori%','%waldorf%']);

        INSERT INTO pois_custom_no_fusion(name,amenity,geom)
        SELECT p.name, 'nursery' AS amenity, p.geom   
        FROM "Freiburg_pois_gp_bw" p, buffer_study_area s  
        WHERE poityp IN ('kita')
        AND ST_Intersects(p.geom,s.geom);
    ''',
    "buildings_custom": '''DROP TABLE IF EXISTS buildings_custom;
            CREATE TABLE buildings_custom AS 
            SELECT b.ags, (ST_DUMP(b.geom)).geom  
            FROM germany_buildings b, (SELECT ST_UNION(geom) AS geom FROM study_area) s
            WHERE ST_Intersects(b.geom, s.geom);
            ALTER TABLE buildings_custom ADD COLUMN gid serial;
            CREATE INDEX ON buildings_custom(gid);
            CREATE INDEX ON buildings_custom USING GIST(geom);''',
    "geographical_names": '''DROP TABLE IF EXISTS geographical_names;
            CREATE TABLE geographical_names AS 
            SELECT g.* 
            FROM germany_geographical_names_points g, study_area s 
            WHERE ST_Intersects(g.geom,s.geom);
            CREATE INDEX ON geographical_names(id);
            CREATE INDEX ON geographical_names USING GIST(geom);''',
    "census": '''DROP TABLE IF EXISTS grid;
            DROP TABLE IF EXISTS census;
            CREATE TEMP TABLE grid AS 
            SELECT DISTINCT g.id, g.geom
            FROM germany_grid_100_100 g, study_area s
            WHERE ST_Intersects(s.geom,g.geom);

            ALTER TABLE grid ADD PRIMARY KEY(id);

            CREATE TABLE census AS 
            WITH first_group AS 
            (
                SELECT g.id, REPLACE(merkmal,'"','') AS merkmal, jsonb_object(array_agg(c.auspraegung_text), array_agg(c.anzahl)::TEXT[]) AS demography
                FROM grid g, germany_census_demography_2011 c
                WHERE g.id = c.gitter_id_100m
                GROUP BY g.id, merkmal
            ),
            second_group AS 
            (
                SELECT id, jsonb_object_agg(merkmal, demography)::text AS demography 
                FROM first_group
                GROUP BY id
            )
            SELECT g.id, CASE WHEN f.id IS NULL THEN NULL ELSE demography::text END AS demography , g.geom
            FROM grid g 
            LEFT JOIN second_group f 
            ON g.id = f.id;

            ALTER TABLE census ADD COLUMN pop integer; 
            UPDATE census  
            SET pop = (demography::jsonb -> ' INSGESAMT' ->> 'Einheiten insgesamt')::integer;
            CREATE INDEX ON census(id);
            CREATE INDEX ON census USING GIST(geom);
            ''',
    "street_items": '''
            DROP TABLE IF EXISTS street_items;
            CREATE TABLE street_items AS 
            SELECT m.geom, m.KEY AS original_key, '{
                "object--bike-rack": "bicycle_parking",
                "object--street-light": "street_lamp",
                "marking--discrete--crosswalk-zebra": "zebra",
                "object--trash-can": "waste_basket",
                "object--bench":"bench"
             }'::jsonb ->> value AS amenity, 'mapillary' AS data_source    
             FROM mapillary_points m, study_area s 
             WHERE ST_Intersects(m.geom,s.geom);

             ALTER TABLE street_items ADD COLUMN gid serial;
             ALTER TABLE street_items ADD PRIMARY KEY(gid);
             CREATE INDEX ON street_items USING GIST(geom);''',
    "study_area": None

}


