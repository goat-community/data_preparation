sql_queries = {
    "accidents": '''
            DROP TABLE IF EXISTS temporal.accidents;
            CREATE TABLE temporal.accidents as 
            SELECT a.* 
            FROM public.germany_accidents a, temporal.study_area s
            WHERE ST_Intersects(a.geom,s.geom)
            AND (istrad = '1' OR istfuss = '1'); ''',
    "landuse": '''
            DROP TABLE IF EXISTS temporal.landuse; 
            DO $$                  
            BEGIN 
                    IF  ( SELECT count(*) 
                    FROM public.landuse_atkis l, (SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
                    WHERE ST_Intersects(l.geom, s.geom)
                    ) = 0    
                    THEN
                            CREATE TABLE temporal.landuse AS 
                            SELECT l.objart_txt::text AS landuse, l.geom 
                            FROM public.dlm250_polygon l, (SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
                            WHERE ST_Intersects(l.geom, s.geom)
                            AND objart_txt IN ('AX_Siedlungsflaeche','AX_FlaecheBesondererFunktionalerPraegung','AX_Friedhof','AX_IndustrieUndGewerbeflaeche','AX_Landwirtschaft',
                            'AX_Siedlungsflaeche','AX_SportFreizeitUndErholungsflaeche');
                    ELSE
                            CREATE TABLE temporal.landuse AS 
                            SELECT l.objart_txt::text AS landuse, l.geom 
                            FROM public.landuse_atkis l, (SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
                            WHERE ST_Intersects(l.geom, s.geom);
                    END IF;
            END
            $$ ;
            ALTER TABLE temporal.landuse ADD COLUMN gid serial;
            CREATE INDEX ON temporal.landuse(gid);
            CREATE INDEX ON temporal.landuse USING GIST(geom);''',
    "landuse_additional": '''DROP TABLE IF EXISTS temporal.landuse_additional;
            CREATE TABLE temporal.landuse_additional AS 
            SELECT u.class_2018::text AS landuse, u.geom  
            FROM public.urban_atlas u, (SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
            WHERE ST_Intersects(u.geom, s.geom)
            AND u.class_2018 NOT IN ('Fast transit roads and associated land', 'Other roads and associated land');
            ALTER TABLE temporal.landuse_additional ADD COLUMN gid serial;
            CREATE INDEX ON temporal.landuse_additional(gid);
            CREATE INDEX ON temporal.landuse_additional USING GIST(geom);''',
    "landuse_osm": '''DROP TABLE IF EXISTS temporal.landuse_osm;
            CREATE TABLE temporal.landuse_osm AS 
            SELECT u.* 
            FROM public.landuse_osm u, (SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
            WHERE ST_Intersects(u.geom, s.geom);
            ALTER TABLE temporal.landuse_osm ADD COLUMN gid serial;
            CREATE INDEX ON temporal.landuse_osm(gid);
            CREATE INDEX ON temporal.landuse_osm USING GIST(geom);''',
    "buildings_osm": '''
            DROP TABLE IF EXISTS temporal.buildings_osm;
            CREATE TABLE temporal.buildings_osm AS 
            SELECT b.* 
            FROM public.buildings_osm b, (SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
            WHERE ST_Intersects(b.geom,s.geom);
            CREATE INDEX ON temporal.buildings_osm(osm_id);
            CREATE INDEX ON temporal.buildings_osm USING GIST(geom);
            
                DROP TABLE IF EXISTS temporal.buildings_osm_board;
                CREATE TABLE temporal.buildings_osm_board AS 
                WITH coverage_perc AS (
                SELECT round((ST_Area(ST_Intersection(bo.geom,s.geom))/ST_Area(bo.geom))::numeric,4) AS perc, osm_id 
                FROM temporal.buildings_osm bo,(SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
                )
                SELECT cp.perc, bo.osm_id, bo.geom 
                FROM temporal.buildings_osm bo, coverage_perc cp
                WHERE cp.perc <= 0.5
                --AND cp.perc > 0.25
                AND cp.osm_id = bo.osm_id;

                WITH shares_buildings AS (
                SELECT round((ST_Area(ST_Intersection(bob.geom,s.geom))/ST_Area(bob.geom))::numeric,4) AS shares, s.rs, bob.osm_id
                FROM temporal.buildings_osm_board bob, sub_study_area s
                WHERE ST_intersects(bob.geom,s.geom)
                ),
                rs_code AS (
                SELECT DISTINCT rs FROM temporal.study_area
                )
                DELETE FROM temporal.buildings_osm
                WHERE osm_id IN (
                                SELECT osm_id
                                FROM shares_buildings sb, rs_code rc
                                WHERE shares = (SELECT max(shares) 
                                                FROM shares_buildings
                                                WHERE osm_id = sb.osm_id)
                                AND sb.rs != rc.rs);

                DROP TABLE IF EXISTS temporal.buildings_osm_board; ''',
    "pois": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DO $$
                DECLARE
                        rec record; 
                        total_cnt integer;
                BEGIN 
                        
                        DROP TABLE IF EXISTS count_pois;
                        CREATE TEMP TABLE count_pois 
                        (
                                cnt integer,
                                starting_uid TEXT,
                                uid TEXT,
                                category TEXT,
                                geom geometry,
                                test float
                        ); 
                        ALTER TABLE count_pois ADD PRIMARY KEY(uid);	
                        
                        DROP TABLE IF EXISTS intersecting_pois;
                        CREATE TEMP TABLE intersecting_pois (starting_uid TEXT, uid TEXT, category TEXT, geom geometry);
                        FOR rec IN SELECT * FROM public.poi p, buffer_study_area s WHERE ST_Intersects(p.geom,s.geom) 
                        LOOP
                                IF (SELECT count(*) FROM count_pois WHERE uid = rec.uid) = 0 THEN  
                                        
                                        TRUNCATE intersecting_pois; 
                                        INSERT INTO intersecting_pois
                                        SELECT p.*
                                        FROM
                                        (
                                                SELECT rec.uid starting_uid, p.uid, p.category, p.geom
                                                FROM public.poi p
                                                WHERE ST_Intersects(p.geom, ST_BUFFER(rec.geom, 0.00000001))
                                                AND p.uid <> rec.uid
                                                UNION ALL 
                                                SELECT rec.uid, rec.uid, rec.category, rec.geom	
                                        ) p
                                        ORDER BY p.starting_uid, p.category;  
                                        total_cnt = (SELECT count(*) FROM intersecting_pois); 
                                        
                                        IF total_cnt > 1 THEN 
                                                INSERT INTO count_pois 
                                                SELECT ROW_NUMBER() OVER() cnt, o.starting_uid, o.uid, o.category, 
                                                ST_PROJECT(geom::geography, 3, radians(360 * ((ROW_NUMBER() OVER())::float / total_cnt::float)))::geometry 
                                                FROM intersecting_pois o;
                                        END IF; 
                                
                                END IF; 
                        END LOOP;
                        DROP TABLE IF EXISTS temporal.poi;
                        CREATE TABLE temporal.poi AS 
                        SELECT p.id, p.category, p.name, p.street, p.housenumber, p.zipcode, p.opening_hours, p.wheelchair, p.tags, c.geom, p.uid
                        FROM public.poi p, buffer_study_area s, count_pois c  
                        WHERE ST_Intersects(p.geom,s.geom) 
                        AND p.uid = c.uid 
                        UNION ALL 
                        SELECT p.*
                        FROM 
                        (
                                SELECT p.id, p.category, p.name, p.street, p.housenumber, p.zipcode, p.opening_hours, p.wheelchair, p.tags, p.geom, p.uid
                                FROM public.poi p, buffer_study_area s
                                WHERE ST_Intersects(p.geom,s.geom) 
                        ) p
                        LEFT JOIN count_pois c
                        ON p.uid = c.uid 
                        WHERE c.uid IS NULL; 
	        END$$;''',
    "planet_osm_point": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DROP TABLE IF EXISTS temporal.planet_osm_point;
            CREATE TABLE temporal.planet_osm_point as 
            SELECT p.* 
            FROM public.planet_osm_point p, buffer_study_area s
            WHERE ST_Intersects(p.way,s.geom);''',
    "ways": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DROP TABLE IF EXISTS temporal.ways;
            CREATE TABLE temporal.ways AS 
            SELECT w.* 
            FROM ways w, buffer_study_area sa 
            WHERE ST_Intersects(sa.geom,w.geom);

            DROP TABLE IF EXISTS temporal.ways_vertices_pgr;
            CREATE TABLE temporal.ways_vertices_pgr AS 
            SELECT w.* 
            FROM ways_vertices_pgr w, temporal.ways wa
            WHERE w.id = wa.source
            OR w.id = wa.target;''',
    "aoi": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DROP TABLE IF EXISTS temporal.aoi;
            CREATE TABLE temporal.aoi AS 
            SELECT ua.objart_txt as category, ua.geom 
            FROM public.landuse_atkis ua, buffer_study_area s
            WHERE ST_Intersects(ua.geom,s.geom)
            AND (objart_txt = 'AX_Wald'
            OR objart_txt = 'AX_SportFreizeitUndErholungsflaeche'
            OR objart_txt = 'AX_Landwirtschaft'
            OR objart_txt = 'AX_Gehoelz'
            );

            UPDATE temporal.aoi
            SET category = 'forest'
            WHERE category = 'AX_Wald';

            UPDATE temporal.aoi
            SET category = 'park'
            WHERE category = 'AX_SportFreizeitUndErholungsflaeche';

            UPDATE temporal.aoi
            SET category = 'field'
            WHERE category = 'AX_Landwirtschaft';

            UPDATE temporal.aoi
            SET category = 'heath_scrub'
            WHERE category = 'AX_Gehoelz';''',
    "aoi_freiburg": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DROP TABLE IF EXISTS temporal.aoi;
            CREATE TABLE temporal.aoi AS 
            SELECT ua.class_2018 as category, ua.geom 
            FROM public.urban_atlas ua, buffer_study_area s
            WHERE ST_Intersects(ua.geom,s.geom)
            AND (class_2018 = 'Forests'
            OR class_2018 = 'Green urban areas');

            UPDATE temporal.aoi
            SET category = 'forest'
            WHERE category = 'Forests';

            UPDATE temporal.aoi
            SET category = 'park'
            WHERE category = 'Green urban areas';''',
    "buildings_custom": '''DROP TABLE IF EXISTS temporal.buildings_custom;
            CREATE TABLE temporal.buildings_custom AS 
            SELECT b.ags, (ST_DUMP(b.geom)).geom, b.height, b.residential_status 
            FROM public.germany_buildings b, (SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
            WHERE ST_Intersects(b.geom, s.geom);
            ALTER TABLE temporal.buildings_custom ADD COLUMN gid serial;
            CREATE INDEX ON temporal.buildings_custom(gid);
            CREATE INDEX ON temporal.buildings_custom USING GIST(geom);
            
            DROP TABLE IF EXISTS temporal.buildings_custom_board;
            CREATE TABLE temporal.buildings_custom_board AS 
            WITH coverage_perc AS (
                SELECT round((ST_Area(ST_Intersection(bc.geom,s.geom))/ST_Area(bc.geom))::numeric,4) AS perc, gid 
                FROM temporal.buildings_custom bc,(SELECT ST_UNION(geom) AS geom FROM temporal.study_area) s
                )
                SELECT cp.perc, bc.geom, bc.gid 
                FROM temporal.buildings_custom bc, coverage_perc cp
                WHERE cp.perc <= 0.5
                --AND cp.perc > 0.25
                AND cp.gid = bc.gid;

            WITH shares_buildings AS (
                SELECT round((ST_Area(ST_Intersection(bcb.geom,s.geom))/ST_Area(bcb.geom))::numeric,4) AS shares, s.rs, bcb.gid
                FROM temporal.buildings_custom_board bcb, sub_study_area s
                WHERE ST_intersects(bcb.geom,s.geom)
                ),
                rs_code AS (
                SELECT DISTINCT rs FROM temporal.study_area
                )
                DELETE FROM temporal.buildings_custom
                WHERE gid IN (
                                SELECT gid
                                FROM shares_buildings sb, rs_code rc
                                WHERE shares = (SELECT max(shares) 
                                                FROM shares_buildings
                                                WHERE gid = sb.gid)
                                AND sb.rs != rc.rs);

            DROP TABLE IF EXISTS temporal.buildings_custom_board;''',
    "geographical_names": '''DROP TABLE IF EXISTS temporal.geographical_names;
            CREATE TABLE temporal.geographical_names AS 
            SELECT g.* 
            FROM public.germany_geographical_names_points g, temporal.study_area s 
            WHERE ST_Intersects(g.geom,s.geom);
            CREATE INDEX ON temporal.geographical_names(id);
            CREATE INDEX ON temporal.geographical_names USING GIST(geom);''',
    "census": '''DROP TABLE IF EXISTS grid;
            DROP TABLE IF EXISTS temporal.census;
            CREATE TEMP TABLE grid AS 
            SELECT DISTINCT g.id, g.geom
            FROM public.germany_grid_100_100 g, temporal.study_area s
            WHERE ST_Intersects(s.geom,g.geom);

            ALTER TABLE grid ADD PRIMARY KEY(id);

            CREATE TABLE temporal.census AS 
            WITH first_group AS 
            (
                SELECT g.id, REPLACE(merkmal,'"','') AS merkmal, jsonb_object(array_agg(c.auspraegung_text), array_agg(c.anzahl)::TEXT[]) AS demography
                FROM grid g, public.germany_census_demography_2011 c
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

            ALTER TABLE temporal.census ADD COLUMN pop integer; 
            UPDATE temporal.census  
            SET pop = (demography::jsonb -> ' INSGESAMT' ->> 'Einheiten insgesamt')::integer;
            CREATE INDEX ON temporal.census(id);
            CREATE INDEX ON temporal.census USING GIST(geom);
            ''',
    "study_area": None

}


