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
    "buildings_osm": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DROP TABLE IF EXISTS temporal.buildings_osm;
            CREATE TABLE temporal.buildings_osm AS 
            SELECT b.* 
            FROM public.buildings_osm b, buffer_study_area s
            WHERE ST_Intersects(b.geom,s.geom);''',
    "pois": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DROP TABLE IF EXISTS temporal.pois;
            CREATE TABLE temporal.pois as 
            SELECT p.* 
            FROM public.pois_goat p, buffer_study_area s
            WHERE ST_Intersects(p.geom,s.geom);''',
    "planet_osm_points": '''DROP TABLE IF EXISTS buffer_study_area;
            CREATE TEMP TABLE buffer_study_area AS 
            SELECT ST_BUFFER(ST_UNION(geom), 0.027) AS geom 
            FROM temporal.study_area;

            DROP TABLE IF EXISTS temporal.planet_osm_points_p;
            CREATE TABLE temporal.planet_osm_points_p as 
            SELECT p.* 
            FROM public.planet_osm_points p, buffer_study_area s
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
            CREATE INDEX ON temporal.buildings_custom USING GIST(geom);''',
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


