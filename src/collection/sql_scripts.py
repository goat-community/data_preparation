sql_queries_goat = {
    "pois_update": '''
        -- ##################################################################################  --
        -- ## TEMPORAL FIX FOR poiS TABLE #############################################  --

        DROP TABLE IF EXISTS temp_p;
        CREATE TABLE temp_p as
        SELECT  poi_goat_id,
                jsonb_build_object('addr:country',"addr:country") AS "addr:country" , 
                jsonb_build_object('addr:city',"addr:city") AS "addr:city" , 
                jsonb_build_object('website', website) as website, 
                jsonb_build_object('source', "source") AS "source",
                jsonb_build_object('brand', brand) AS "brand",
                jsonb_build_object('operator', "operator") AS "operator",
                jsonb_build_object('origin_geometry', origin_geometry) AS origin_geometry ,
                jsonb_build_object('phone', phone) AS "phone",
                jsonb_build_object('osm_id', osm_id) AS "osm_id",
                tags 
        FROM pois_upload pf; 

        UPDATE temp_p 
        SET tags = '{}'::jsonb
        WHERE tags = 'null'::jsonb;

        UPDATE temp_p 
        SET tags = "addr:country" || tags;

        UPDATE temp_p 
        SET tags = "addr:city" || tags;

        UPDATE temp_p 
        SET tags = website || tags;

        UPDATE temp_p 
        SET tags = "source" || tags;

        UPDATE temp_p 
        SET tags = brand || tags;

        UPDATE temp_p 
        SET tags = "operator" || tags;

        UPDATE temp_p 
        SET tags = "origin_geometry" || tags;

        UPDATE temp_p 
        SET tags = phone || tags;

        UPDATE temp_p 
        SET tags = osm_id || tags;

        UPDATE pois_upload pf 
        SET    tags = tp.tags
        FROM   temp_p tp
        WHERE  pf.poi_goat_id = tp.poi_goat_id;

        ALTER TABLE pois_upload 
        ADD COLUMN wheelchair text; 

        UPDATE pois_upload 
        SET wheelchair = tags ->> 'wheelchair';

        INSERT INTO poi (category, "name", street, housenumber, zipcode, opening_hours, wheelchair, tags, geom, uid)
        SELECT amenity, "name", "addr:street", housenumber, "addr:postcode" , opening_hours, wheelchair, tags, geometry, poi_goat_id
        FROM pois_upload;
        
        DELETE FROM poi 
        WHERE uid IN (SELECT uid FROM pois_remove);
        ''',
        "pois2goatschema" :'''
        DROP TABLE IF EXISTS poi;
        CREATE TABLE poi (
        id serial4 NOT NULL,
        category text NOT NULL,
        "name" text NULL,
        street text NULL,
        housenumber text NULL,
        zipcode text NULL,
        opening_hours text NULL,
        wheelchair text NULL,
        tags jsonb NULL,
        geom geometry(point, 4326) NOT NULL,
        uid text NOT NULL,
        CONSTRAINT poi_pkey PRIMARY KEY (id),
        CONSTRAINT poi_uid_key UNIQUE (uid)
        );
        CREATE INDEX idx_poi_geom ON poi USING gist (geom);
        CREATE INDEX ix_basic_poi_category ON poi USING btree (category);
        CREATE INDEX ix_basic_poi_uid ON poi USING btree (uid);

        -- ##################################################################################  --
        -- ## TEMPORAL FIX FOR poiS TABLE #############################################  --

        DROP TABLE IF EXISTS temp_p;
        CREATE TABLE temp_p as
        SELECT  poi_goat_id,
                jsonb_build_object('addr:country',"addr:country") AS "addr:country" , 
                jsonb_build_object('addr:city',"addr:city") AS "addr:city" , 
                jsonb_build_object('website', website) as website, 
                jsonb_build_object('source', "source") AS "source",
                jsonb_build_object('brand', brand) AS "brand",
                jsonb_build_object('operator', "operator") AS "operator",
                jsonb_build_object('origin_geometry', origin_geometry) AS origin_geometry ,
                jsonb_build_object('phone', phone) AS "phone",
                tags 
        FROM pois_goat pf; 

        UPDATE temp_p 
        SET tags = '{}'::jsonb
        WHERE tags = 'null'::jsonb;

        UPDATE temp_p 
        SET tags = "addr:city" || tags;

        UPDATE temp_p 
        SET tags = website || tags;

        UPDATE temp_p 
        SET tags = "source" || tags;

        UPDATE temp_p 
        SET tags = brand || tags;

        UPDATE temp_p 
        SET tags = "operator" || tags;

        UPDATE temp_p 
        SET tags = "origin_geometry" || tags;

        UPDATE temp_p 
        SET tags = phone || tags;

        UPDATE pois_goat pf 
        SET    tags = tp.tags
        FROM   temp_p tp
        WHERE  pf.poi_goat_id = tp.poi_goat_id;

        ALTER TABLE pois_goat 
        ADD COLUMN wheelchair text; 

        UPDATE pois_goat
        SET wheelchair = tags ->> 'wheelchair';

        INSERT INTO poi (category, "name", street, housenumber, zipcode, opening_hours, wheelchair, tags, geom, uid)
        SELECT amenity, "name", "addr:street", housenumber, "addr:postcode" , opening_hours, wheelchair, tags, geom, poi_goat_id
        FROM pois_goat;
        ''',
        'update_poi_id_table' : '''
            WITH new_poi_id AS (
            SELECT concat(split_part(uid, '-', 1),'-',split_part(uid, '-', 2),'-',split_part(uid, '-', 3)) AS poi_goat_id, split_part(uid, '-', 4)::int4 AS "index" , (tags->'osm_id')::bigint AS osm_id, name, (tags->'origin_geometry')::TEXT  AS origin_geometry
            FROM poi pt 
            WHERE NOT EXISTS (SELECT 
                            FROM poi_goat_id 
                            WHERE concat(poi_goat_id, '-', to_char("index", 'fm0000')) = pt.uid)
            )
            INSERT INTO poi_goat_id (poi_goat_id, "index", osm_id, name, origin_geometry)
            SELECT poi_goat_id , "index", osm_id, name, origin_geometry
            FROM new_poi_id;'''
}