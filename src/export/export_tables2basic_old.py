sql_queries_goat = {
    ##"schema_basic": '''CREATE SCHEMA IF NOT EXISTS basic;''',
    "study_area": '''
        DROP TABLE IF EXISTS basic.sub_study_area CASCADE;
        DROP TABLE IF EXISTS basic.study_area CASCADE;
        CREATE TABLE basic.study_area (
        id serial4 NOT NULL,
        "name" text NOT NULL,
        geom geometry(multipolygon, 4326) NOT NULL,
        population int4 NOT NULL,
        setting jsonb NOT NULL,
        buffer_geom_heatmap geometry NOT NULL,
        CONSTRAINT study_area_pkey PRIMARY KEY (id)
        );
        CREATE INDEX idx_study_area_geom ON basic.study_area USING gist (geom);

        CREATE TABLE basic.sub_study_area (
        id serial4 NOT NULL,
        "name" text NOT NULL,
        default_building_levels int2 NULL,
        default_roof_levels int2 NULL,
        area float8 NULL,
        geom geometry(multipolygon, 4326) NOT NULL,
        population int4 NOT NULL,
        study_area_id int4 NOT NULL,
        CONSTRAINT sub_study_area_pkey PRIMARY KEY (id),
        CONSTRAINT sub_study_area_study_area_id_fkey FOREIGN KEY (study_area_id) REFERENCES basic.study_area(id)
        );
        CREATE INDEX idx_sub_study_area_geom ON basic.sub_study_area USING gist (geom);
        CREATE INDEX ix_basic_sub_study_area_study_area_id ON basic.sub_study_area USING btree (study_area_id);

        DROP TABLE IF EXISTS coll_st_area;
        CREATE TEMP TABLE coll_st_area as
        (SELECT RIGHT(rs, length(rs) - 1)::int4 AS rs, sa.gen AS "name", ST_UNION(sa.geom) AS geom, sum(sa.sum_pop) AS population, '{}'::jsonb AS setting
        FROM temporal.study_area sa
        GROUP BY sa.gen, sa.rs);

        INSERT INTO basic.study_area (id, "name", geom, population, setting, buffer_geom_heatmap) 
        SELECT rs, "name", st_multi(geom), population , setting, ST_MULTI(ST_BUFFER(geom::geography, 2500)::geometry)
        FROM coll_st_area;

        ALTER TABLE temporal.study_area 
        ADD COLUMN area float; 

        UPDATE temporal.study_area 
        SET area = ST_AREA(geom::geography);

        INSERT INTO basic.sub_study_area ("name", default_building_levels, default_roof_levels, area, geom, population, study_area_id)
        SELECT "name", default_building_levels, default_roof_levels, area, geom, sum_pop, RIGHT(rs, length(rs) - 1)::int4
        FROM temporal.study_area;''',
    "nodes_edges":'''
        DELETE from temporal.ways_vertices_pgr 
        WHERE geom IS NULL;

        DROP TABLE IF EXISTS basic.edge;
        DROP TABLE IF EXISTS basic.node;
        CREATE TABLE basic.node (
            id int4 NOT NULL,
            osm_id int8 NULL,
            class_ids _int4 NULL,
            foot _text NULL,
            bicycle _text NULL,
            lit_classified _text NULL,
            wheelchair_classified _text NULL,
            geom geometry(point, 4326) NOT NULL,
            scenario_id int4 NULL,
            cnt int4 NULL,
            death_end bool NULL,
            CONSTRAINT node_pkey PRIMARY KEY (id)
            );
        CREATE INDEX idx_node_geom ON basic.node USING gist (geom);
        CREATE INDEX ix_basic_node_death_end ON basic.node USING btree (death_end);
        CREATE INDEX ix_basic_node_scenario_id ON basic.node USING btree (scenario_id);

        INSERT INTO basic.node (id, osm_id,	class_ids, foot,bicycle,lit_classified,	wheelchair_classified,	geom, cnt, death_end)
        SELECT id, osm_id,	class_ids, foot, bicycle, lit_classified, wheelchair_classified, geom, cnt, death_end
        FROM temporal.ways_vertices_pgr;

        CREATE TABLE basic.edge (
            id int4 NOT NULL,
            length_m float8 NOT NULL,
            "name" text NULL,
            osm_id int8 NULL,
            bicycle text NULL,
            foot text NULL,
            oneway text NULL,
            crossing text NULL,
            crossing_delay_category int2 NULL,
            bicycle_road text NULL,
            cycleway text NULL,
            highway text NULL,
            incline text NULL,
            lanes float8 NULL,
            lit text NULL,
            lit_classified text NULL,
            parking text NULL,
            parking_lane_both text NULL,
            parking_lane_right text NULL,
            parking_lane_left text NULL,
            segregated text NULL,
            sidewalk text NULL,
            sidewalk_both_width float8 NULL,
            sidewalk_left_width float8 NULL,
            sidewalk_right_width float8 NULL,
            smoothness text NULL,
            surface text NULL,
            wheelchair text NULL,
            wheelchair_classified text NULL,
            width float8 NULL,
            s_imp float8 NULL,
            rs_imp float8 NULL,
            impedance_surface float8 NULL,
            geom geometry(linestring, 4326) NOT NULL,
            length_3857 float8 NOT NULL,
            coordinates_3857 json NOT NULL,
            scenario_id int4 NULL,
            class_id int4 NOT NULL,
            one_way int4 NULL,
            maxspeed_forward int4 NULL,
            maxspeed_backward int4 NULL,
            one_link_crossing bool NULL,
            incline_percent int4 NULL,
            death_end int4 NULL,
            "source" int4 NOT NULL,
            target int4 NOT NULL,
            edge_id int4 NULL,
            CONSTRAINT edge_pkey PRIMARY KEY (id),
            CONSTRAINT edge_edge_id_fkey FOREIGN KEY (edge_id) REFERENCES basic.edge(id),
            CONSTRAINT edge_source_fkey FOREIGN KEY ("source") REFERENCES basic.node(id),
            CONSTRAINT edge_target_fkey FOREIGN KEY (target) REFERENCES basic.node(id)
        );
        CREATE INDEX idx_edge_geom ON basic.edge USING gist (geom);
        CREATE INDEX ix_basic_edge_bicycle ON basic.edge USING btree (bicycle);
        CREATE INDEX ix_basic_edge_edge_id ON basic.edge USING btree (edge_id);
        CREATE INDEX ix_basic_edge_foot ON basic.edge USING btree (foot);
        CREATE INDEX ix_basic_edge_scenario_id ON basic.edge USING btree (scenario_id);
        CREATE INDEX ix_basic_edge_source ON basic.edge USING btree (source);
        CREATE INDEX ix_basic_edge_target ON basic.edge USING btree (target);

        INSERT INTO basic.edge (
            id,
            length_m,
            "name",
            osm_id,
            bicycle,
            foot,
            oneway,
            crossing,
            crossing_delay_category,
            bicycle_road,
            cycleway,
            highway,
            incline,
            lanes,
            lit,
            lit_classified,
            parking,
            parking_lane_both,
            parking_lane_right,
            parking_lane_left,
            segregated,
            sidewalk,
            sidewalk_both_width,
            sidewalk_left_width,
            sidewalk_right_width,
            smoothness,
            surface,
            wheelchair,
            wheelchair_classified,
            width ,
            s_imp ,
            rs_imp ,
            impedance_surface,
            geom,
            length_3857 ,
            coordinates_3857,
            class_id ,
            one_way ,
            maxspeed_forward ,
            maxspeed_backward ,
            one_link_crossing,
            incline_percent ,
            death_end ,
            "source",
            target)
        SELECT id,
            length_m,
            "name",
            osm_id,
            bicycle,
            foot,
            oneway,
            crossing,
            crossing_delay_category,
            bicycle_road,
            cycleway,
            highway,
            incline,
            lanes,
            lit,
            lit_classified,
            parking,
            parking_lane_both,
            parking_lane_right,
            parking_lane_left,
            segregated,
            sidewalk,
            sidewalk_both_width,
            sidewalk_left_width,
            sidewalk_right_width,
            smoothness,
            surface,
            wheelchair,
            wheelchair_classified,
            width ,
            s_imp ,
            rs_imp ,
            impedance_surface,
            geom,
            length_3857 ,
            coordinates_3857,
            class_id ,
            one_way ,
            maxspeed_forward ,
            maxspeed_backward ,
            one_link_crossing,
            incline_percent ,
            death_end ,
            "source",
            target
        FROM temporal.ways 
        WHERE length_m IS NOT NULL
        AND length_3857 IS NOT NULL 
        AND coordinates_3857 IS NOT NULL
        AND class_id IS NOT NULL
        AND "source" IS NOT NULL
        AND target IS NOT NULL;''',
    "pois": '''
        DROP TABLE IF EXISTS basic.poi;
        CREATE TABLE basic.poi (
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
        CREATE INDEX idx_poi_geom ON basic.poi USING gist (geom);
        CREATE INDEX ix_basic_poi_category ON basic.poi USING btree (category);
        CREATE INDEX ix_basic_poi_uid ON basic.poi USING btree (uid); 

        -- ##################################################################################  --
        -- ## TEMPORAL FIX FOR pois_goat TABLE #############################################  --

        DROP TABLE IF EXISTS temp_p;
        CREATE TABLE temp_p as
        SELECT  poi_goat_id,
                jsonb_build_object('addr:country',"addr:country") AS "addr:country" , 
                jsonb_build_object('addr:city',"addr:city") AS "addr:city" , 
                jsonb_build_object('website', website) as website, 
                jsonb_build_object('source', "source") AS "source",
                jsonb_build_object('brand', brand) AS "brand",
                jsonb_build_object('operator', "operator") AS "operator",
                jsonb_build_object('origin_geometry', 'origin_geometry') AS origin_geometry ,
                jsonb_build_object('phone', phone) AS "phone",
                tags 
        FROM pois_goat pf; 

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
        DROP COLUMN "addr:city" , 
        DROP COLUMN website, 
        DROP COLUMN "source",
        DROP COLUMN "brand",
        DROP COLUMN "operator",
        DROP COLUMN origin_geometry ,
        DROP COLUMN "phone",
        DROP COLUMN "addr:country";

        ALTER TABLE pois_goat 
        ADD COLUMN wheelchair text; 

        UPDATE pois_goat 
        SET wheelchair = tags ->> 'wheelchair';

        INSERT INTO basic.poi (category, "name", street, housenumber, zipcode, opening_hours, wheelchair, tags, geom, uid)
        SELECT amenity, "name", "addr:street", housenumber, "addr:postcode" , opening_hours, wheelchair, tags, geom, poi_goat_id
        FROM pois_goat;''',
    "building": '''
        DROP TABLE IF EXISTS basic.population;
        DROP TABLE IF EXISTS basic.building;
        CREATE TABLE basic.building (
        id serial4 NOT NULL,
        amenity text NULL,
        residential_status text NULL,
        housenumber text NULL,
        street text NULL,
        building_levels int2 NULL,
        building_levels_residential int2 NULL,
        roof_levels int2 NULL,
        height float8 NULL,
        geom geometry(polygon, 4326) NOT NULL,
        area int4 NULL,
        gross_floor_area_residential int4 NULL,
        osm_id int4 NULL,
        building_type text NULL,
        CONSTRAINT building_pkey PRIMARY KEY (id)
        );
        CREATE INDEX idx_building_geom ON basic.building USING gist (geom);

        INSERT INTO basic.building (amenity,
                    residential_status,
                    housenumber,
                    street,
                    building_levels,
                    building_levels_residential,
                    roof_levels,
                    height,
                    geom ,
                    area,
                    gross_floor_area_residential,
                    osm_id,
                    building_type)
        SELECT amenity,
                residential_status,
                housenumber,
                street,
                building_levels,
                building_levels_residential,
                roof_levels,
                height,
                geom ,
                area,
                gross_floor_area_residential,
                osm_id,
                building
        FROM buildings;''',
    "population": '''
        CREATE TABLE basic.population (
        id serial4 NOT NULL,
        population float8 NULL,
        geom geometry(point, 4326) NOT NULL,
        demography jsonb NULL,
        building_id int4 NULL,
        sub_study_area_id int4 NULL,
        CONSTRAINT population_pkey PRIMARY KEY (id),
        CONSTRAINT population_building_id_fkey FOREIGN KEY (building_id) REFERENCES basic.building(id) ON DELETE CASCADE
        );
        CREATE INDEX idx_population_geom ON basic.population USING gist (geom);
        CREATE INDEX ix_basic_population_building_id ON basic.population USING btree (building_id);
        CREATE INDEX ix_basic_population_sub_study_area_id ON basic.population USING btree (sub_study_area_id);

        INSERT INTO basic.population (population,
                    geom,
                    building_id,
                    sub_study_area_id)
        SELECT p.population,
                p.geom,
                p.building_gid,
                s.id
        FROM population p, basic.sub_study_area s
        WHERE ST_Intersects(p.geom, s.geom);
    '''
    ,
    "grids": '''
        DROP TABLE IF EXISTS basic.grid_calculation CASCADE;
        DROP TABLE IF EXISTS basic.grid_visualization CASCADE;
        DROP TABLE IF EXISTS basic.study_area_grid_visualization CASCADE;
        CREATE TABLE basic.grid_visualization (
        id int8 NOT NULL,
        geom public.geometry(polygon, 4326) NOT NULL,
        area_isochrone float8 NULL,
        percentile_area_isochrone int2 NULL,
        percentile_population int2 NULL,
        population int4 NULL,
        CONSTRAINT grid_visualization_pkey PRIMARY KEY (id)
        );
        CREATE INDEX idx_grid_visualization_geom ON basic.grid_visualization USING gist (geom);

        INSERT INTO basic.grid_visualization (id, geom)
        SELECT gv.id, gv.geometry
        FROM public.grid_visualization gv, basic.study_area sa
        WHERE ST_intersects(sa.geom,gv.geometry);

        -- Study area grid visualization
        CREATE TABLE basic.study_area_grid_visualization (
        id serial4 NOT NULL,
        study_area_id int4 NOT NULL,
        grid_visualization_id int8 NOT NULL,
        CONSTRAINT study_area_grid_visualization_pkey PRIMARY KEY (id),
        CONSTRAINT study_area_grid_visualization_grid_visualization_id_fkey FOREIGN KEY (grid_visualization_id) REFERENCES basic.grid_visualization(id),
        CONSTRAINT study_area_grid_visualization_study_area_id_fkey FOREIGN KEY (study_area_id) REFERENCES basic.study_area(id)
        );
        CREATE INDEX ix_basic_study_area_grid_visualization_study_area_id ON basic.study_area_grid_visualization USING btree (study_area_id);

        INSERT INTO basic.study_area_grid_visualization (study_area_id , grid_visualization_id)
        SELECT gv.study_area_id, gv.id
        FROM public.grid_visualization gv, basic.study_area sa
        WHERE ST_intersects(sa.geom,gv.geometry);

        -- Grid calculation
        CREATE TABLE basic.grid_calculation (
        id int8 NOT NULL,
        geom public.geometry(polygon, 4326) NOT NULL,
        grid_visualization_id int8 NOT NULL,
        CONSTRAINT grid_calculation_pkey PRIMARY KEY (id),
        CONSTRAINT grid_calculation_grid_visualization_id_fkey FOREIGN KEY (grid_visualization_id) REFERENCES basic.grid_visualization(id) ON DELETE CASCADE
        );
        CREATE INDEX idx_grid_caclulation_geom ON basic.grid_calculation USING gist (geom);

        INSERT INTO basic.grid_calculation (id, geom, grid_visualization_id)
        SELECT gc.id ,gc.geometry, gc.parent_id
        FROM public.grid_calculation gc
        WHERE gc.parent_id IN (SELECT id FROM basic.grid_visualization) ;
    '''
    ,
    "aoi":  '''
        DROP TABLE IF EXISTS basic.aoi;
        CREATE TABLE basic.aoi (
        id serial4 NOT NULL,
        category text NOT NULL,
        "name" text NULL,
        opening_hours text NULL,
        wheelchair text NULL,
        tags jsonb NULL,
        geom geometry(multipolygon, 4326) NOT NULL,
        CONSTRAINT aoi_pkey PRIMARY KEY (id)
        );
        CREATE INDEX idx_aoi_geom ON basic.aoi USING gist (geom);
        CREATE INDEX ix_basic_aoi_category ON basic.aoi USING btree (category);

        INSERT INTO basic.aoi (category,geom)
        SELECT category, geom
        FROM aoi;
    '''
    ,
    "dem": '''
        DROP TABLE IF EXISTS basic.dem;
        CREATE TABLE basic.dem as
        SELECT d.rid, ST_CLIP(d.rast,1, sa.geom, 20) AS rast, d.filename
        FROM public.dem d, basic.study_area sa;
    '''
}


