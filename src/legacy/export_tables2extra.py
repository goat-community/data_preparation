sql_queries_goat = {
    ##"schema_extra": '''CREATE SCHEMA IF NOT EXISTS extra;''',
    "landuse": '''
        DROP TABLE IF EXISTS extra.landuse_atkis;
        CREATE TABLE extra.landuse_atkis (LIKE public.landuse INCLUDING ALL);
        INSERT INTO extra.landuse_atkis
        SELECT * 
        FROM public.landuse;
    '''
    ,
    "landuse_osm": ''' 
        DROP TABLE IF EXISTS extra.landuse_osm;
        CREATE TABLE extra.landuse_osm (LIKE public.landuse_osm INCLUDING ALL);
        INSERT INTO extra.landuse_osm
        SELECT * 
        FROM public.landuse_osm;
    '''
    ,
    "landuse_additional": ''' 
        DROP TABLE IF EXISTS extra.landuse_additional;
        CREATE TABLE extra.landuse_additional (LIKE public.landuse_additional INCLUDING ALL);
        INSERT INTO extra.landuse_additional
        SELECT * 
        FROM public.landuse_additional;
    '''
    ,
    "accidents": '''
        DROP TABLE IF EXISTS extra.accidents_walking;
        CREATE TABLE extra.accidents_walking (LIKE public.accidents INCLUDING ALL);
        INSERT INTO extra.accidents_walking
        SELECT * 
        FROM public.accidents
        WHERE istfuss = '1';    
        
        DROP TABLE IF EXISTS extra.accidents_cycling;
        CREATE TABLE extra.accidents_cycling (LIKE public.accidents INCLUDING ALL);
        INSERT INTO extra.accidents_cycling
        SELECT * 
        FROM public.accidents
        WHERE istrad = '1';
    '''
    ,
    "buildings": '''
        DROP TABLE IF EXISTS extra.building;
        CREATE TABLE extra.building (LIKE basic.building INCLUDING ALL);
        INSERT INTO extra.building
        SELECT * 
        FROM basic.building;
    '''
}


