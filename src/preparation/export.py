



"""SELECT uid, category, name, street, housenumber, zipcode, opening_hours, wheelchair, tags, geom   
FROM basic.poi 
WHERE ST_Intersects(p.geom, ST_SETSRID(ST_GEOMFROMTEXT(hex_geom), 4326))"""

"""SELECT id, population, demography, building_id, sub_study_area_id, geom   
FROM basic.population p 
WHERE ST_Intersects(p.geom, ST_SETSRID(ST_GEOMFROMTEXT(hex_geom), 4326))"""

"""SELECT id, category, name, opening_hours, wheelchair, tags, geom   
FROM basic.aoi p
WHERE ST_Intersects(p.geom, ST_SETSRID(ST_GEOMFROMTEXT(hex_geom), 4326))"""
