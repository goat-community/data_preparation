BEGIN;
SET LOCAL check_function_bodies TO FALSE;
DROP FUNCTION IF EXISTS public.get_idw_values;
CREATE OR REPLACE FUNCTION public.get_idw_values(geom geometry, buffer_distance float)
RETURNS TABLE (dp_geom geometry, distance float, val float)
 LANGUAGE sql
AS $function$
	SELECT dp.geom, ST_DISTANCE(r.geom,dp.geom) distance, val
	FROM  
	(
		SELECT geom, st_clip(d.rast, st_buffer(geom, buffer_distance)) AS rast 	
		FROM dem d
		WHERE d.rast && st_buffer(geom, buffer_distance)
	) r
	, LATERAL ST_PixelAsCentroids(rast, 1) AS dp
	ORDER BY r.geom <-> dp.geom 
	LIMIT 3

$function$;
COMMIT;


