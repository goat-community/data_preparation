/*This script was used to create the GTFS region.*/

DROP TABLE public.gtfs_regions;
CREATE TABLE public.gtfs_regions(
	id serial,
	area float, 
	buffer_geom geometry,
	geom geometry
); 

/*Germany, France and Poland where calculated like this*/
DROP TABLE IF EXISTS public.density_points;
CREATE TABLE public.density_points AS 
WITH nuts AS 
(
	SELECT n.nuts_id, n.geom, p."OBS_VALUE"::float AS density
	FROM population_nuts p, kart_poi_goat.nuts n  
	WHERE p."TIME_PERIOD" = '2021'
	AND levl_code = 3
	AND cntr_code IN ('FR') 
	AND nuts_id = p.geo
	AND nuts_id NOT LIKE 'FRY%'
)
SELECT nuts_id, (ST_Dump(ST_GeneratePoints(geom, (density / 3)::integer))).geom AS geom
FROM nuts x; 

CREATE INDEX ON public.density_points USING GIST(geom);

DROP TABLE public.density_points_classified; 
CREATE TABLE public.density_points_classified AS 
SELECT geom, ST_ClusterKMeans(geom, 5) OVER() AS cid
FROM public.density_points; 
CREATE INDEX ON public.density_points_classified USING GIST(geom);

INSERT INTO public.gtfs_regions(area, buffer_geom, geom)
WITH centroids AS 
(
	SELECT cid, ST_Centroid(ST_collect(geom)) AS geom
	FROM public.density_points_classified
	GROUP BY cid
),
voronoi AS 
(
	SELECT (ST_Dump(ST_VoronoiPolygons(ST_collect(geom)))).geom AS geom
	FROM centroids
),
buffered AS 
(
	SELECT ST_BUFFER(ST_INTERSECTION(n.geom, v.geom)::geography, 80000)::geometry buffer_geom, ST_INTERSECTION(n.geom, v.geom) geom
	FROM voronoi v, kart_poi_goat.nuts n
	WHERE nuts_id = 'FR'
)
SELECT ST_AREA(buffer_geom::geography) / 1000000 area, b.buffer_geom, geom 
FROM buffered b;

/*Austria, Denmark, Netherlands and Czech Republic where calculated like this*/
INSERT INTO public.gtfs_regions(area, buffer_geom, geom)
SELECT ST_AREA(ST_BUFFER(n.geom::geography, 80000)) / 1000000 , ST_BUFFER(n.geom::geography, 80000)::geometry  buffer_geom, n.geom
FROM (SELECT * FROM kart_poi_goat.nuts n WHERE nuts_id IN ('AT', 'DK', 'NL', 'CZ')) n; 

/*Switzerland + Lichtenstein and Belgium + Luxemburg where calculated like this*/
INSERT INTO public.gtfs_regions(area, buffer_geom, geom)
WITH x AS 
(
	SELECT ST_AREA(ST_BUFFER(n.geom::geography, 80000)) / 1000000 , ST_BUFFER(n.geom::geography, 80000)::geometry  buffer_geom, n.geom
	FROM (SELECT ST_UNION(geom) geom FROM kart_poi_goat.nuts n WHERE nuts_id IN ('CH', 'LI')) n
)
SELECT ST_AREA(ST_BUFFER(n.geom::geography, 80000)) / 1000000 , ST_BUFFER(n.geom::geography, 80000)::geometry  buffer_geom, n.geom
FROM x n;  
