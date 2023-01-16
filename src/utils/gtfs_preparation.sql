DROP TABLE IF EXISTS temporal.test_train_routes; 
CREATE TABLE temporal.test_train_routes AS 
WITH train_routes AS 
(
	SELECT r.*, a.agency_name 
	FROM gtfs_new.routes r, gtfs_new.agency a  
	WHERE route_type = '2'
	AND r.agency_id = a.agency_id 
),
selected_trips_routes AS 
(
	SELECT r.*, j.shape_id
	FROM train_routes r
	CROSS JOIN LATERAL 
	(
		SELECT t.* 
		FROM gtfs_new.trips t
		WHERE t.route_id = r.route_id  
		LIMIT 1 
	) j
)
SELECT t.*, s.*
FROM selected_trips_routes t
CROSS JOIN LATERAL 
(
	SELECT ST_MakeLine(shape_pt_loc) AS geom 
	FROM gtfs_new.shapes s
	WHERE s.shape_id = t.shape_id 
) s;
CREATE INDEX ON temporal.test_train_routes USING GIST(geom);


DROP TABLE IF EXISTS temporal.test_train_routes_bavaria;
CREATE TABLE temporal.test_train_routes_bavaria AS 
SELECT DISTINCT w.*
FROM temporal.test_train_routes w, basic.study_area s
WHERE ST_Intersects(s.geom, w.geom)
AND s.id::text LIKE '9%';
CREATE INDEX ON temporal.test_train_routes_bavaria USING GIST(geom);

agency_names_out =  ['Döllnitzbahngesellschaft', 'Wanderbahn im Regental', 'Nichtbundeseigene Eisenbahnen', 'Mainschleifenbahn', 'Chiemgauer Lokalbahn', 'Rhön-Zügle']
agency_name_long_distance = ['Österreichische Bundesbahnen', 'Nachtzug']
route_lomg_names_out = ['Sonderzug', ]


agency_to_bus= Schmetterling Reisen, DB ZugBus Regionalverkehr Alb-Bodensee, Regensburger Verkehrsverbund
Gesellschaft zur Förderung des ÖPNV Regensburg
Landkreis Landsberg (Lech)


--Label touristic trains
WITH agency_ids AS 
(
	SELECT agency_id 
	FROM gtfs_new.agency a 
	WHERE a.agency_name IN ('Döllnitzbahngesellschaft', 'Wanderbahn im Regental', 'Nichtbundeseigene Eisenbahnen', 'Mainschleifenbahn', 'Chiemgauer Lokalbahn', 'Rhön-Zügle') 
)
UPDATE gtfs_new.routes r
SET route_type = '107'
WHERE r.agency_id IN (SELECT * FROM agency_ids) 

--Label wrongly labelled train as bus 
WITH agency_ids AS 
(
	SELECT a.*
	FROM gtfs_new.agency a 
	WHERE a.agency_name IN ('Schmetterling Reisen', 'DB ZugBus Regionalverkehr Alb-Bodensee', 
	'Regensburger Verkehrsverbund', 'Gesellschaft zur Förderung des ÖPNV Regensburg', 'Landkreis Landsberg (Lech)') 
)
UPDATE gtfs_new.routes r
SET route_type = '3'
WHERE r.agency_id IN (SELECT agency_id  FROM agency_ids); 

UPDATE gtfs_new.routes r 
SET route_type = '107'
WHERE route_long_name = 'Sonderzug';

UPDATE gtfs_new.routes r
SET route_type = '101'
WHERE route_type = '2'
AND route_long_name LIKE ANY (array['ICE', 'EC', 'RJX', 'EuroCity-Express', 'Intercity-Express']); 

UPDATE gtfs_new.routes r
SET route_type = '102'
WHERE route_type = '2'
AND route_long_name LIKE ANY (array['EuroCity', 'Intercity', 'IC']);  

UPDATE gtfs_new.routes r
SET route_type = '105'
WHERE route_type = '2'
AND route_long_name LIKE ANY (array['EuroNight', 'NJ', 'EN']);  

--Label Flixtrain as Long  Distance
UPDATE gtfs_new.routes r 
SET route_type = '102'
WHERE route_type = '2'
AND agency_id = '80';


--Reclassify Bus services
--Label on demand services
UPDATE gtfs_new,routes r 
SET route_type = '715'
WHERE route_short_name LIKE 'AST%'; 

--Label on demand services in MVV
UPDATE gtfs_new.routes r 
SET route_type = '715'
WHERE length(regexp_replace(route_short_name, '[^0-9]', '', 'g')) = 4
AND agency_id = '14'
AND route_type = '3'; 

--Label Flixbus as Coach
UPDATE gtfs_new.routes r 
SET route_type = '200'
WHERE agency_id = '258'
AND route_type = '3'; 


DROP TABLE IF EXISTS gtfs_new.trips_weekday;
CREATE TABLE gtfs_new.trips_weekday (id serial, trip_id TEXT, route_type gtfs_new."route_type_val", weekdays boolean[]);
INSERT INTO gtfs_new.trips_weekday(trip_id, route_type, weekdays)
SELECT t.trip_id, t.route_type, ARRAY[
(('{"available": "true", "not_available": "false"}'::jsonb) ->> c.monday::text)::boolean,
(('{"available": "true", "not_available": "false"}'::jsonb) ->> c.tuesday::text)::boolean,
(('{"available": "true", "not_available": "false"}'::jsonb) ->> c.wednesday::text)::boolean,
(('{"available": "true", "not_available": "false"}'::jsonb) ->> c.thursday::text)::boolean,
(('{"available": "true", "not_available": "false"}'::jsonb) ->> c.friday::text)::boolean,
(('{"available": "true", "not_available": "false"}'::jsonb) ->> c.saturday::text)::boolean,
(('{"available": "true", "not_available": "false"}'::jsonb) ->> c.sunday::text)::boolean
] AS weekdays
FROM
 (
 SELECT t.trip_id, t.service_id, r.route_type
	FROM gtfs_new.trips t, gtfs_new.routes r
    WHERE t.route_id = r.route_id
 ) t, gtfs_new.calendar c
WHERE t.service_id = c.service_id
AND '2022-02-07' >= start_date
AND '2022-02-13' <= end_date;
ALTER TABLE gtfs_new.trips_weekday ADD PRIMARY KEY (id);
CREATE INDEX ON gtfs_new.trips_weekday (trip_id);
CREATE TABLE gtfs_new.stop_times_optimized (
    id serial4 NOT NULL,
    trip_id text NULL,
    arrival_time interval NULL,
    stop_id text NULL,
    route_type gtfs_new."route_type_val" NULL,
    weekdays _bool NULL
);
INSERT INTO gtfs_new.stop_times_optimized(trip_id, arrival_time, stop_id, route_type, weekdays)
SELECT st.trip_id, st.arrival_time, stop_id, route_type, weekdays
FROM gtfs_new.stop_times st, gtfs_new.trips_weekday w
WHERE st.trip_id = w.trip_id;
ALTER TABLE gtfs_new.stop_times_optimized ADD PRIMARY KEY(id);
CREATE INDEX ON gtfs_new.stop_times_optimized(stop_id, arrival_time);