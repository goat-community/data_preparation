DROP TABLE IF EXISTS temporal.test_train_routes; 
CREATE TABLE temporal.test_train_routes AS 
WITH train_routes AS 
(
	SELECT r.*, a.agency_name 
	FROM gtfs_germany.routes r, gtfs_germany.agency a  
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
		FROM gtfs_germany.trips t
		WHERE t.route_id = r.route_id  
		LIMIT 1 
	) j
)
SELECT t.*, s.*
FROM selected_trips_routes t
CROSS JOIN LATERAL 
(
	SELECT ST_MakeLine(shape_pt_loc) AS geom 
	FROM gtfs_germany.shapes s
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

/*
Insert pre-labelled on-demand-services
*/
DROP TABLE IF EXISTS gtfs_germany.on_demand_services;
CREATE TABLE gtfs_germany.on_demand_services
(
	agency_name text, 
	route_short_name text
);
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Augsburger Verkehrs- und Tarifverbund','70AST'),
	 ('Augsburger Verkehrs- und Tarifverbund','71AST'),
	 ('Augsburger Verkehrs- und Tarifverbund','72A'),
	 ('Augsburger Verkehrs- und Tarifverbund','73AST'),
	 ('Augsburger Verkehrs- und Tarifverbund','76AST'),
	 ('Augsburger Verkehrs- und Tarifverbund','74AST'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 210'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 206'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 208'),
	 ('Augsburger Verkehrs- und Tarifverbund','415');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Augsburger Verkehrs- und Tarifverbund','503'),
	 ('Augsburger Verkehrs- und Tarifverbund','620'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 241'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 243'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 244'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 230'),
	 ('Augsburger Verkehrs- und Tarifverbund','AST 27'),
	 ('Augsburger Verkehrs- und Tarifverbund','628'),
	 ('Augsburger Verkehrs- und Tarifverbund','641'),
	 ('Augsburger Verkehrs- und Tarifverbund','642');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Augsburger Verkehrs- und Tarifverbund','513 AST'),
	 ('Augsburger Verkehrs- und Tarifverbund','Ruf 709'),
	 ('Stadtverkehr Bamberg','913'),
	 ('Stadtverkehr Bamberg','910'),
	 ('Bodensee-Oberschwaben-Verkehrsverbund','74'),
	 ('Stadtverkehr Bayreuth','307'),
	 ('Stadtverkehr Bayreuth','304'),
	 ('Stadtverkehr Bayreuth','303'),
	 ('Stadtverkehr Bayreuth','312'),
	 ('Stadtverkehr Bayreuth','310');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Landkreis Cham','908'),
	 ('Landkreis Cham','906'),
	 ('Landkreis Cham','904'),
	 ('Landkreis Cham','903'),
	 ('Landkreis Cham','902'),
	 ('Landkreis Cham','912'),
	 ('Landkreis Cham','911'),
	 ('Landkreis Cham','910'),
	 ('Landkreis Cham','901'),
	 ('Stadtverkehr Coburg','8');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Donau-Iller-Nahverkehrs-GmbH (UL+BC)','712R'),
	 ('Donau-Iller-Nahverkehrs-GmbH (UL+BC)','716'),
	 ('Donau-Iller-Nahverkehrs-GmbH (UL+BC)','251'),
	 ('DB RegioBus Bayern','103'),
	 ('DB RegioBus Bayern','227'),
	 ('Elko-Tours','E525'),
	 ('Erlanger Stadtverkehr','283T'),
	 ('Erlanger Stadtverkehr','281T'),
	 ('Erlanger Stadtverkehr','293T'),
	 ('Erlanger Stadtverkehr','287T');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Erlanger Stadtverkehr','285T'),
	 ('Gesellschaft zur Förderung des ÖPNV Regensburg','900'),
	 ('Grasmann-Reisen GmbH','660'),
	 ('Grasmann-Reisen GmbH','8067'),
	 ('Grasmann-Reisen GmbH','654'),
	 ('Grasmann-Reisen GmbH','652'),
	 ('Grasmann-Reisen GmbH','650'),
	 ('Grasmann-Reisen GmbH','610'),
	 ('Grasmann-Reisen GmbH','8068'),
	 ('Grasmann-Reisen GmbH','631');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Grasmann-Reisen GmbH','630'),
	 ('Grasmann-Reisen GmbH','662'),
	 ('Grasmann-Reisen GmbH','661'),
	 ('Heiner Geis GmbH','8230'),
	 ('Heiner Geis GmbH','8305'),
	 ('Heiner Geis GmbH','8184'),
	 ('Heiner Geis GmbH','8181'),
	 ('Heiner Geis GmbH','8153'),
	 ('Heiner Geis GmbH','8012'),
	 ('Heiner Geis GmbH','811');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Heiner Geis GmbH','8300'),
	 ('Omnipart','914R'),
	 ('Heiner Geis GmbH','8007'),
	 ('Landkreis Landsberg (Lech)','A31'),
	 ('Landkreis Landsberg (Lech)','A312'),
	 ('Landkreis Landsberg (Lech)','A311'),
	 ('Landkreis Landsberg (Lech)','A302'),
	 ('Landkreis Landsberg (Lech)','A21'),
	 ('Landkreis Landsberg (Lech)','A6'),
	 ('Landkreis Landsberg (Lech)','A91');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Landkreis Landsberg (Lech)','A61'),
	 ('Landkreis Landsberg (Lech)','A60'),
	 ('Landkreis Landsberg (Lech)','A22'),
	 ('Landkreis Landsberg (Lech)','A601'),
	 ('Landkreis Landsberg (Lech)','A51'),
	 ('Landkreis Landsberg (Lech)','A50'),
	 ('Landkreis Landsberg (Lech)','A14'),
	 ('Landkreis Landsberg (Lech)','A11'),
	 ('Münchner Verkehrs- und Tarifverbund','8800'),
	 ('Münchner Verkehrs- und Tarifverbund','8700');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Münchner Verkehrs- und Tarifverbund','8500'),
	 ('Münchner Verkehrs- und Tarifverbund','8400'),
	 ('Münchner Verkehrs- und Tarifverbund','8300'),
	 ('Münchner Verkehrs- und Tarifverbund','8200'),
	 ('Münchner Verkehrs- und Tarifverbund','8000'),
	 ('Münchner Verkehrs- und Tarifverbund','7321'),
	 ('Münchner Verkehrs- und Tarifverbund','7320'),
	 ('Münchner Verkehrs- und Tarifverbund','7280'),
	 ('Münchner Verkehrs- und Tarifverbund','7270'),
	 ('Münchner Verkehrs- und Tarifverbund','6800');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Münchner Verkehrs- und Tarifverbund','6004'),
	 ('Münchner Verkehrs- und Tarifverbund','6003'),
	 ('Münchner Verkehrs- und Tarifverbund','6002'),
	 ('Münchner Verkehrs- und Tarifverbund','6001'),
	 ('Münchner Verkehrs- und Tarifverbund','5680'),
	 ('Münchner Verkehrs- und Tarifverbund','5670'),
	 ('Omnipart','852R'),
	 ('Münchner Verkehrs- und Tarifverbund','5621'),
	 ('Münchner Verkehrs- und Tarifverbund','5403'),
	 ('Münchner Verkehrs- und Tarifverbund','5050');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Münchner Verkehrs- und Tarifverbund','5020'),
	 ('Münchner Verkehrs- und Tarifverbund','5010'),
	 ('Omnipart','871R'),
	 ('Omnipart','870R'),
	 ('Omnipart','AST171'),
	 ('Omnipart','AST166'),
	 ('Omnipart','AST163'),
	 ('Omnipart','AST162'),
	 ('Omnipart','AST161'),
	 ('Omnipart','AST150');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Omnipart','AST140'),
	 ('Omnipart','AST130'),
	 ('Omnipart','AST109'),
	 ('Omnipart','AST103'),
	 ('Omnipart','AST102'),
	 ('Omnipart','LT'),
	 ('Omnipart','35'),
	 ('Omnipart','26'),
	 ('Omnipart','17'),
	 ('Omnipart','16');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Omnipart','10'),
	 ('Omnipart','9'),
	 ('Omnipart','8'),
	 ('Omnipart','7'),
	 ('Omnipart','6'),
	 ('Omnipart','5'),
	 ('Omnipart','709'),
	 ('Omnipart','506'),
	 ('Omnipart','505'),
	 ('Omnipart','504');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Omnipart','503'),
	 ('Omnipart','502'),
	 ('Omnipart','501'),
	 ('Omnipart','126'),
	 ('Omnipart','AST 7'),
	 ('Omnipart','818R'),
	 ('Omnipart','940R'),
	 ('Omnipart','717'),
	 ('Omnipart','48'),
	 ('Omnipart','47');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Omnipart','45'),
	 ('Omnipart','81'),
	 ('Omnipart','819R'),
	 ('Omnipart','951R'),
	 ('Omnipart','921R'),
	 ('Omnipart','810R'),
	 ('Omnipart','964R'),
	 ('Omnipart','963R'),
	 ('Omnipart','959R'),
	 ('Omnipart','966R');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Omnipart','965R'),
	 ('Omnipart','955R'),
	 ('Omnipart','950R'),
	 ('Omnipart','931R'),
	 ('Omnipart','838R'),
	 ('Omnipart','831R'),
	 ('Omnipart','826R'),
	 ('Omnipart','20'),
	 ('Omnibusverkehr Franken','8350'),
	 ('Omnibusverkehr Franken','8435');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Omnibusverkehr Franken','397'),
	 ('Omnibusverkehr Franken','677'),
	 ('Omnibusverkehr Franken','506'),
	 ('Omnibusverkehr Franken','8365'),
	 ('Regionalbus Ostbayern','7581'),
	 ('Regionalbus Ostbayern','7518'),
	 ('Regionalbus Ostbayern','7512'),
	 ('Regionalbus Ostbayern','4'),

INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Schmetterling Reisen','389'),
	 ('Schmetterling Reisen','266'),
	 ('Schmetterling Reisen','265'),
	 ('Schmetterling Reisen','236'),
	 ('Schmetterling Reisen','234'),
	 ('Schmetterling Reisen','231'),
	 ('Schmetterling Reisen','226'),
	 ('Schmetterling Reisen','225'),
	 ('Schmetterling Reisen','224'),
	 ('Schmetterling Reisen','223'),
	 ('Schmetterling Reisen','222');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Schmetterling Reisen','221'),
	 ('Schmetterling Reisen','220'),
	 ('Schmetterling Reisen','219'),
	 ('Schmetterling Reisen','217'),
	 ('Schmetterling Reisen','211'),
	 ('Schmetterling Reisen','265'),
	 ('Schmetterling Reisen','256'),
	 ('Schmetterling Reisen','236'),
	 ('Schmetterling Reisen','235'),
	 ('Schmetterling Reisen','234');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Schmetterling Reisen','231'),
	 ('Schmetterling Reisen','226'),
	 ('Schmetterling Reisen','225'),
	 ('Schmetterling Reisen','224'),
	 ('Schmetterling Reisen','223'),
	 ('Schmetterling Reisen','222'),
	 ('Schmetterling Reisen','221'),
	 ('Schmetterling Reisen','220'),
	 ('Schmetterling Reisen','219'),
	 ('Schmetterling Reisen','211');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('EW Bus GmbH','40'),
	 ('EW Bus GmbH','32'),
	 ('EW Bus GmbH','26'),
	 ('EW Bus GmbH','1'),
	 ('Jenaer Nahverkehr GmbH','15'),
	 ('Jenaer Nahverkehr GmbH','AST'),
	 ('Jenaer Nahverkehr GmbH','12'),
	 ('JES Verkehrsgesellschaft mbH','440'),
	 ('JES Verkehrsgesellschaft mbH','426'),
	 ('JES Verkehrsgesellschaft mbH','424');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('JES Verkehrsgesellschaft mbH','422'),
	 ('JES Verkehrsgesellschaft mbH','420'),
	 ('KomBus GmbH','986'),
	 ('KomBus GmbH','720'),
	 ('KomBus GmbH','155'),
	 ('Omnibus Verkehrs Gesellschaft mbH Sonneberg/Thür,','A1'),
	 ('Omnibus Verkehrs Gesellschaft mbH Sonneberg/Thür,','A1'),
	 ('Omnibus Verkehrs Gesellschaft mbH Sonneberg/Thür,','720'),
	 ('Omnibus Verkehrs Gesellschaft mbH Sonneberg/Thür,','708'),
	 ('Personenverkehrsgesellschaft mbH Weimarer Land','291');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Personenverkehrsgesellschaft mbH Weimarer Land','291'),
	 ('VerkehrsAG Nürnberg','81'),
	 ('Verkehrsverbund Großraum Nürnberg','8112R'),
	 ('Verkehrsverbund Großraum Nürnberg','8103'),
	 ('Verkehrsverbund Großraum Nürnberg','343'),
	 ('Verkehrsverbund Großraum Nürnberg','197'),
	 ('Verkehrsverbund Großraum Nürnberg','133'),
	 ('Verkehrsverbund Großraum Nürnberg','132'),
	 ('Verkehrsverbund Großraum Nürnberg','865'),
	 ('Verkehrsverbund Großraum Nürnberg','839');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Verkehrsverbund Großraum Nürnberg','803'),
	 ('Verkehrsverbund Großraum Nürnberg','762'),
	 ('Verkehrsverbund Großraum Nürnberg','756'),
	 ('Verkehrsverbund Großraum Nürnberg','755'),
	 ('Verkehrsverbund Großraum Nürnberg','753'),
	 ('Verkehrsverbund Großraum Nürnberg','751'),
	 ('Verkehrsverbund Großraum Nürnberg','718'),
	 ('Verkehrsverbund Großraum Nürnberg','649'),
	 ('Verkehrsverbund Großraum Nürnberg','648'),
	 ('Verkehrsverbund Großraum Nürnberg','986');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Verkehrsverbund Großraum Nürnberg','985'),
	 ('Verkehrsverbund Großraum Nürnberg','A630'),
	 ('Verkehrsverbund Großraum Nürnberg','A620'),
	 ('Verkehrsverbund Großraum Nürnberg','750'),
	 ('Verkehrsverbund Rhein-Neckar','986 / 632'),
	 ('Verkehrsverbund Rhein-Neckar','82'),
	 ('Verkehrsverbund Rhein-Neckar','9852'),
	 ('Verkehrsverbund Rhein-Neckar','9872'),
	 ('Verkehrsverbund Rhein-Neckar','9870'),
	 ('Verkehrsgemeinschaft Schwäbisch Hall','R59');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Verkehrsgemeinschaft Schwäbisch Hall','R59'),
	 ('Verkehrsgemeinschaft Schwäbisch Hall','R58'),
	 ('Verkehrsgemeinschaft Schwäbisch Hall','R55'),
	 ('Verkehrsgemeinschaft Untermain','63'),
	 ('Würzburger Verkehrsverbund','8'),
	 ('Würzburger Verkehrsverbund','673'),
	 ('Würzburger Verkehrsverbund','672'),
	 ('Würzburger Verkehrsverbund','621'),
	 ('Würzburger Verkehrsverbund','620'),
	 ('Würzburger Verkehrsverbund','614');
INSERT INTO gtfs_germany.on_demand_services (agency_name,route_short_name) VALUES
	 ('Würzburger Verkehrsverbund','310');

CREATE INDEX ON gtfs_germany.on_demand_services(agency_name,route_short_name);

/*
Label touristic trains
*/
WITH agency_ids AS 
(
	SELECT agency_id 
	FROM gtfs_germany.agency a 
	WHERE a.agency_name IN ('Döllnitzbahngesellschaft', 'Wanderbahn im Regental', 'Nichtbundeseigene Eisenbahnen', 'Mainschleifenbahn', 'Chiemgauer Lokalbahn', 'Rhön-Zügle') 
)
UPDATE gtfs_germany.routes r
SET route_type = '107'
WHERE r.agency_id IN (SELECT * FROM agency_ids); 

UPDATE gtfs_germany.routes r 
SET route_type = '107'
WHERE route_long_name = 'Sonderzug';

/*
Label wrongly labelled train as bus 
*/
WITH agency_ids AS 
(
	SELECT a.*
	FROM gtfs_germany.agency a 
	WHERE a.agency_name IN ('Schmetterling Reisen', 'DB ZugBus Regionalverkehr Alb-Bodensee', 
	'Regensburger Verkehrsverbund', 'Gesellschaft zur Förderung des ÖPNV Regensburg', 'Landkreis Landsberg (Lech)') 
)
UPDATE gtfs_germany.routes r
SET route_type = '3'
WHERE r.agency_id IN (SELECT agency_id FROM agency_ids); 

/*
Label trains
*/
UPDATE gtfs_germany.routes r
SET route_type = '101'
WHERE route_type = '2'
AND route_long_name LIKE ANY (array['ICE', 'RJX', 'EuroCity-Express', 'Intercity-Express']); 

UPDATE gtfs_germany.routes r
SET route_type = '102'
WHERE route_type = '2'
AND route_long_name LIKE ANY (array['EuroCity', 'EC', 'Intercity', 'IC']);  

UPDATE gtfs_germany.routes r
SET route_type = '105'
WHERE route_type = '2'
AND route_long_name LIKE ANY (array['EuroNight', 'NJ', 'EN']);  

UPDATE gtfs_germany.routes r 
SET route_type = '102'
FROM gtfs_germany.agency a 
WHERE route_type = '2'
AND a.agency_name = 'FlixTrain'
AND r.agency_id = a.agency_id;

/*
Label On-Demand buses
*/
UPDATE gtfs_germany.routes r 
SET route_type = '715'
WHERE route_short_name LIKE 'AST%'; 

--Label on demand services in MVV
UPDATE gtfs_germany.routes r 
SET route_type = '715'
FROM gtfs_germany.agency a 
WHERE length(regexp_replace(route_short_name, '[^0-9]', '', 'g')) = 4
AND a.agency_name = 'Münchner Verkehrs- und Tarifverbund'
AND route_type = '3'
AND r.agency_id = a.agency_id;

--Label on demand services using custom list
WITH on_demand_routes AS 
(
	SELECT a.agency_id, o.*
	FROM gtfs_germany.on_demand_services o, gtfs_germany.agency a 
	WHERE o.agency_name = a.agency_name 
)
UPDATE gtfs_germany.routes r
SET route_type = '715'
FROM on_demand_routes o 
WHERE r.agency_id = o.agency_id 
AND r.route_short_name = o.route_short_name 
AND r.route_type = '3';

/*
Label Flixbus as Coach
*/
UPDATE gtfs_germany.routes r 
SET route_type = '200'
FROM gtfs_germany.agency a
WHERE a.agency_name = 'Flixbus'
AND route_type = '3'
AND r.agency_id = a.agency_id; 

DROP TABLE IF EXISTS gtfs_germany.trips_weekday;
CREATE TABLE gtfs_germany.trips_weekday (id serial, trip_id TEXT, route_type gtfs_germany."route_type_val", weekdays boolean[]);
INSERT INTO gtfs_germany.trips_weekday(trip_id, route_type, weekdays)
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
	FROM gtfs_germany.trips t, gtfs_germany.routes r
    WHERE t.route_id = r.route_id
 ) t, gtfs_germany.calendar c
WHERE t.service_id = c.service_id
AND '2022-02-07' >= start_date
AND '2022-02-13' <= end_date;
ALTER TABLE gtfs_germany.trips_weekday ADD PRIMARY KEY (id);
CREATE INDEX ON gtfs_germany.trips_weekday (trip_id);
CREATE TABLE gtfs_germany.stop_times_optimized (
    id serial4 NOT NULL,
    trip_id text NULL,
    arrival_time interval NULL,
    stop_id text NULL,
    route_type gtfs_germany."route_type_val" NULL,
    weekdays _bool NULL
);
INSERT INTO gtfs_germany.stop_times_optimized(trip_id, arrival_time, stop_id, route_type, weekdays)
SELECT st.trip_id, st.arrival_time, stop_id, route_type, weekdays
FROM gtfs_germany.stop_times st, gtfs_germany.trips_weekday w
WHERE st.trip_id = w.trip_id;
ALTER TABLE gtfs_germany.stop_times_optimized ADD PRIMARY KEY(id);
CREATE INDEX ON gtfs_germany.stop_times_optimized(stop_id, arrival_time);