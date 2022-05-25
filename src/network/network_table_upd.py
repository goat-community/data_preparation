network_table_upd = '''
DROP TABLE IF EXISTS ways_temp;
DROP TABLE IF EXISTS ways_vertices_pgr_temp;
CREATE TABLE ways_temp AS TABLE ways;
CREATE TABLE ways_vertices_pgr_temp AS TABLE ways_vertices_pgr;
DROP TABLE IF EXISTS ways;
DROP TABLE IF EXISTS ways_vertices_pgr;
ALTER TABLE ways_temp RENAME TO ways;
ALTER TABLE ways_vertices_pgr_temp RENAME TO ways_vertices_pgr;

ALTER TABLE ways ADD PRIMARY KEY (id);
ALTER TABLE ways_vertices_pgr ADD PRIMARY KEY (id);

CREATE INDEX ON ways USING GIST(geom);
CREATE INDEX ON ways_vertices_pgr USING GIST(geom);

ALTER TABLE "configuration" RENAME COLUMN tag_id TO class_id;
ALTER TABLE "configuration" RENAME COLUMN tag_key TO class_key;
ALTER TABLE "configuration" RENAME COLUMN tag_value TO class_value;

CREATE INDEX ON ways USING btree (bicycle);
CREATE INDEX ON ways USING btree (foot);
CREATE INDEX ON ways USING btree (source);
CREATE INDEX ON ways USING btree (target);

'''