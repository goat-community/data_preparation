/*Adaptation of https://github.com/igor-suhorukov/openstreetmap_h3*/
DROP FUNCTION IF EXISTS to_short_h3_5;
CREATE FUNCTION to_short_h3_5(bigint) RETURNS int
AS $$ select ($1 & 'x000ffffff0000000'::bit(64)::bigint>>28)::bit(32)::int;$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;