#!/bin/bash
set -e

function update_database_with_postgis() {
    local db=$1
    echo "  Updating databse '$db' with extension"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<-EOSQL
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS pgrouting;
        CREATE EXTENSION IF NOT EXISTS hstore;
        CREATE EXTENSION IF NOT EXISTS intarray;
        CREATE EXTENSION IF NOT EXISTS plpython3u;
        CREATE EXTENSION IF NOT EXISTS arraymath;
        CREATE EXTENSION IF NOT EXISTS floatvec;
        CREATE EXTENSION IF NOT EXISTS postgis_raster;
        CREATE EXTENSION IF NOT EXISTS plv8;
EOSQL
}


if [ -n "$POSTGRES_DB_NAME" ]; then
	echo "Database creation requested: $POSTGRES_DB_NAME"
    update_database_with_postgis $POSTGRES_DB_NAME
	echo "Database created"
fi
