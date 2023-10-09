# function returns sql as string with input variables
# input: data set type e.g. poi, schema name e.g. temporal, data set e.g. public_transport_stop

#TODO: add other_id

def create_poi_table(data_set_type: str, schema_name: str, data_set: str) -> str:

    sql_create_poi_table = f"""
        DROP TABLE IF EXISTS {schema_name}.{data_set_type}_{data_set};
        CREATE TABLE {schema_name}.{data_set_type}_{data_set} (
            category_1 text,
            category_2 text,
            category_3 text,
            category_4 text,
            category_5 text,
            name text NULL,
            street text NULL,
            housenumber text NULL,
            zipcode text NULL,
            opening_hours text NULL,
            wheelchair text NULL,
            tags jsonb NULL,
            geom public.geometry(point, 4326) NOT NULL,
            id INT8 NOT NULL,
            CONSTRAINT {data_set_type}_{data_set}_pkey PRIMARY KEY (id)
        );
        CREATE INDEX {schema_name}_{data_set_type}_{data_set}_geom_idx ON {schema_name}.{data_set_type}_{data_set} USING gist (geom);
        """

    return sql_create_poi_table
