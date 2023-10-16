class POITable:
    def __init__(self, data_set_type: str, schema_name: str, data_set_name: str):
        self.data_set_type = data_set_type
        self.data_set_name = data_set_name
        self.schema_name = schema_name
        self.table_name = f"{self.data_set_type}_{self.data_set_name}"

        self.index_columns = "geom"
        self.index_method = "gist"

        # Define common columns
        self.common_columns = [
            "name text NULL",
            "operator text NULL",
            "street text NULL",
            "housenumber text NULL",
            "zipcode text NULL",
            "phone text NULL",
            "email text NULL",
            "website text NULL",
            "capacity text NULL",
            "opening_hours text NULL",
            "wheelchair text NULL",
            "source text NULL",
            "tags jsonb NULL",
            "geom geometry NOT NULL",
            "auto_pk SERIAL NOT NULL",
            f"CONSTRAINT {self.table_name}_pkey PRIMARY KEY (auto_pk)"
        ]

    def create_table(self, table_name: str, category_columns: list) -> str:
        all_columns = category_columns + self.common_columns
        all_columns_str = ",\n".join(all_columns)
        sql_create_table = f"""
            DROP TABLE IF EXISTS {self.schema_name}.{table_name};
            CREATE TABLE {self.schema_name}.{table_name} (
                {all_columns_str}
            );
            CREATE INDEX ON {self.schema_name}.{table_name} USING {self.index_method} ({self.index_columns});
            """
        return sql_create_table

    def create_poi_table(self) -> str:
        table_name = f"{self.data_set_type}_{self.data_set_name}"
        category_columns = [
            "category text NULL",
            "other_categories text[] NULL"
        ]
        return self.create_table(table_name, category_columns)

    def create_poi_childcare(self) -> str:
        table_name = f"{self.data_set_type}_childcare_{self.data_set_name}"
        category_columns = [
            "nursery bool NULL",
            "kindergarten bool NULL",
            "after_school bool NULL"
        ]
        return self.create_table(table_name, category_columns)

    def create_poi_school(self) -> str:
        table_name = f"{self.data_set_type}_school_{self.data_set_name}"
        category_columns = [
            "school_isced_level_1 bool NULL",
            "school_isced_level_2 bool NULL",
            "school_isced_level_3 bool NULL"
        ]
        return self.create_table(table_name, category_columns)
