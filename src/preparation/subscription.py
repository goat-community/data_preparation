from src.config.config import Config
from src.db.db import Database
from src.db.tables.poi import POITable
from src.collection.kart.prepare_kart import PrepareKart
from uuid import uuid4
from src.utils.utils import print_info, timing


class Subscription:
    """Class to prepare the POIs from OpenStreetMap."""

    def __init__(self, db: Database, region: str):
        """Constructor method.

        Args:
            db (Database): Database object
        """
        self.db = db
        self.db_config = self.db.db_config
        self.db_uri = f"postgresql://{self.db_config.user}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}{self.db_config.path}"
        self.engine = self.db.return_sqlalchemy_engine()

        self.table_name = "poi"
        self.region = region
        self.config_pois = Config(self.table_name, region)
        self.config_pois_preparation = self.config_pois.preparation
        self.repo_url = self.config_pois.subscription["repo_url"]
        self.batch_size = 10000
        self.max_commit_size = 100000

        self.prepare_kart = PrepareKart(
            self.db,
            repo_url=self.repo_url,
            maintainer=self.db_config.user,
            table_name=self.table_name,
        )
        self.kart_schema = f'kart_{self.table_name}s'

        # Get the date of the OSM data. In upstream functions it is guaranteeed that all dates are the same
        self.osm_data_date = self.db.select("SELECT date FROM poi_osm_boundary LIMIT 1")[0][0]

    def get_source_table(self, category):
        # get source to get table name and source
        source = self.db.select(f"""SELECT source from {self.kart_schema}.data_subscription WHERE category = '{category}' AND rule = 'subscribe'""")[0][0]

        if source == 'OSM':
            return f'public.poi_osm_{self.region}'
        elif source == 'Overture':
            return f'public.poi_overture_{self.region}'
        elif source == 'OSM_Overture':
            return f'temporal.poi_osm_overture_{self.region}_fusion_result'

    def get_kart_poi_table_name(self, category):
        # find correct poi table name within kart
        sql_kart_poi_table_name = f"""
            SELECT table_name
            FROM {self.kart_schema}.poi_categories
            WHERE category = '{category}';
        """
        return self.db.select(sql_kart_poi_table_name)[0][0]

    @timing
    def read_poi(self, category: str):
        """Method to read the relevant POIs from one category into a temporary table based on the subscription criteria.

        Args:
            category (str): Category of POIs to read
        """
        # get the area where where the category is subscribed to either OSM, Overture or OSM_Overture
        sql_geom_ref_ids_subscribe = f"""
            SELECT geom_ref_id
            FROM {self.kart_schema}.data_subscription
            WHERE category = '{category}'
            AND rule = 'subscribe'
        """
        geom_ref_ids_subscribe = self.db.select(sql_geom_ref_ids_subscribe)

        if geom_ref_ids_subscribe == []:
            print_info(f"No geom for subscription of category {category}.")
        else:
            # Join all returned values into a comma-separated string with quotes around each value
            geom_ref_ids_subscribe = "', '".join([str(row[0]) for row in geom_ref_ids_subscribe])

            sql_geom_filter = f"""
                DROP TABLE IF EXISTS temporal.geom_filter_subscribe;
                CREATE TABLE temporal.geom_filter_subscribe AS
                WITH geom_refs_excluded_areas AS (
                    SELECT geom_ref_id
                    FROM {self.kart_schema}.data_subscription
                    WHERE category = '{category}'
                    AND rule = 'exclude'
                ),
                geom_individual_source AS (
                    SELECT ST_Union(n.geom) AS geom
                    FROM {self.kart_schema}.geom_ref n
                    WHERE n.id IN (SELECT geom_ref_id FROM geom_refs_excluded_areas)
                ),
                geom_union_subscribe AS (
                    SELECT ST_Union(g.geom) AS geom
                    FROM {self.kart_schema}.geom_ref g
                    WHERE g.id IN ('{geom_ref_ids_subscribe}')
                )
                SELECT
                    CASE
                        WHEN NOT EXISTS (SELECT 1 FROM geom_refs_excluded_areas) THEN
                            s.geom
                        ELSE
                            ST_Difference(s.geom, i.geom)
                    END AS geom
                FROM geom_union_subscribe s, geom_individual_source i;
            """
            self.db.perform(sql_geom_filter)

        # get the area covered by individual sources
        sql_geom_ref_ids_exclude = f"""
            SELECT geom_ref_id
            FROM {self.kart_schema}.data_subscription
            WHERE category = '{category}'
            AND rule = 'exclude'
        """
        sql_geom_ref_ids_exclude = self.db.select(sql_geom_ref_ids_exclude)

        if sql_geom_ref_ids_exclude == []:
            print_info(f"No individual source for category {category}.")
        else:
            # Join all returned values into a comma-separated string with quotes around each value
            sql_geom_ref_ids_exclude = "', '".join([str(row[0]) for row in sql_geom_ref_ids_exclude])

            sql_geom_filter = f"""
                DROP TABLE IF EXISTS temporal.geom_filter_exclude;
                CREATE TABLE temporal.geom_filter_exclude AS
                SELECT ST_Union(geom) as geom
                FROM {self.kart_schema}.geom_ref n
                WHERE n.id IN ('{sql_geom_ref_ids_exclude}')
            """
            self.db.perform(sql_geom_filter)

            # drop entries from specific poi table where temporal.geom_filter_exclude intersects and category = currenty category
            sql_delete_poi = f"""
                DELETE FROM {self.kart_schema}.{self.get_kart_poi_table_name(category)} p
                WHERE ST_Intersects(p.geom, (SELECT geom FROM temporal.geom_filter_exclude))
                AND p.category = '{category}';
            """
            self.db.perform(sql_delete_poi)
        # Get the pois to integrate and check if they fit the geometry filter and validation rules
        # TODO: validation here or at the end of poi preparation or both?

        create_table_sql = POITable(data_set_type=f'{self.table_name}', schema_name = 'temporal', data_set_name ='to_seed').create_poi_table(table_type='standard')
        self.db.perform(create_table_sql)

        sql_create_poi_to_integrate = f"""
            INSERT INTO temporal.{self.table_name}_to_seed(category, other_categories, name, operator, street, housenumber, zipcode, phone, email, website, capacity, opening_hours, wheelchair, source, tags, geom)
            SELECT
                category,
                other_categories,
                CASE WHEN TRIM(name) = '' THEN NULL ELSE TRIM(name) END,
                CASE WHEN TRIM(operator) = '' THEN NULL ELSE TRIM(operator) END,
                CASE WHEN TRIM(street) = '' THEN NULL ELSE TRIM(street) END,
                CASE WHEN TRIM(housenumber) = '' THEN NULL ELSE TRIM(housenumber) END,
                CASE WHEN TRIM(zipcode) = '' THEN NULL ELSE TRIM(zipcode) END,
                CASE WHEN TRIM(phone) = '' THEN NULL ELSE TRIM(phone) END,
                CASE WHEN octet_length(TRIM(email)) BETWEEN 6 AND 320 AND TRIM(email) LIKE '_%@_%.__%' THEN TRIM(email) ELSE NULL END,
                CASE WHEN TRIM(website) ~* '^[a-z](?:[-a-z0-9\+\.])*:(?:\/\/(?:(?:%[0-9a-f][0-9a-f]|[-a-z0-9\._~!\$&''\(\)\*\+,;=:@])|[\/\?])*)?' :: TEXT THEN TRIM(website) ELSE NULL END,
                CASE WHEN TRIM(capacity) ~ '^[0-9\.]+$' THEN try_cast_to_int(TRIM(capacity)) ELSE NULL END,
                CASE WHEN TRIM(opening_hours) = '' THEN NULL ELSE TRIM(opening_hours) END,
                CASE WHEN TRIM(wheelchair) IN ('yes', 'no', 'limited') THEN TRIM(wheelchair) ELSE NULL END,
                source,
                tags,
                p.geom
            FROM {self.get_source_table(category)} p, temporal.geom_filter_subscribe f
            WHERE ST_Intersects(p.geom, f.geom)
            AND p.source in ('OSM', 'Overture', 'OSM_Overture', 'Overture_OSM')
            AND p.category = '{category}';
        """
        self.db.perform(sql_create_poi_to_integrate)

    @timing
    def get_row_count(self, category: str):
        """Counts the number of rows from the temporary table.

        Args:
            category (str): Category of POIs to read

        Returns:
            row_cnt (int): Number of rows to process
        """
        # Get the number of rows to insert and update
        sql_row_count = f"""
            SELECT COUNT(*) FROM temporal.{self.table_name}_to_seed;
        """
        row_cnt = self.db.select(sql_row_count)
        print_info(
            f"Number of rows to process for category {category}: {row_cnt[0][0]}"
        )
        return row_cnt[0][0]

    @timing
    def insert_poi(self, row_cnt: int, category: str):
        """Inserts the POIs into Kart POI table in case the extended source is not already present.

        Args:
            row_cnt (int): Number of rows to process
            category (str): Category of POIs to read
        """

        # Read rows in batches and insert them into Kart POI table
        for i in range(0, row_cnt, self.batch_size):
            # Create a temporary table for the batch data of to_seed
            sql_create_temp_to_seed = f"""
                DROP TABLE IF EXISTS temp_to_seed;
                CREATE TEMP TABLE temp_to_seed AS
                SELECT
                    category,
                    other_categories,
                    name,
                    operator,
                    street,
                    housenumber,
                    zipcode,
                    phone,
                    email,
                    website,
                    capacity,
                    opening_hours,
                    wheelchair,
                    source,
                    tags::text,
                    geom
                FROM temporal.{self.table_name}_to_seed
                ORDER BY tags ->> 'extended_source'
                LIMIT {self.batch_size}
                OFFSET {i};
                CREATE INDEX ON temp_to_seed USING gin((tags::jsonb -> 'extended_source') jsonb_path_ops);
            """
            self.db.perform(sql_create_temp_to_seed)

            # Insert the POIs if the extended source is not already in the database
            sql_insert_poi = f"""
                INSERT INTO {self.kart_schema}.{self.get_kart_poi_table_name(category)}(
                    category,
                    other_categories,
                    name,
                    operator,
                    street,
                    housenumber,
                    zipcode,
                    phone,
                    email,
                    website,
                    capacity,
                    opening_hours,
                    wheelchair,
                    source,
                    tags,
                    geom
                )
                SELECT p.*
                FROM temp_to_seed p
                LEFT JOIN {self.kart_schema}.{self.get_kart_poi_table_name(category)} k
                ON p.tags::jsonb -> 'extended_source' = k.tags::jsonb -> 'extended_source'
                WHERE k.tags::jsonb -> 'extended_source' IS NULL;
            """
            self.db.perform(sql_insert_poi)

            # Print the number of rows processed
            processed = i + self.batch_size
            if processed > row_cnt:
                processed = row_cnt

            print_info(
                f"Processed INSERT for {processed} rows out of {row_cnt} rows for category {category}"
            )
            # Commit to repository of max_commit_size rows have been reached or when all rows have been processed
            if processed % self.max_commit_size == 0 or processed == row_cnt:
                print_info(f"Kart Status: {self.prepare_kart.status()}")
                print_info(f"Commit changes for category {category} started")
                self.prepare_kart.commit(
                    f"Automatically INSERT category {category} with {processed} rows"
                )
                print_info(f"Commit changes for category {category} finished")

    @timing
    def update_poi(self, row_cnt: int, category: str):
        """Updates the POIs in Kart POI table in case the extended source is already present.

        Args:
            row_cnt (int): Number of rows to process
            category (str): Category of POIs to read
        """

        # Update the POIs if the extended source is already in the database and the attributes have changed
        for i in range(0, row_cnt, self.batch_size):
            # Create a temporary table for the batch data of to_seed
            sql_create_temp_to_seed = f"""
                DROP TABLE IF EXISTS temp_to_seed;
                CREATE TEMP TABLE temp_to_seed AS
                SELECT
                    category,
                    other_categories,
                    name,
                    operator,
                    street,
                    housenumber,
                    zipcode,
                    phone,
                    email,
                    website,
                    capacity,
                    opening_hours,
                    wheelchair,
                    source,
                    tags::text,
                    geom
                FROM temporal.{self.table_name}_to_seed
                ORDER BY tags ->> 'extended_source'
                LIMIT {self.batch_size}
                OFFSET {i};
                CREATE INDEX ON temp_to_seed USING gin((tags::jsonb -> 'extended_source') jsonb_path_ops);
            """
            self.db.perform(sql_create_temp_to_seed)

            sql_update_poi = f"""
                UPDATE {self.kart_schema}.{self.get_kart_poi_table_name(category)} p
                SET
                    category = s.category,
                    other_categories = s.other_categories,
                    name = s.name,
                    "operator" = s."operator",
                    street = s.street,
                    housenumber = s.housenumber,
                    zipcode = s.zipcode,
                    phone = s.phone,
                    email = s.email,
                    website = s.website,
                    capacity = s.capacity,
                    opening_hours = s.opening_hours,
                    wheelchair = s.wheelchair,
                    source = s.source,
                    tags = s.tags::text,
                    geom = s.geom
                FROM temp_to_seed s
                WHERE p.tags::jsonb ->> 'extended_source' = s.tags::jsonb ->> 'extended_source'
                AND (
                    p.category != s.category
                    OR p.name != s.name
                    OR p."operator" != s."operator"
                    OR p.street != s.street
                    OR p.housenumber != s.housenumber
                    OR p.zipcode != s.zipcode
                    OR p.phone != s.phone
                    OR p.email != s.email
                    OR p.website != s.website
                    OR p.capacity != s.capacity
                    OR p.opening_hours != s.opening_hours
                    OR p.wheelchair != s.wheelchair
                    OR p.source != s.source
                    OR p.tags != s.tags::text
                    OR ST_ASTEXT(p.geom) != ST_ASTEXT(s.geom)
                );
            """
            self.db.perform(sql_update_poi)

            # Print the number of rows processed
            processed = i + self.batch_size
            if processed > row_cnt:
                processed = row_cnt

            print_info(
                f"Processed UPDATE for {processed} rows out of {row_cnt} rows for category {category}"
            )

            # Commit to repository of max_commit_size rows have been reached or when all rows have been processed
            if processed % self.max_commit_size == 0 or processed == row_cnt:
                print_info(f"Kart Status: {self.prepare_kart.status()}")
                print_info(f"Commit changes for category {category} started")
                self.prepare_kart.commit(
                    f"Automatically updated category {category} with {processed} rows"
                )
                print_info(f"Commit changes for category {category} finished")

    # TODO: Revise delete function as it is not working properly when the inpute extent is smaller then the extent of the POIs table -> not sure if still the cae
    @timing
    def delete_poi(self):
        """Deletes POIs that are not in the OSM, Overture or OSM_Overture data anymore"""

        # SQL query to delete the POIs if the extended source is not in the temporal table
        # It is important to run this for all categories at once as it might be that is not deleted but received a new category.
        sql_kart_poi_table_names = f"""
            SELECT DISTINCT p.table_name
            FROM {self.kart_schema}.poi_categories p, {self.kart_schema}.data_subscription s
            WHERE p.category IN (SELECT DISTINCT category FROM {self.kart_schema}.data_subscription WHERE "source" in ('OSM', 'Overture', 'OSM_Overture'))
            """
        kart_poi_table_names = self.db.select(sql_kart_poi_table_names)
        kart_poi_table_names = [table_name[0] for table_name in kart_poi_table_names]

        for kart_poi_table_name in kart_poi_table_names:
            # i need to find out for every table the source table name, but this could be multiple ones for a single table
            # ran all tables for all sources? could check prior which a relevant

            # based on kart_poi_table_name find all categories in poi_categories
            categories = self.db.select(f"""SELECT category FROM {self.kart_schema}.poi_categories WHERE table_name = '{kart_poi_table_name}'""")

            # Convert categories into a comma-separated string of quoted values
            categories = ', '.join(f"'{category[0]}'" for category in categories)

            # with the list of categories find the sources in data_subscription
            sources = self.db.select(f"""SELECT DISTINCT source FROM {self.kart_schema}.data_subscription WHERE category IN ({categories}) and rule = 'unsubscribe'""")
            # iwie den source table name finden bzw. nur über die loopen die da sind oder if else

            for source in sources:

                if source[0] == 'OSM':
                    source_table = f'public.poi_osm_{self.region}'
                elif source[0] == 'Overture':
                    source_table = f'public.poi_overture_{self.region}'
                elif source[0] == 'OSM_Overture':
                    source_table = f'temporal.poi_osm_overture_{self.region}_fusion_result'

                sql_delete_poi = f"""
                    WITH geom_to_check AS (
                        Select ST_Union(g.geom) as geom
                        FROM {self.kart_schema}.geom_ref g
                        WHERE g.id IN (SELECT geom_ref_id FROM {self.kart_schema}.data_subscription WHERE category IN ({categories}) and rule = 'subscribe')
                    ),
                    WITH poi_to_check AS
                    (
                        SELECT *
                        FROM {source_table} s, geom_to_check f
                        WHERE ST_Intersects(s.geom, f.geom)
                    )
                    to_delete AS
                    (
                        SELECT p.tags
                        FROM {self.kart_schema}.{kart_poi_table_name} p
                        LEFT JOIN poi_to_check o
                        ON p.tags::jsonb ->> 'extended_source' = o.tags::jsonb ->> 'extended_source'
                        WHERE o.tags::jsonb ->> 'extended_source' IS NULL
                        AND p.tags::jsonb ->> 'extended_source' IS NOT NULL
                        AND p.source = '{source[0]}'
                    )
                    DELETE FROM {self.kart_schema}.{kart_poi_table_name} p
                    USING to_delete d
                    WHERE p.tags::jsonb ->> 'extended_source' = d.tags::jsonb ->> 'extended_source';
                """
                self.db.perform(sql_delete_poi)

        # Commit to repository
        self.prepare_kart.commit("Automatically DELETE POIs that are not in OSM, Overture or OSM_Overture anymore")


    def update_date_subscription(self, category: str):
        """Updates the date of the data subscription table for the given category. """

        sources = self.db.select(f"""SELECT DISTINCT source FROM {self.kart_schema}.data_subscription WHERE rule = 'subscribe'""")

        for source in sources:
            if source[0] == 'OSM':
                source_date = self.osm_data_date.replace(tzinfo=None)
            elif source[0] == 'Overture':
                source_date = self.db.select(f"SELECT updatetime FROM temporal.places_{self.region} LIMIT 1")[0][0].replace(tzinfo=None)
            elif source[0] == 'OSM_Overture':
                osm_date = self.osm_data_date.replace(tzinfo=None)
                overture_date = self.db.select(f"SELECT updatetime FROM temporal.places_{self.region} LIMIT 1")[0][0].replace(tzinfo=None)
                # Use the earlier date
                source_date = min(osm_date, overture_date)

            sql_update_date = f"""
                UPDATE {self.kart_schema}.data_subscription
                SET source_date = '{source_date}'
                WHERE rule = 'subscribe'
                AND category = '{category}'
                AND source = '{source[0]}'
            """
            self.db.perform(sql_update_date)

        # Commit to repository
        self.prepare_kart.commit(f"Automatically updated date of data subscription for category {category}")

    def subscribe_osm(self):

        # Prepare a fresh kart repo
        self.prepare_kart.prepare_kart()

        # Create a new kart branch
        branch_name = uuid4().hex
        self.prepare_kart.create_new_branch(branch_name)
        self.prepare_kart.checkout_branch(branch_name)

        # Get categories to update
        sql_get_categories_to_update = f"""
            SELECT DISTINCT category
            FROM {self.kart_schema}.data_subscription
            WHERE source IN ('OSM', 'OSM_Overture', 'Overture');
        """
        categories = self.db.select(sql_get_categories_to_update)
        categories = [category[0] for category in categories]

        # Update each category and create for each a new kart commit
        for i, category in enumerate(categories, start=1):
            print_info(f"Processing category {i} of {len(categories)}")

            # Perform integration
            self.read_poi(category)
            row_cnt = self.get_row_count(category)
            self.insert_poi(row_cnt, category)
            self.update_poi(row_cnt, category)
            self.update_date_subscription(category)

        # Delete POIs that are not in the OSM, Overture or OSM_Overture data anymore
        self.delete_poi()

        # Push changes to remote
        self.prepare_kart.push(branch_name)

        # Create body message for PR
        categories_str = "\n".join(categories)
        pr_body = f"""
            This PR was created automatically by the POI subscription workflow.
            It contains the latest POIs from OSM and Overture for the following categories:
            {categories_str}
        """
        # Create PR on main branch
        self.prepare_kart.create_pull_request(
            branch_name= branch_name,
            base_branch="main",
            title="Automated PR for subscribed POIs",
            body=pr_body,
        )
