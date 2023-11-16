from src.config.config import Config
from src.db.db import Database
from src.core.config import settings
from src.db.tables.poi import POITable

class OverturePOIPreparation:
    """Preparation of the places data set from the Overture Maps Foundation"""
    def __init__(self, db: Database, region: str = "de"):
        self.region = region
        self.db = db

        self.data_config = Config('poi_overture', region)
        self.data_config_preparation = self.data_config.preparation

    def run(self):

        # operator, capacity, opening_hours currently not in Overture data
        # TODO: add emails (currently only NULLs in orginial data set)
        # TODO: GERS ID once introduced in Overture data set

        self.db.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=f"overture_{self.region}_raw").create_poi_table(table_type='standard'))

        insert_into_poi_table = f"""
            INSERT INTO temporal.poi_overture_{self.region}_raw(category, other_categories, name, street, housenumber, zipcode, phone, website, source, tags, geom)
            SELECT
                categories,
                other_categories,
                names,
                street,
                housenumber,
                zipcode,
                CASE
                    WHEN cardinality(phones) > 0 THEN phones[1]::varchar
                    ELSE NULL
                END AS phone,
                CASE
                    WHEN cardinality(websites) > 0 THEN websites[1]::varchar
                    ELSE NULL
                END AS website,
                'Overture' AS source,
                (JSONB_STRIP_NULLS(
                    JSONB_BUILD_OBJECT(
                        'confidence', confidence,
                        'social_media', CASE
                            WHEN cardinality(socials) > 0 THEN socials[1]::varchar
                            ELSE NULL
                        END,
                        'brand', brand) || 
                    JSONB_BUILD_OBJECT('extended_source', JSONB_BUILD_OBJECT('ogc_fid', id))
                )) AS TAGS,            
                wkb_geometry
            FROM temporal.places_{self.region};
        """

        self.db.perform(insert_into_poi_table)

        categories = ', '.join(["'{}'".format(cat.replace("'", "''")) for cats in self.data_config.preparation['category'].values() for cat in cats])

        clean_data = f"""
            DROP TABLE IF EXISTS public.poi_overture_{self.region};
            CREATE TABLE public.poi_overture_{self.region} AS (
                SELECT *
                FROM temporal.poi_overture_{self.region}_raw
                WHERE category IS NOT NULL
                AND (tags ->> 'confidence')::numeric > 0.6
                AND category IN ({categories})
            );
        """
        self.db.perform(clean_data)

def prepare_poi_overture(region: str):
    db = Database(settings.LOCAL_DATABASE_URI)
    overture_poi_preparation = OverturePOIPreparation(db, region)
    overture_poi_preparation.run()
    db.conn.close()
