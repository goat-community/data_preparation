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

        # tags jsonb NULL, -> confidence, websites, socials, emails, phones --- gers_id/ id
        # TODO: add emails (currently only NULLs in orginial data set)
        create_tags_column = f"""
        ALTER TABLE temporal.places_{self.region}
        ADD COLUMN tags jsonb;

        UPDATE temporal.places_{self.region}
        SET tags = jsonb_build_object(
            'confidence', confidence,
            'website', websites,
            'social_media', socials,
            'phone', phones,
            'gers_id', id
        );
        """
        self.db.perform(create_tags_column)

        #TODO: new POI schema as e.g. source is missing

        self.db.perform(POITable(data_set_type="poi", schema_name="temporal", data_set_name=f"overture_{self.region}_raw").create_poi_table(table_type='standard'))

        insert_into_poi_table = f"""
            INSERT INTO temporal.poi_overture_{self.region}_raw(category, other_categories, name, street, housenumber, zipcode, tags, geom)
            SELECT
                categories,
                other_categories,
                names,
                street,
                housenumber,
                zipcode,
                tags,
                wkb_geometry
            FROM public.places_{self.region};
        """

        self.db.perform(insert_into_poi_table)

        categories = ', '.join(["'{}'".format(cat.replace("'", "''")) for cats in self.data_config.preparation['category'].values() for cat in cats])

        clean_data = f"""
            DROP TABLE IF EXISTS public.poi_overture_{self.region};
            CREATE TABLE public.poi_overture_{self.region} AS (
                SELECT *
                FROM public.poi_overture_{self.region}_raw
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
