from src.config.config import Config
from src.db.db import Database
from src.core.config import settings

class OverturePOIPreparation:
    """Preparation of the places data set from the Overture Maps Foundation"""
    def __init__(self, db_rd: Database, region: str = "de"):
        self.region = region
        self.db_rd = db_rd

        self.data_config = Config('poi_overture', region)
        self.data_config_preparation = self.data_config.preparation

    def run(self):

        categories = ', '.join(["'{}'".format(cat.replace("'", "''")) for cats in self.data_config.preparation['category'].values() for cat in cats])

        clean_data = f"""
            DROP TABLE IF EXISTS temporal.poi_overture_{self.region};
            CREATE TABLE temporal.poi_overture_{self.region} AS (
                SELECT *
                FROM temporal.poi_overture_{self.region}_raw
                WHERE category_1 IS NOT NULL
                AND (tags ->> 'confidence')::numeric > 0.6
                AND category_1 IN ({categories})
            );
        """
        self.db_rd.perform(clean_data)

        # reclassify data -> overture categories to our categories
        for category, values in self.data_config_preparation['category'].items():

            overture_categories = ', '.join(["'{}'".format(cat.replace("'", "''")) for cat in values])

            update_query = f"""
                UPDATE temporal.poi_overture_{self.region}
                SET category_1 = '{category}'
                WHERE category_1 IN ({overture_categories});
            """
            self.db_rd.perform(update_query)

def prepare_poi_overture(region: str):
    db_rd = Database(settings.RAW_DATABASE_URI)
    overture_poi_preparation = OverturePOIPreparation(db_rd, region)
    overture_poi_preparation.run()
