from src.db.db import Database
from src.config.config import Config
from src.core.config import settings
from src.utils.utils import print_info

class PopulationPreparation():
    
    def __init__(self, db: Database, region: str):

        self.db = db
        self.region = region
        # Get config for population
        #self.config = Config("population", region)
        
        
    def disaggregate_population(self, sub_study_area_id: int):
        
        
        print_info(f"Disaggregate population for sub study area {sub_study_area_id}")
        # Get sum of gross floor area of buildings in sub study area
        sql_sum_gross_floor_area = f"""
            SELECT SUM(gross_floor_area_residential) AS sum_gross_floor_area
            FROM basic.building b, basic.sub_study_area s
            WHERE s.id = {sub_study_area_id}
            AND ST_Intersects(b.geom, s.geom)
            AND ST_Intersects(ST_CENTROID(b.geom), s.geom)
            AND residential_status = 'with_residents'
        """
        sum_gross_floor_area = self.db.select(sql_sum_gross_floor_area)[0][0]
        
        if sum_gross_floor_area is None:
            return
        
        sql_disaggregate_population = f"""
            INSERT INTO temporal.population (population, building_id, geom, sub_study_area_id)
            SELECT gross_floor_area_residential::float / {sum_gross_floor_area}::float * s.population::float AS population, b.id, ST_CENTROID(b.geom), s.id 
            FROM basic.building b, basic.sub_study_area s
            WHERE s.id = {sub_study_area_id}
            AND ST_Intersects(b.geom, s.geom)
            AND ST_Intersects(ST_CENTROID(b.geom), s.geom)
            AND residential_status = 'with_residents'
        """
        
        self.db.perform(sql_disaggregate_population)
        
        
    def run(self, study_area_ids: list[int]):
        
        study_area_ids = [5334,5358,5370,8315,8316,9161,9163,9173,9174,9175,9177,9178,9179,9184,9186,9188,9261,9262,9263,9274,9361,9362,9363,9461,9462,9463,9464,9474,9561,9562,9563,9564,9565,9572,9573,9574,9576,9661,9662,9663,9761,9762,9763,9764,14626,83110000,91620000]

        sql_sub_study_area_ids = f"SELECT id FROM basic.sub_study_area WHERE study_area_id IN ({str(study_area_ids)[1:-1]});"
        sub_study_area_ids = self.db.select(sql_sub_study_area_ids)
        sub_study_area_ids = [id for id, in sub_study_area_ids]
        
        # Create temporal population table
        sql_create_population_table = """
            DROP TABLE IF EXISTS temporal.population;
            CREATE TABLE temporal.population AS
            SELECT * 
            FROM basic.population
            LIMIT 0;
        """
        self.db.perform(sql_create_population_table)
        
        for sub_study_area_id in sub_study_area_ids:
            self.disaggregate_population(sub_study_area_id)
    

def main():
    
    study_area_ids = [5334,5358,5370,8315,8316,9161,9163,9173,9174,9175,9177,9178,9179,9184,9186,9188,9261,9262,9263,9274,9361,9362,9363,9461,9462,9463,9464,9474,9561,9562,9563,9564,9565,9572,9573,9574,9576,9661,9662,9663,9761,9762,9763,9764,14626,83110000,91620000]
    db_rd = Database(settings.REMOTE_DATABASE_URI)
    PopulationPreparation(db_rd, "de").run(study_area_ids)
     
if __name__ == "__main__":
    main()   