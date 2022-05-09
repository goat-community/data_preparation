from src.other.utility_functions import file2df, database_table2df, gdf_conversion
from src.collection.fusion import df2area
import sys
from src.config.config import Config
from src.db.db import Database

import pandas as pd 

config = Config("pois")
values = ['car_sharing', 'bike_sharing']

        #  ['atm','bakery','bank','bar','bike_sharing','biergarten','butcher','cafe','car_sharing','charging_station',
        #   'chemist','cinema','clothes','convenience','discount_gym','discount_supermarket', 'fast_food',
        #   'fuel','greengrocer','guest_house','gym','hairdresser','health_food','hostel','hotel','hypermarket','ice_cream',
        #   'international_hypermarket','kindergarten','kiosk','library','mall','marketplace','museum','nightclub','organic_supermarket',
        #   'parking','playground','post_box','pub','rail_station','recycling','restaurant',
        #   'shoes','subway_entrance','taxi','theatre','toilets','tram_stop','yoga']



db = Database()
con = db.connect()
db_rd = Database('reading')
con_rd = db_rd.connect_rd()

df_pois = database_table2df(con, 'pois', geometry_column='geom')
df_pois = df_pois[df_pois.amenity.isin(values)]

df_area = config.get_areas_by_rs(con_rd, buffer=8300)
df_pois = df2area(df_pois, df_area)

select = '''SELECT osm_id, split_part(poi_goat_id, '-', 3) AS amenity --, "name"
            FROM poi_goat_id;'''

df_goat_id = pd.read_sql(select, con_rd)

keys = list(df_goat_id.columns.values)
i1 = df_pois.set_index(keys).index
i2 = df_goat_id.set_index(keys).index
df_new_pois = df_pois[~i1.isin(i2)]

gdf_conversion(df_new_pois,'df_new_pois', 'GeoJSON')