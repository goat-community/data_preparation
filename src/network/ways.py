import sys
import os
from types import prepare_class
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
import yaml
from pathlib import Path
from db.db import Database
from config.config import Config


class Profiles:
    def __init__(self, ways_table, filter_ways):

        db = Database()
        self.ways_table = ways_table
        self.filter_ways = filter_ways
        self.batch_size = 200
        self.elevs_interval = 25
        self.var_container = Config('ways').preparation

        conn = db.connect()
        cur = conn.cursor()
        
        cur.execute('''SELECT count(*) FROM {0} {1};'''.format(self.ways_table, self.filter_ways))
        self.total_cnt = cur.fetchall()[0][0]
        
        self.meter_degree = self.var_container['one_meter_degree']

        cur.execute('''SELECT id FROM {0} {1};'''.format(self.ways_table, self.filter_ways))
        ids = cur.fetchall()
        self.ids = [x[0] for x in ids]

        conn.close()
        
        
    def slope_profile(self): 
        db = Database()
        conn = db.connect() 
        cur = conn.cursor()

        sql_create_table = '''DROP TABLE IF EXISTS slope_profile;
                            CREATE TABLE slope_profile
                            (
                             id integer,
                             imp float,
                             rs_imp float,
                             elevs _float8,
                             linklength float8,
                             lengthinterval float8
                            );'''.format(self.ways_table)

        cur.execute(sql_create_table)
        conn.commit()

        cnt = 0
        for i in self.ids:         
            cnt = cnt + 1 
            if (cnt/self.batch_size).is_integer():
                print('Slope profile for %s out of %s lines' % (cnt,self.total_cnt)) 
                conn.commit()
            sql_slope = '''
            INSERT INTO slope_profile(id, imp, rs_imp, elevs, linklength, lengthinterval)
            SELECT w.id, ci.imp, ci.rs_imp, sp.*
            FROM ways w,
            LATERAL get_slope_profile(w.id, 10, 'ways') sp, LATERAL compute_impedances(COALESCE(sp.elevs, ARRAY[0,0]::float[]), sp.linkLength, 10) ci
            WHERE w.id = {0}
            ;'''.format(i)
            try:
                cur.execute(sql_slope)
            except Exception as inst:
                print(type(inst))
                print(inst.args)
                print(inst)
                print(f'The problem id is {i}')
                pass

        conn.commit()

        sql_null_false = '''UPDATE slope_profile 
            SET elevs = NULL 
            WHERE ARRAY_LENGTH(elevs,1) = 1
        '''.format(self.ways_table)
        cur.execute(sql_null_false)

        cur.execute('ALTER TABLE slope_profile ADD PRIMARY KEY (id);'.format(self.ways_table))
        conn.commit() 
        conn.close()
    
class PrepareLayers():
    """Data layers such as population as prepared with this class."""
    def __init__(self,layer):
        config = Config(layer)
        self.variable_container = config.preparation
        self.db = Database()

    def check_table_exists(self, table_name):
        self.db_conn = self.db.connect()
        return self.db.select('''SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = %(table_name)s);''', params={"table_name": table_name})[0][0]

    def ways(self):

        from src.network.network_preparation1 import network_preparation1
        self.db.perform(query = network_preparation1)
        from src.network.network_preparation2 import network_preparation2
        self.db.perform(query = network_preparation2)
        from src.network.network_table_upd import network_table_upd
        self.db.perform(query = network_table_upd)

        if self.variable_container["compute_slope_impedance"][1:-1] == 'yes':
            slope_profiles = Profiles(ways_table='ways', filter_ways=f'''WHERE class_id::text NOT IN (SELECT UNNEST(ARRAY{self.variable_container['excluded_class_id_cycling']}::text[]))''')
            print('Computing slopes...')
            slope_profiles.slope_profile()

            db = Database()
            conn = db.connect() 
            cur = conn.cursor()
            update_ways = '''UPDATE ways w  
                SET s_imp = s.imp, rs_imp = s.rs_imp 
                FROM slope_profile s 
                WHERE w.id = s.id;''' 
            cur.execute(update_ways)
            conn.commit()
            conn.close()

##==========================================OUTDATED===============================================###
