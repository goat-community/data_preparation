import os 
import psycopg2
from db.db import Database

def read_files(directory, target_type):
    files = []
    for f in os.listdir(directory):
        if f.endswith(target_type):
            files.append(directory + '/' + f)
    return files

print('\nPlease input the file location')
file_location = input()
print('\nPlease insert the table name and with columns. \n Like this DE_Grid_ETRS89_LAEA_100m (id,x_sw,y_sw,x_mp,y_mp,f_staat,f_land,f_wasser,p_staat,p_land,p_wasser,ags)')
table_name = input()
print('\nHas the file a CSV Header? Type yes or no.')
csv_header = input()
print('\nIf the script should a apply a custom endoding please pass it.')
csv_encoding = input()


if csv_header == 'yes':
    header_sql = 'CSV HEADER'
else:
    header_sql = ''

if csv_encoding != '':
    encoding_sql = "encoding '%s'" % csv_encoding 
else: 
    encoding_sql = ''


fnames = read_files(file_location, '.csv')

sql_command = """
    COPY %s
    FROM '%s'
    DELIMITER ';'
    %s
    %s
    ;
"""

db = Database()
for f in fnames:
    sql_execute = sql_command % (table_name,f,header_sql,encoding_sql)+ '\n'
    print(sql_execute)
    #db.perform(sql_execute)
    
    



