import os
import yaml


with open(os.path.dirname(__file__) + "/../config/db.yaml", 'r') as stream:
        db_conf = yaml.load(stream, Loader=yaml.FullLoader)

DATABASE = {
    'user':     db_conf["USER"],
    'password': db_conf["PASSWORD"],
    'host':     db_conf["HOST"],
    'port':     db_conf["PORT"],
    'dbname': db_conf["DB_NAME"]
}
