from decouple import config

DATABASE = {
    'user':     config("POSTGRES_USER"),
    'password': config("POSTGRES_PASSWORD"),
    'host':     config("POSTGRES_HOST"),
    'port':     5432,
    'dbname':   config("POSTGRES_DB")
}

DATABASE_RD = {
    'user':     config("USER_RD"),
    'password': config("PASSWORD_RD"),
    'host':     config("HOST_RD"),
    'port':     config("PORT_RD"),
    'dbname':   config("DB_NAME_RD")
}

S3 = {
    'bucket': config("S3_BUCKET"),
    'access_key_id': config("ACCESS_KEY_ID"),
    'secret_access_key': config("SECRET_ACCESS_KEY")
}

REMOTE_SERVER = {
    'host': config("REMOTE_SERVER"),
    'user': config("REMOTE_SERVER_USER")
}