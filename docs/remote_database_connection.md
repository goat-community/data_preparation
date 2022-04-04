# Remote database connection
Some data preparation operations require a connection to a remote database. For this purpose it is necessary to make certain connection settings. 

Some data preparation operations require a connection to a remote database. For this purpose it is necessary to make certain connection settings. This can be done in the file .env file. It is necessary to specify the credidentials for the remote connection in the second part of the settings. The first part refers to the Docker database. After the settings are made, it is possible to interact with the remote database given in fusion.

```yaml
...
## Remote connection credits
HOST_RD=host
PORT_RD=port
DB_NAME_RD=database_name
USER_RD=user_name
PASSWORD_RD=password
```