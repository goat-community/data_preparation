# POIs

In order to update the OSM data, you can use the command line tool kollekt.po. To do this, you must first collect the data using the same tool 
as described in the [Date Collection](/app/docs/data_collection/pois.md) section. After the fresh data will be loaded in the form of a table (pois) in the local database, 
you need to make adjustments in the file **config.yaml**.

It could be run with the help of command line command `python collect.py -c LAYERNAME`. (pois)

```
update :
            rs_set:    ['091620000', '095640000', ...]
            categories : ['car_sharing', 'bike_sharing', ...]
            
```
It is necessary to specify **rs_set** for municipalities to be updated. In **categories** list categories the data for which should be updated. **Be sure that the OSM data of category you are going to update were not replaced with fusion process!**

In the next step update action could be executed with the help of command line command `python collect.py -u LAYERNAME`. (pois)

Existed table (**poi**) in a local database will be copied to local database and new data will be inserted. (For each new poi *uid* will be generated).

To replace existed (old) table in remote database in is necessary to transfer data from local database.
For this purpose run from command line `python collect.py -t LAYERNAME`. (pois) Old table will be dumped and saved as sql file locally under *src/data/sql_dumps/poi_geonode_ddmmyy_HHMMSS*. After that table will be replaced and **poi_goat_id** table will be updated. 

