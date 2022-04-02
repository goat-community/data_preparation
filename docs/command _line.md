# Quick Start - Command line

## HowTo Command Line

### Start Docker and Prepare Database

1. Rename .env_template file to .env
2. Run `docker-compose up -d`
3. Connect using the credentials defined in .env
4. Use command line to prepare database `python prepare.py -db`.

### Collect Layers and Store it in Local Database

5. Configure collection(preparation) settings in `src/config/config.yaml` _*See related chapters_ 
6. Use command line to prepare layers in database tables `python collect.py -c LAYERNAME` (pois, network, landuse, buildings)

### (Optional) Fuse Custom Data with Stored One

7. Configure fusion(pois) settings in `src/config/config.yaml` _*See related chapters_
8. In case of use of data from geonode, specify credidentials defined in .env for remote database
9. Use command line to fuse data and store in layers in database tables `python collect.py -f LAYERNAME` (pois)

### Prepare Data for GOAT for defined municipality

10. Make sure that all neccesary tables existed in REMOTE database. _*See related chapter_
11. Use command line to fuse data and store in layers in database tables `python prepare.py -p LAYERNAME -m MUNICIPALITIES_CODES`
12. Resulting **_sql_** files will be stored in local database schema **_basic_**.   