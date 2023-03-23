# Data preparation
This is a repository containing the data preparation steps for GOAT. 


# Start Database Docker Container and Connect

1. Create your personal .env from .env.template
2. Create your personal id.rsa and id.rsa.pub from the templates
3. Run `docker-compose up -d`
4. Work inside the docker container

# Import GTFS data

docker run --rm --network=data_preparation_data_preparation_proxy --volume $(pwd)/src/data/gtfs:/gtfs -e PGHOST={PGHOST} -e PGPASSWORD={PGPASSWORD} -e PGUSER={PGUSER} -e PGDATABASE={PGDATABASE} majkshkurti/gtfs-via-postgres:4.3.4 --trips-without-shape-id --schema gtfs  -- *.txt"
