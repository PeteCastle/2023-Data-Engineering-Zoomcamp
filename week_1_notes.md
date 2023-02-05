# Lecture Notes
## Running PostgreSQL
```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi"\
    -v "D:/Educational Others/2023 Data Engineering Zoomcamp/ny_taxi_postgres_data":/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network\
    --name=pg-database\
    postgres:13
```

## Connecting to PostgreSQL

`pgcli -h localhost -p 5432 -u root d -d ny_taxi`

## PGADMIN
```
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com"\
    -e PGADMIN_DEFAULT_PASSWORD="root"\
    -p 8080:80\
    --network=pg-network\
    --name=pg-admin1\
    dpage/pgadmin4
```

## Docker Networks
`docker network create pg-network`

## Python Script
```
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
```
## Building Python
`docker build -t taxi_ingest:v001 .`

## MAKE IT TO DOCKER!
```
docker run taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
```

## DOCKER PUSH
Used to publish your image to the web
`docker image push petecastle/prefect:zoom`

Note that "petecastle" must be the username

## DOCKER COMPOSE
`docker-compose up`

`docker-compose stop`

`docker-compose down`



# Assignment 1 Notes:
## Pipeline for Green Trips Dataset
```
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
```
## Docker Version
```
docker run taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
```