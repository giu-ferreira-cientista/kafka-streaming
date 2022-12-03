#!/bin/bash

cd kafka

docker-compose up -d

cd ..

cd spark

docker-compose up -d  

cd ..

cd pinot

docker-compose up -d  

cd ..

cd superset

docker-compose up -d

cd ..

cd airflow

docker-compose up airflow-init 
docker-compose up -d 

cd ..


docker network create data-streaming
docker network connect data-streaming spark
docker network connect data-streaming pinot-controller
docker network connect data-streaming pinot-zookeeper
docker network connect data-streaming pinot-broker
docker network connect data-streaming pinot-server
docker network connect data-streaming kafka
docker network connect data-streaming zookeeper
docker network connect data-streaming kafka-ui


docker exec -t pinot-controller bin/pinot-admin.sh AddTable \
    -schemaFile examples/addtable/patient_schema.json \
    -tableConfigFile examples/addtable/patient_realtime_table_config.json \
    -exec

docker exec -t spark bash -c 'chmod -R 777 *'

