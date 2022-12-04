# kafka-streaming

Airflow:

    - https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Kafka:

    - https://developer.confluent.io/quickstart/kafka-docker/?build=apps

Spark:

    - Referencia docker compose Jupyter + All Spark - https://hub.docker.io

    - docker exec -t spark bash -c 'chmod -R 777 *'

    - Execucao Scripts Jupyter

Pinot:

    - https://docs.pinot.apache.org/basics/getting-started/running-pinot-in-docker

    - Connect to Kafka Table

    - docker exec -it pinot-controller bin/pinot-admin.sh AddTable \
            -schemaFile examples/addtable/patient_schema.json \
            -tableConfigFile examples/addtable/patient_realtime_table_config.json \
            -exec

Superset:

    - https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/

    - Github fork

    - pip install pinotdb

    - Connect to Pinot Realtime Table

    - docker exec \
        -t superset_app \
        bash -c 'superset import-dashboards -p /superset/import/dashboard_pinot_superset_add_historico.zip'

Docker network:    

    - docker network create 

    - docker network connect all containers