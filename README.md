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
        service:
            networks:
            static-network:
                ipv4_address: 172.10.128.103

    - networks:
        static-network:     
            name: static-network
            ipam:
                config:
                    - subnet: 172.10.0.0/16
            #docker-compose v3+ do not use ip_range
            ip_range: 172.18.5.0/24
            external: true
    
    docker network connect static-network spark
    docker network connect static-network pinot-controller
    docker network connect static-network superset_app