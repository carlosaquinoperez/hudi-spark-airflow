#!/usr/bin/env bash
airflow db init
airflow users create \
    --username admin \
    --password admin \
    --firstname Carlos \
    --lastname Admin \
    --role Admin \
    --email admin@example.com

exec airflow webserver