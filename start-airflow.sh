#!/bin/bash

export AIRFLOW_HOME=${PWD}/airflow
export SF_ENV=local

# initialize the database
airflow db init

airflow users create \
    --username admin \
    --password admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

export AIRFLOW__CORE__DAGS_FOLDER=${PWD}/dags
export AIRFLOW__CORE__PLUGINS_FOLDER=${PWD}/plugins

# start the web server, default port is 8080
airflow webserver --port 8080 -D

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler -D
