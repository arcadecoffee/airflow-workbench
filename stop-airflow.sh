#!/bin/bash

export AIRFLOW_HOME=${PWD}/airflow

cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill
cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill
