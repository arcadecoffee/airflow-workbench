#!/bin/bash

# Brew stuff installed: python3, numpy, rust, mysql, virtualvenv
# Had to pip3 install numpy and pandas at the system level first

virtualenv venv --pythons=3.9

source venv/bin/activate

AIRFLOW_VERSION=2.0.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install --upgrade pip==20.2.4

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
