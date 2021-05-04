#!/bin/bash

# Brew stuff installed: python3, numpy, rust, mysql, virtualvenv
# Had to pip3 install numpy==1.20.1 and pandas==1.2.2 at the system level first

python3 -m venv venv

source venv/bin/activate

AIRFLOW_VERSION=2.0.1
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# pip install --upgrade pip==20.2.4

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
