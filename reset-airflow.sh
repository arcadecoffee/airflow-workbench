#!/bin/bash

export AIRFLOW_HOME=${PWD}/airflow

# initialize the database
airflow db reset