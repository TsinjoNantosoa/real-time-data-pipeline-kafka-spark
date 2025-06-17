#!/bin/bash
set -e
if [ -e "/opt/airflow/requirement.txt" ]; then
  $(command -v pip) install --user -r requirement.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
  -- username admin \
  -- firstname admin \
  -- lastname admin \
  -- role Admin \
  -- email admin@example.com \
  -- password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver