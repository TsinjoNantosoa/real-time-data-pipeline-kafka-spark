#!/bin/bash
set -e

# Installer les dépendances si présentes
if [ -e "/opt/airflow/requirement.txt" ]; then
  pip install -r /opt/airflow/requirement.txt
fi

# Initialisation de la DB et création de l'utilisateur admin si la base n'existe pas
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade toujours la DB au cas où
airflow db upgrade

# Démarre le webserver
exec airflow webserver
