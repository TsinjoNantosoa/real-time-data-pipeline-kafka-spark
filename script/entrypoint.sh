#!/bin/bash
set -e

# Installer les dépendances si présentes
if [ -e "/opt/airflow/requirements.txt" ]; then
  pip install -r /opt/airflow/requirements.txt
fi

# Upgrade toujours la DB
airflow db upgrade

# Crée un utilisateur admin si nécessaire
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# Démarre le webserver
exec airflow webserver
