import json
import requests
import logging
from airflow.operators.python import PythonOperator
# from airflow.providers.standard.operators.python import PythonOperator
from kafka import KafkaProducer
from datetime import datetime, timedelta, time

from airflow import DAG
# from airflow.operators.python import PythonOperator

# ✅ 1. Définir default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ✅ 2. Fonction pour obtenir les données
def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]

# ✅ 3. Formatage des données
def format_data(res):
    data = {
        'gender': res['gender'],
        'title': res['name']['title'],
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'address': f"{res['location']['street']['number']} {res['location']['street']['name']}",
        'city': res['location']['city'],
        'state': res['location']['state'],
        'country': res['location']['country'],
        'postcode': res['location']['postcode'],
        'latitude': res['location']['coordinates']['latitude'],
        'longitude': res['location']['coordinates']['longitude'],
        'timezone_offset': res['location']['timezone']['offset'],
        'timezone_description': res['location']['timezone']['description'],
        'email': res['email'],
        'phone': res['phone'],
        'cell': res['cell'],
        'uuid': res['login']['uuid'],
        'username': res['login']['username'],
        'birthdate': res['dob']['date'],
        'age': res['dob']['age'],
        'registered_date': res['registered']['date'],
        'registered_age': res['registered']['age'],
        'nationality': res['nat'],
        'id_name': res['id']['name'],
        'id_value': res['id']['value'],
        'picture_thumbnail': res['picture']['thumbnail'],
        'picture_medium': res['picture']['medium'],
        'picture_large': res['picture']['large']
    }
    return data

# ✅ 4. Fonction principale appelée par Airflow
# def stream_data():
#     try:
#         res = get_data()
#         res = format_data(res)
#         producer = KafkaProducer(
#             bootstrap_servers=['broker:29092'],
#             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#             max_block_ms=5000
#         )
#         producer.send('users_created', value=res)
#         producer.flush()
#         logging.info("✅ Message envoyé dans 'users_created'")
#     except Exception as e:
#         logging.error(f"❌ Erreur : {e}")
#         raise

def stream_data():
    try:
        # Temps de départ
        curr_time = time.time()

        # Initialisation du producteur Kafka
        producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=5000
        )

        # Boucle pendant 60 secondes
        while time.time() - curr_time < 60:
            try:
                raw_data = get_data()
                formatted_data = format_data(raw_data)
                producer.send('users_created', value=formatted_data)
                logging.info("✅ Message envoyé dans 'users_created'")
            except Exception as e:
                logging.error(f"❌ Erreur lors de l'envoi du message : {e}")
                continue

        # Nettoyage
        producer.flush()
        producer.close()

    except Exception as e:
        logging.error(f"❌ Erreur générale dans stream_data : {e}")
        raise

# ✅ 5. Définir le DAG avec les arguments corrigés
with DAG(
    dag_id='user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='Stream data daily from API'
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
