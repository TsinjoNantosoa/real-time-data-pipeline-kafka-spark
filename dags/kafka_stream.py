import json
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# from airflow.operators.python import PythonOperator  # corrected operator path

default_args = {
    'owner': 'tsinjo',
    'start_date': datetime(2023, 8, 3, 10, 0)
}

def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}

    # Informations générales
    data['gender'] = res['gender']
    data['title'] = res['name']['title']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']

    # Localisation
    location = res['location']
    data['address'] = f"{location['street']['number']} {location['street']['name']}"
    data['city'] = location['city']
    data['state'] = location['state']
    data['country'] = location['country']
    data['postcode'] = location['postcode']
    data['latitude'] = location['coordinates']['latitude']
    data['longitude'] = location['coordinates']['longitude']
    data['timezone_offset'] = location['timezone']['offset']
    data['timezone_description'] = location['timezone']['description']

    # Contact
    data['email'] = res['email']
    data['phone'] = res['phone']
    data['cell'] = res['cell']

    # Identifiants
    data['uuid'] = res['login']['uuid']
    data['username'] = res['login']['username']

    # Date de naissance et inscription
    data['birthdate'] = res['dob']['date']
    data['age'] = res['dob']['age']
    data['registered_date'] = res['registered']['date']
    data['registered_age'] = res['registered']['age']

    # Nationalité et ID
    data['nationality'] = res['nat']
    data['id_name'] = res['id']['name']
    data['id_value'] = res['id']['value']

    # Images
    data['picture_thumbnail'] = res['picture']['thumbnail']
    data['picture_medium'] = res['picture']['medium']
    data['picture_large'] = res['picture']['large']

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    res = get_data()
    res = format_data(res)
    # print(json.dumps(res, indent=3))
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('users_created' , json.dumps(res).encode('utf-8'))


# with DAG(
#     'user_automation',
#     default_args=default_args,
#     schedule='@daily',
#     catchup=False
# ) as dag:
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data()
