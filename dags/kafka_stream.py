import json
import requests
from kafka import KafkaProducer


def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]


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


def stream_data():
    res = get_data()
    res = format_data(res)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(res).encode('utf-8'))
    producer.flush()
    print("✅ Message envoyé dans 'users_created'")


if __name__ == "__main__":
    stream_data()
