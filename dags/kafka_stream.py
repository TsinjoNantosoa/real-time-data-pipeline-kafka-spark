import json
from datetime import datetime
from airflow import DAG
# from airflow.operators.python import PythonOperator  # corrected operator path

default_args = {
    'owner': 'tsinjo',
    'start_date': datetime(2023, 8, 3, 10, 0)
}


def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    data = res.json()
    return data['results'][0]  # We only need the user info


def format_data(user):
    formatted = {
        "gender": user.get("gender"),
        "first_name": user.get("name", {}).get("first"),
        "last_name": user.get("name", {}).get("last"),
        "email": user.get("email"),
        "country": user.get("location", {}).get("country"),
        "city": user.get("location", {}).get("city"),
        "phone": user.get("phone"),
        "picture": user.get("picture", {}).get("large")
    }
    return formatted


def stream_data():
    user = get_data()
    result = format_data(user)
    print(json.dumps(result, indent=3))


# Décommente ceci pour exécuter dans Airflow
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

# Test local
stream_data()
