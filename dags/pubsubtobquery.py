from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import json
import requests
import base64
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import faker
from google.cloud import bigquery
from google.cloud import pubsub_v1

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/rakeshnanankal/Downloads/tempproject-437919-0f08acf748af.json'
# key_file_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# print("Key file path:", key_file_path)

service_account_info = {
    "type": "service_account",
    "project_id": "tempproject-437919",
    "private_key_id": "0f08acf748af4d0a1c52bef8b5c34f5a7840e266",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCQtkm5tRdpkXhy\nXWzuLZnLAhVqItpxX5bF+t+nNyZBOQ9aVDJcusLUDVz9tI7UsO+lv1zubmqtsq9F\nwnqE3YvLuVmbsuOiVyOvvtaXiIXAErfrWcr3pHeOvg2kFFQ8K1QIYwEWUiBYW3GY\nEAUoZxRzvTqlGpN2IjR4h3JRsQhnIjnd7HPjBKy+kYluSHt53WHfV4Vu1TqeyrCU\nEf0hCo2h9m2q9Ak11d6QknbBBuLpJ8GRMNe4N+Rmu0wqsmv/r2GPaWhk9spQc3cx\n0aU0uDeXu+pz0guSdm1R/QVnI0r0584vdxkSQMMDnvfxFdJ24AwenMBbxz4Nq1Nb\nBtKNN7ZVAgMBAAECggEABg36a7Ct+BbECVTrV9PEmX/O0Y//v7qWHlZuwRpHmXb/\ns3HOYZ7Ww/SDLYI+b5uviynEg51PDVFqj+qhIan7TVzF6QPFDmfC8Hl1lQV7QxwF\nYe1x1oalEe3VuEW5eIL/kxJBW2O4QroguJIDBTu62VTTfVDtVoX72Aciu1RC2728\nvgwSlxP4POd5mOXqeP6UvRKM0O/xZkF+eDtePDlooNoNCTa/DnkMrUdYYO47PaQE\nscfRIieS0uGiaVSuH3vqHCiQCYgPrJjkOygS8rkGhSOLFbp/2lwP4hCIZcNcHRS0\nlUBJQmCoQFSkBGVP9BbksOtHC5M7g4c0FRKVrOzeyQKBgQDEdpy0UHzBXpb+JsD3\n31WSdiOaLHEPodf1gEEAV9mxSE6PyiXvm1KJXswWtbZl7BnmZrUeO2dn1c33WrWj\n0BPYrXx6Pj849455g+ItQfr6e2NkgHJEO8b39a/js1ZQW7L4LMI1JErAQ5va+Y8w\n0JtXUk0qCoNbIytv7FA2qp39OwKBgQC8kOAG+wtwDgiAah5yVwY/enKrQrGgmzUj\nG9xVhLIufCLt0YOSTQMh0H02AVxsMfdq9oan8CT7T1gEbjpzxVuGopAcE6lzaOZo\nS4C7GEX9+3ntF0OfTJwz/raWkIhGje0qOkT+kboDZ5uikLP5F7fXu7H+nfu1eIfE\naQyei4khrwKBgQCHpF0NIYUA/Q3C637PpKKwPGDYUVa+ND+mgKZgVkc233VeUXNf\nux2BOrIt+8ezFw/Kw57JALqkNjm+TiKat078issmm1OLQWBdBXFjTeq6hfZZrVU3\nRw5YpIiL7YXqdWVbc4uipSv3ErbIuQTj7yNRZsFgBWIrylxzKfU3yRTLgQKBgQCk\nQNJUYlOZPTAco9N3p+RyYPgHxGWzz97egUNRXvBS26EVZX5lbx3+U3zuEQtv16p8\nitfK671bB8dUvKu/8rcCNRKw0lO1izfAu5wb3U4Cqg6P/sbRuUsLY6Gqz6VVSAXB\n1dYJpejRVGvWlJOgtA9ZJU8rynPapG4nIZZzeHpwUwKBgB4sO8PxEPTQsn27chaY\nftWtHVrkQ2gWO37ILMCJrcvrnM4vqd4apOUpUZajfOI93YzxE91qhAgwpsnb5HCf\nJjLP2hzZzNMBGt1KR+Jm2hNsJmOn7BE1foFuYcZCMcw2aec+IxkknmdDEhq4uApQ\nntZF6e6zmUeBokTSGdUlk9TZ\n-----END PRIVATE KEY-----\n",
    "client_email": "tempserviceaccount@tempproject-437919.iam.gserviceaccount.com",
    "client_id": "104407418916090670695",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/tempserviceaccount%40tempproject-437919.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}


# credentials = service_account.Credentials.from_service_account_file(key_file_path, scopes=['https://www.googleapis.com/auth/cloud-platform'])
credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=['https://www.googleapis.com/auth/cloud-platform'])
    
   
credentials.refresh(Request())
access_token = credentials.token

#Project and Topic details 
project_id = 'tempproject-437919'
topic_id = 'getpost'

## Subcription Details 
subscription_id = 'getpost-sub'
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)


url = f'https://pubsub.googleapis.com/v1/projects/{project_id}/topics/{topic_id}:publish'


#Create a list of random text 
rand_text = faker.Faker('en_US')
text_list = []
for i in range(100):
    text_list.append(rand_text.text())

count = 0 #Counter to keep track of the number of messages sent to Pub/Sub

def publish_to_pubsub():
    try:
        # Load the service account key
        # Prepare your message (ensure to base64 encode the data)
        for text in text_list:
            data = text  # Your message
            encoded_data = base64.b64encode(data.encode()).decode()  # Base64 encode the message

            # Prepare the message payload
            message = {
                "messages": [
                    {
                        "data": encoded_data,
                        "attributes": {
                            "key": "value"  # Optional attributes
                        }
                    }
                ]
            }

        # Make the request
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }

            response = requests.post(url, headers=headers, data=json.dumps(message))
            count += 1
    except Exception as e:
        print("Error:", e)

def subtobigquery():
    count = 0 
    response = subscriber.pull(subscription=subscription_path, max_messages=100)

    for received_message in response.received_messages:
        # Print the message data (decode if base64 encoded)
        message_data = received_message.message.data.decode('utf-8')
        count+=1
        print(f"Received message {count}: {message_data}")
        try:
            bigquery_client = bigquery.Client(credentials=credentials)
            dataset_id = 'practice'
            table_id = 'pubsubdata'
            table_ref = bigquery_client.dataset(dataset_id).table(table_id)
            table = bigquery_client.get_table(table_ref)
            rows_to_insert = [(message_data,)]
            errors = bigquery_client.insert_rows(table, rows_to_insert)
            if errors == []:
                print("New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))
        except Exception as e:
            print(e)


        # Acknowledge the message
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[received_message.ack_id])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(default_args=default_args, catchup=False, dag_id='pubsubtobquery', schedule_interval='@daily')

publish_to_pubsub = PythonOperator(task_id='publish_to_pubsub', python_callable=publish_to_pubsub, dag=dag)
subtobigquery = PythonOperator(task_id='subtobigquery', python_callable=subtobigquery, dag=dag)

publish_to_pubsub >> subtobigquery

    