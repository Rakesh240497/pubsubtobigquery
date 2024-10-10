import os
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.cloud import bigquery

# Get the path to the service account file from the environment variable
key_file_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
# print(key_file_path)

# Create a Pub/Sub client with your service account
credentials = service_account.Credentials.from_service_account_file(key_file_path)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

# Set your project and subscription
project_id = 'tempproject-437919'
subscription_id = 'getpost-sub'  # Replace with your subscription ID

# Define the subscription path
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Pull messages
# message_data_list = []
def pull_messages():
    count = 0 
    response = subscriber.pull(subscription=subscription_path, max_messages=20)

    for received_message in response.received_messages:
        # Print the message data (decode if base64 encoded)
        message_data = received_message.message.data.decode('utf-8')
        count+=1
        print(f"Received message {count}: {message_data}")
        try:
            bigquery_client = bigquery.Client()
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

def send_sub_to_bquery():
    # Create BigQuery client
    client = bigquery.Client()

    # Define the schema
    # schema = [
    #     {"name": "message", "type": "STRING"},
    # ]

    # Create the BigQuery table
    # create_table = BigQueryCreateEmptyTableOperator(
    #     task_id='create_table',
    #     dataset_id='practice',
    #     table_id='pubsubdata',
    #     project_id='tempproject-437919',
    #     schema_fields=schema
    # )
    # create_table.execute(context=None)  # Execute the create table operation

    # Insert data into BigQuery
    # errors = client.insert_rows_json(bq_table, json_rows=message_data)

    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))

if __name__ == "__main__":
    pull_messages()
