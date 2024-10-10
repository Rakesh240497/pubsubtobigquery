import os
import json
import requests
import base64
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import faker

# Get the path to the service account file from the environment variable
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/rakeshnanankal/airflow/tempproject-437919-cdff304c5759.json'
key_file_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
print("Key file path:", key_file_path)

rand_text = faker.Faker('en_US')
text_list = []
for i in range(10):
    text_list.append(rand_text.text())
# print(text_list)
credentials = service_account.Credentials.from_service_account_file(key_file_path, scopes=['https://www.googleapis.com/auth/cloud-platform'])
    
    # Get the access token
credentials.refresh(Request())
access_token = credentials.token
    # print("Access Token:", access_token)


    # Set your project and topic
project_id = 'tempproject-437919'
topic_id = 'getpost'
url = f'https://pubsub.googleapis.com/v1/projects/{project_id}/topics/{topic_id}:publish'
count = 0

print('Someting')
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
        # print(json.dumps(message))  
        # Check the response
        if response.status_code == 200:
            print("Message published successfully:", response.json())
        else:
            print("Error publishing message:", response.status_code, response.text)

except Exception as e:
    print("An error occurred:", str(e))
print(count)
