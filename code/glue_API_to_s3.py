import sys

import pandas as pd
import numpy as np


import boto3
from botocore.exceptions import ClientError

### Secret call to protect API login credentials
def get_secret():

    secret_name = "flume-secret"
    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

# Gaining access to the API
import requests

#Chunk that allows "access_token" to be generated
url = "https://api.flumewater.com/oauth/token?envelope=true"

secret = get_secret()
payload = {
    "grant_type": "password",
    "client_id": "118363EZ9TS29WG2",
    "client_secret": "FESV1ZYAZZ8H8PGX4HSP",
    "username": "ryancharlesperez@gmail.com",
    "password": secret
}
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}

token_response = requests.post(url, json=payload, headers=headers)

# Data dictionaries and tokens
import json
# Create a dictionary from token response
token_dictionary = json.loads(token_response.text)
# Create separate "data" element list
first_data_element = token_dictionary["data"][0]
# Access the value associated with 'access_token'
access_token = first_data_element.get('access_token')

# Pull user id
import requests

#Chunk that allows "user_id" to be generated
url = "https://api.flumewater.com/me"

headers = {
    "accept": "application/json",
    "authorization": f"Bearer {access_token}"
}

user_response = requests.get(url, headers=headers)

print(user_response.text)

#Convert to a dictionary and retrieve 'user_id'
user_dictionary = json.loads(user_response.text)
user_id = user_dictionary["data"][0]["id"]

# Chunk that allows "device_id" to be generated
import requests
url = f"https://api.flumewater.com/users/{user_id}/devices?limit=50&offset=0&sort_field=id&sort_direction=ASC&user=false&location=false&list_shared=false"

headers = {
    "accept": "application/json",
    "authorization": f"Bearer {access_token}"
}

device_response = requests.get(url, headers=headers)

#Convert to a dictionary and retrieve 'device_id'
device_dictionary = json.loads(device_response.text)
device_id = device_dictionary["data"][0]["id"]

from datetime import datetime, timedelta

# Get the UTC date and set the time to midnight (00:00:00) of the previous day
beginning = (datetime.utcnow() - timedelta(days=1)).replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')

# Get the UTC date and set the time to midnight (00:00:00) of the current day
end = datetime.utcnow().replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')

# Get the UTC date of the previous day
beginning_date = (datetime.utcnow() - timedelta(days=1)).replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d')

# Get the UTC date for today
end_date = datetime.utcnow().replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d')

# Define API url and headers
url = f"https://api.flumetech.com/users/{user_id}/devices/{device_id}/query"

headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {access_token}'
}

### Query Part 1
payload = {
  "queries": [
    {
        "request_id": "total_minutes", # This will return total gallons per minute for the previous day
        "bucket": "MIN",
        "since_datetime": f"{beginning}",
        "until_datetime": f"{end}",
        "types": ["ALL"] 
    },
    {
        "request_id": "appliance_per_minute", # Total gallons per minute broken down by appliance
        "bucket": "MIN",
        "since_datetime": f"{beginning}",
        "until_datetime": f"{end}",
        "types": ["ALL", "OUTDOOR", "CLOTHES_WASHER", "DISH_WASHER", "TOILET", "FAUCET", "SHOWER"]
    }
  ]
}
# API request
query_response = requests.request("POST", url, json=payload, headers=headers)

# Parsing API response
data = json.loads(query_response.text)

# Extracting data
data_list = data.get('data', [])

### DataFrames for Query Part 1

# DataFrame for minute totals
# Extracting info from 'total_minutes' query using list comprehension
extracted_minutes = [
    {
        'datetime': entry['datetime'],
        'value': entry['value']
    }
    for query_result in data_list
    for entry in query_result.get('total_minutes', [])
]

# Create a Pandas DataFrame
df_minutes = pd.DataFrame(extracted_minutes)

#Adding the value of 'gallons' to each row of a new column titled 'units'
df_minutes['units'] = 'gallons'

# DataFrame for appliance by minute
# Extracting info for 'appliance_per_minute' using list comprehension
extracted_appliance = [
    {
        'datetime': entry['datetime'],
        'value': entry['value'],
        'washer': entry['types']['CLOTHES_WASHER'],
        'dishwasher': entry['types']['DISH_WASHER'],
        'toilet': entry['types']['TOILET'],
        'faucet': entry['types']['FAUCET'],
        'shower': entry['types']['SHOWER']
    }
    for query_result in data_list
    for entry in query_result.get('appliance_per_minute', [])
]

# Create a Pandas DataFrame
df_appliance = pd.DataFrame(extracted_appliance)

import boto3
import pyarrow
import io

# Initialize a boto3 client
s3 = boto3.client('s3')

# Save DataFrame for minute totals to S3, with date embedded in file name
with io.BytesIO() as buffer:
    df_minutes.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket='flume-analysis', Key=f'hourly_data_{beginning_date}.parquet', Body=buffer)

# Save DataFrame for appliance by minute to S3, with date embedded in file bame
with io.BytesIO() as buffer:
    df_appliance.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket='flume-analysis', Key=f'appliance_data_{beginning_date}.parquet', Body=buffer)