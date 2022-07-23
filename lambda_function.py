import requests
import csv
import base64
import boto3
import json
from datetime import timedelta, date, datetime
from botocore.exceptions import ClientError

def getYesterdayDate():
    yesterday_date = (date.today() - timedelta(1)).isoformat()
    return yesterday_date


def getTodayDate():
    today = date.today().isoformat()
    return today


def get_secret():
    secret_name = ""
    region_name = ""
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e)

        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.

            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.

            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.

            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.

            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.

            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return json.loads(secret)

def lambda_handler(event, context):
    secrets = get_secret()
    # with open('sample.csv', 'r') as file:
    #     reader = csv.reader(file)
    #     data = {
    #                 "rows": []
    #             }
    #     device = { "Tablet" : "mobile",
    #     "Phone" : "mobile",
    #     "PC": "desktop",
    #     "MediaCenter": "desktop"
    #     }
    #     for row in reader:
    #         date = (row[0].split("-"))
    #         if len(date) == 3 and row[5] in ["Phone", "MediaCenter", "Tablet", "PC"]:
    #             data["month"] = int(date[1])
    #             data["year"] = int(date[0])
    #             element = {
    #              "device": device['Tablet'],
    #              "creative": {"format": "banner"},
    #              "domain": row[1],
    #              "impressions": int(row[6].replace(',', '')),
    #              "country": row[3]
    #             }
    #             data['rows'].append(element)
    # return data

    # try:
    #     url = "https://api.scope3.com/v0/calculate"
    #     payload = data
    #     headers = {
    #     "Accept": "application/json",
    #     "Content-Type": "application/json",
    #     "AccessClientId": secrets['ScopeAccessClientId'],
    #     "AccessClientSecret":secrets['ScopeAccessClientSecret']
    #     }
    #     response = requests.post(url, json=payload, headers=headers)

    #     print(response.text)
    #     return response.json()
    # except requests.exceptions.HTTPError as error:
    #     print(error)


    # try:
    #     url = "https://api.appnexus.com/auth"
    #     payload = {
    #         "auth": {
    #         "username" : "oliver@13772",
    #         "password" : "Xandr2022$"
    #         }
    #     }
    #     get_info = requests.post(url, json=payload)
    #     raw = get_info.json()
    #     token = raw["response"]["token"]
    #     print(token)
    yesterday = getYesterdayDate()
    today = getTodayDate()
    total = 0
    try:
        slicer = "https://uslicer.iponweb.com/API/v2/query"
        payload = {
            "project_name": "",
            "slicer_name": "",
            "token":"",
            "split_by": ["bid_general_details.tech.agent.device.type", "bid_general_details.tech.agent.device.maker", "bid_general_details.tech.agent.device.model", "bid_general_details.tech.agent.os.name", "bid_general_details.tech.agent.browser.name",  "bid_general_details.geo.country", "bid_context.adslot.content_type", "bid_context.publisher.domain", "bid_context.request.inventory_type", "bid_context.adslot.size", "bid_context.adslot.video_placement"],
            "data_fields": ["impressions"],
             "start_date": yesterday,
             "end_date": today,
             "limit": -1
        }
        get_info = requests.post(slicer, headers={"Content-Type":"application/json", "Accept": "application/json"}, json=payload)
        raw = get_info.json()
        total = (raw["total"]['records_found'])
        offset = 10000
        data = raw["rows"]

        length = len(raw["rows"])
        return print(length)
        # while total > 0 :
        #     payload['offset']=offset
        #     get_info = requests.post(slicer, headers={"Content-Type":"application/json", "Accept": "application/json"}, json=payload)
        #     raw = get_info.json()
        #     data += raw["rows"]
        #     total -=  10000
        #     offset += 10000
        # length = len(data)
        # return print(length)
    except requests.exceptions.HTTPError as error:
        print(error)

    except requests.exceptions.HTTPError as error:
        print(error)
