import json
import boto3
import os
import random
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

QUEUE_URL = os.environ['QUEUE_URL']
DYNAMODB_TABLE = os.environ['DYNAMODB_TABLE']
OPENSEARCH_ENDPOINT = os.environ['OPENSEARCH_ENDPOINT']
SES_SOURCE_EMAIL = os.environ['SES_SOURCE_EMAIL']
REGION = os.environ.get('AWS_REGION', 'us-east-2')

sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
table = dynamodb.Table(DYNAMODB_TABLE)

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    'es',
    session_token=credentials.token
)

opensearch_host = OPENSEARCH_ENDPOINT.replace("https://", "").replace("http://", "").rstrip("/")

es = OpenSearch(
    hosts=[{
        'host': opensearch_host,
        'port': 443
    }],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def lambda_handler(event, context):
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0
    )

    if 'Messages' not in response:
        print("No messages found.")
        return {"status": "No messages in queue"}

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    body = json.loads(message['Body'])

    print(f"SQS message body: {json.dumps(body)}")

    cuisine = body.get("Cuisine", "").capitalize()
    location = body.get("Location", "")
    email = body.get("Email", "")
    dining_time = body.get("DiningTime", "")
    number_of_people = body.get("NumberOfPeople", "")

    print(f"Request: {cuisine} in {location} for {number_of_people} at {dining_time}")

    if not cuisine or not email:
        print("Missing cuisine or email. Deleting bad message.")
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        return {"status": "Missing required fields"}

    
    location = "manhattan" if location.lower().strip() in [
        "manhattan", "new york", "new york, ny", "new york city", "nyc", "ny, ny"
    ] else location 

    search_query = {
        "size": 50,
        "query": {
            "term": {
                "Cuisine.keyword": cuisine
            }
        }
    }

    print(f"OpenSearch query: {json.dumps(search_query)}")

    try:
        es_response = es.search(
            index="restaurants",
            body=search_query
        )
    except Exception as e:
        print(f"OpenSearch error: {str(e)}")
        search_query_fallback = {
            "size": 50,
            "query": {
                "match": {
                    "Cuisine": cuisine
                }
            }
        }
        es_response = es.search(
            index="restaurants",
            body=search_query_fallback
        )

    hits = es_response.get("hits", {}).get("hits", [])
    print(f"OpenSearch returned {len(hits)} results")

    if not hits:
        print(f"No restaurants found for {cuisine}")
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        return {"status": f"No restaurants found for {cuisine}"}

    selected_hits = random.sample(hits, min(5, len(hits)))

    restaurant_details = []

    for hit in selected_hits:
        restaurant_id = hit['_source']['RestaurantID']
        print(f"Fetching DynamoDB: {restaurant_id}")

        try:
            db_response = table.get_item(
                Key={"businessId": restaurant_id}
            )

            if 'Item' in db_response:
                restaurant_details.append(db_response['Item'])
            else:
                print(f"  Not found in DynamoDB: {restaurant_id}")
        except Exception as e:
            print(f"  DynamoDB error for {restaurant_id}: {str(e)}")

    if not restaurant_details:
        print("No restaurant details found in DynamoDB")
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        return {"status": "No restaurant details found in DynamoDB"}

    message_text = (
        f"Hello!\n\n"
        f"Here are your {cuisine} restaurant suggestions "
        f"in {location} for {number_of_people} people "
        f"at {dining_time}:\n\n"
    )

    for idx, r in enumerate(restaurant_details, start=1):
        name = r.get('name', 'Unknown')
        address = r.get('address', 'Address not available')
        rating = r.get('rating', 'N/A')
        review_count = r.get('reviewCount', 'N/A')
        message_text += (
            f"{idx}. {name}\n"
            f"   Address: {address}\n"
            f"   Rating: {rating} ({review_count} reviews)\n\n"
        )

    message_text += "Enjoy your meal!\n- Dining Concierge Bot"

    print(f"Email body:\n{message_text}")

    try:
        ses.send_email(
            Source=SES_SOURCE_EMAIL,
            Destination={
                "ToAddresses": [email]
            },
            Message={
                "Subject": {
                    "Data": f"Your {cuisine} Dining Suggestions in {location}"
                },
                "Body": {
                    "Text": {
                        "Data": message_text
                    }
                }
            }
        )
        print(f"Email sent to {email}")
    except Exception as e:
        print(f"SES error: {str(e)}")
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
        raise e

    sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
    print("Message deleted from queue.")
    return {
        "status": "Email sent successfully",
        "restaurants_sent": len(restaurant_details)
    }