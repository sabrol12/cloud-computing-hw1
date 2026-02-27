import boto3
import random
from boto3.dynamodb.conditions import Attr
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = "us-east-1"
TABLE_NAME = "yelpdb-hw1"
OPENSEARCH_ENDPOINT = "https://search-hw1-4grt6d7skljecgsv6qpcqrha5q.us-east-1.es.amazonaws.com"
SAMPLES_PER_CUISINE = 30

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

credentials = boto3.Session().get_credentials()

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    "es",
    session_token=credentials.token
)

es = OpenSearch(
    hosts=[{
        "host": OPENSEARCH_ENDPOINT.replace("https://", ""),
        "port": 443
    }],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

cuisines = [
    "Chinese restaurants",
    "Japanese restaurants",
    "Indian restaurants",
    "Mexican restaurants",
    "Italian restaurants",
    "Thai restaurants",
    "Greek restaurants"
]

grand_total = 0

for cuisine_term in cuisines:

    print(f"\nProcessing {cuisine_term}...")

    # -------- Handle Pagination --------
    items = []
    response = table.scan(
        FilterExpression=Attr("cuisineSearchTerm").eq(cuisine_term)
    )

    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression=Attr("cuisineSearchTerm").eq(cuisine_term),
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        items.extend(response.get("Items", []))

    print(f"Total found in DynamoDB: {len(items)}")

    if len(items) < SAMPLES_PER_CUISINE:
        print("⚠ Not enough items available.")
        selected = items
    else:
        selected = random.sample(items, SAMPLES_PER_CUISINE)

    cuisine_name = cuisine_term.split()[0]

    uploaded_count = 0

    for item in selected:
        doc = {
            "RestaurantID": item["businessId"],
            "Cuisine": cuisine_name
        }

        es.index(index="restaurants", body=doc)
        uploaded_count += 1

    print(f"Uploaded {uploaded_count} to OpenSearch.")
    grand_total += uploaded_count

print(f"\nDONE. Total uploaded across cuisines: {grand_total}")