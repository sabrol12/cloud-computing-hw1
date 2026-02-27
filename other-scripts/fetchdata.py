import requests
import boto3
import os, time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YELP_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

if not API_KEY:
    raise ValueError("YELP_API_KEY not found in .env file")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

URL = "https://api.yelp.com/v3/businesses/search"


def get_restaurants(term, location="New York, NY", total=200):
    restaurants = []
    offset = 0

    print(f"\nGetting data {term}...")
    
    while len(restaurants) < total:
        params = {
            "term": term,
            "location": location,
            "limit": 50,
            "offset": offset
        }

        response = requests.get(URL, headers=HEADERS, params=params)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            break

        data = response.json()
        businesses = data.get("businesses", [])

        if not businesses:
            break

        restaurants.extend(businesses)
        offset += 50

        print(f"Collected {len(restaurants)} so far...")

        time.sleep(0.5)  # respect the api....

    return restaurants[:total]


cuisines = [
    "Chinese restaurants",
    "Italian restaurants",
    "Mexican restaurants",
    "Indian restaurants",
    "Japanese restaurants",
    "Turkish restaurants",
    "Spanish restaurants"
]


all_restaurants = {}

for cuisine in cuisines:
    results = get_restaurants(cuisine)

    print(f"Total pulled for {cuisine}: {len(results)}")

    for r in results:
        all_restaurants[r["id"]] = r  


print("\n====================================")
print("TOTAL UNIQUE RESTAURANTS:", len(all_restaurants))
print("====================================\n")

clean_restaurants = []

timestamp = datetime.utcnow().isoformat()

for r in all_restaurants.values():
    cleaned = {
        "businessId": r["id"],
        "name": r["name"],
        "address": " ".join(r["location"]["display_address"]),
        "coordinates": r["coordinates"],
        "numReviews": r["review_count"],
        "rating": r["rating"],
        "zipCode": r["location"].get("zip_code", ""),
        "insertedAtTimestamp": timestamp
    }

    clean_restaurants.append(cleaned)


output_file = "yelp_restaurants.json"

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(clean_restaurants, f, indent=4)

print("\n====================================")
print(f"Saved {len(clean_restaurants)} restaurants to {output_file}")
print("====================================")
