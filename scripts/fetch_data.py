import requests
import json
import os
from datetime import datetime

API_URL = "https://fakestoreapi.com/products"
RAW_DIR = "data/raw-data"

def fetch_data():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    os.makedirs(RAW_DIR, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(RAW_DIR, f"products_raw_{timestamp}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Raw data ingested successfully: {output_file}")

if __name__ == "__main__":
    fetch_data()
