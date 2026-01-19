import os
import glob
import json
from datetime import datetime, timezone

try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas is required. Install with: python -m pip install pandas")

RAW_DIR = "data/raw-data"
PROCESSED_DIR = "data/processed-data"


def get_latest_raw_file(raw_dir: str) -> str:
    pattern = os.path.join(raw_dir, "products_raw_*.json")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            f"No raw files found in {raw_dir}. Run: python scripts/fetch_data.py"
        )

    # Pick the most recently modified file
    latest = max(files, key=os.path.getmtime)
    return latest


def flatten_products(products: list) -> "pd.DataFrame":
    """
    FakeStore products look like:
    {
      "id": 1,
      "title": "...",
      "price": 109.95,
      "description": "...",
      "category": "...",
      "image": "...",
      "rating": {"rate": 3.9, "count": 120}
    }
    """
    rows = []
    for p in products:
        rating = p.get("rating") or {}
        rows.append(
            {
                "product_id": p.get("id"),
                "title": p.get("title"),
                "price_usd": p.get("price"),
                "description": p.get("description"),
                "category": p.get("category"),
                "image_url": p.get("image"),
                "rating_rate": rating.get("rate"),
                "rating_count": rating.get("count"),
            }
        )
    return pd.DataFrame(rows)


def transform(latest_raw_file: str) -> None:
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Read raw JSON
    with open(latest_raw_file, "r", encoding="utf-8") as f:
        products = json.load(f)

    df = flatten_products(products)

    # Basic data quality cleanup
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")

    # Metadata columns
    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["processed_at_utc"] = processed_at
    df["source_file"] = os.path.basename(latest_raw_file)

    # Save Parquet if available, else JSON
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    parquet_path = os.path.join(PROCESSED_DIR, f"products_cleaned_{ts}.parquet")
    json_path = os.path.join(PROCESSED_DIR, f"products_cleaned_{ts}.json")

    try:
        df.to_parquet(parquet_path, index=False)
        print(f"✅ Saved cleaned data (Parquet): {parquet_path}")
    except Exception as e:
        # If pyarrow/fastparquet isn't installed, parquet will fail
        df.to_json(json_path, orient="records", indent=2)
        print("⚠️ Parquet save failed (likely missing pyarrow/fastparquet).")
        print(f"✅ Saved cleaned data (JSON): {json_path}")
        print(f"   Reason: {e}")


if __name__ == "__main__":
    latest = get_latest_raw_file(RAW_DIR)
    print(f"📥 Using latest raw file: {latest}")
    transform(latest)
