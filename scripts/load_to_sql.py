import os
import glob
import json
import pyodbc

PROCESSED_DIR = "data/processed-data"

SERVER = "sql-data-eng.database.windows.net"
DATABASE = "sqldb-data-eng"
USERNAME = "oluwasegun"  # exact admin username


def get_latest_processed_json(processed_dir: str) -> str:
    pattern = os.path.join(processed_dir, "products_cleaned_*.json")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No processed JSON found in {processed_dir}. Run: python scripts/transform_data.py"
        )
    return max(files, key=os.path.getmtime)


def get_connection() -> pyodbc.Connection:
    password = os.environ.get("AZURE_SQL_PASSWORD")
    if not password:
        raise ValueError('Missing password. Set: $env:AZURE_SQL_PASSWORD="your_password"')

    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server=tcp:{SERVER},1433;"
        f"Database={DATABASE};"
        f"Uid={USERNAME};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=120;"
    )

    return pyodbc.connect(conn_str)



def ensure_table(cursor):
    cursor.execute("""
    IF OBJECT_ID('dbo.products', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.products (
            id INT NOT NULL PRIMARY KEY,
            title NVARCHAR(255) NULL,
            category NVARCHAR(100) NULL,
            price_usd DECIMAL(10,2) NULL,
            price_gbp DECIMAL(10,2) NULL,
            description NVARCHAR(MAX) NULL,
            image NVARCHAR(500) NULL,
            rating_rate FLOAT NULL,
            rating_count INT NULL,
            ingestion_timestamp DATETIME2 NULL
        );
    END
    """)


def load():
    print("✅ load_to_sql.py started")

    latest_file = get_latest_processed_json(PROCESSED_DIR)
    print(f"📄 Loading file: {latest_file}")

    with open(latest_file, "r", encoding="utf-8") as f:
        rows = json.load(f)

    # Debug prints (optional — remove later)
    print("rows type:", type(rows))
    print("rows count:", len(rows) if isinstance(rows, list) else "not a list")
    print("first row:", rows[0] if isinstance(rows, list) and rows else rows)

    insert_sql = """
    MERGE dbo.products AS target
    USING (SELECT ? AS id) AS src
    ON target.id = src.id
    WHEN NOT MATCHED THEN
        INSERT (id, title, category, price_usd, price_gbp, description, image, rating_rate, rating_count, ingestion_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    conn = get_connection()
    try:
        cur = conn.cursor()
        ensure_table(cur)

        inserted = 0
        for r in rows:
            product_id = r["product_id"]  # your JSON uses product_id

            cur.execute(
                insert_sql,
                product_id,                 # MERGE match id
                product_id,                 # INSERT id
                r.get("title"),
                r.get("category"),
                r.get("price_usd"),
                r.get("price_gbp"),
                r.get("description"),
                r.get("image_url"),         # your JSON uses image_url
                r.get("rating_rate"),
                r.get("rating_count"),
                r.get("processed_at_utc"),  # maps to ingestion_timestamp
            )
            inserted += 1

        conn.commit()
        print(f"✅ Done. Attempted inserts: {inserted}")

    finally:
        conn.close()


if __name__ == "__main__":
    load()
