import pandas as pd
import random
import os
import boto3
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

def generate_transactions(n=5000):
    rows = []
    for i in range(n):
        # Inject intentional anomalies in ~10% of rows
        anomaly = random.random() < 0.10

        rows.append({
            "tx_id":           None if (anomaly and random.random() < 0.2) else fake.uuid4(),
            "amount":          round(random.uniform(-500, 50000) if anomaly else random.uniform(1, 4999), 2),
            "user_id":         fake.uuid4(),
            "timestamp":       fake.date_time_this_year().isoformat(),
            "status":          random.choice(["INVALID", "unknown"]) if anomaly else random.choice(["success", "failed", "pending"]),
            "merchant":        fake.company(),
            "is_international": random.choice([0, 1]),
        })
    return pd.DataFrame(rows)


def upload_to_s3(filepath: str, s3_key: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    s3.upload_file(filepath, os.getenv("S3_BUCKET"), s3_key)
    print(f"Uploaded to s3://{os.getenv('S3_BUCKET')}/{s3_key}")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    df = generate_transactions(5000)
    local_path = "data/transactions.csv"
    df.to_csv(local_path, index=False)

    total    = len(df)
    nulls    = df["tx_id"].isna().sum()
    bad_amt  = (df["amount"] < 0).sum()
    bad_stat = (~df["status"].isin(["success","failed","pending"])).sum()

    print(f"Generated {total} rows")
    print(f"  Null tx_ids   : {nulls}")
    print(f"  Negative amounts : {bad_amt}")
    print(f"  Invalid statuses : {bad_stat}")

    upload_to_s3(local_path, "raw/transactions/transactions.csv")