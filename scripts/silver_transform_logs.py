import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

bronze_path = r"D:\data-engineering-project\data\staging\logs_parquet_v1"
silver_path = r"D:\data-engineering-project\data\silver\logs_clean_v1"

Path(silver_path).mkdir(parents=True, exist_ok=True)

print("Reading Bronze parquet...")
df = pd.read_parquet(bronze_path)

print("Parsing log lines...")

parts = df["value"].str.split(" \\| ", expand=True)

df_parsed = pd.DataFrame({
    "log_time": pd.to_datetime(parts[0], errors="coerce"),
    "level": parts[1],
    "ip": parts[2].str.replace("ip=", "", regex=False),
    "user_id": parts[3].str.replace("user_id=", "", regex=False).astype("int64"),
    "endpoint": parts[4].str.replace("endpoint=", "", regex=False),
    "status": parts[5].str.replace("status=", "", regex=False).astype("int64"),
    "response_time_ms": parts[6].str.replace("response_time_ms=", "", regex=False).astype("int64"),
    "ingest_time": df["ingest_time"]
})

print("Silver sample:")
print(df_parsed.head())

print("Writing Silver parquet...")
pq.write_table(
    pa.Table.from_pandas(df_parsed),
    f"{silver_path}/part-00000.parquet"
)

print(f"✅ SUCCESS: Silver logs written to {silver_path}")
print(f"Row count: {len(df_parsed)}")
