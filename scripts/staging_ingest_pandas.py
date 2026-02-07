import pandas as pd
from datetime import datetime
import os

raw_path = r"D:\data-engineering-project\data\raw\app_logs_20260203_202110.log"
staging_path = r"D:\data-engineering-project\data\staging\logs_parquet_v1"

os.makedirs(staging_path, exist_ok=True)

print("Reading raw file with pandas...")
df = pd.read_csv(raw_path, header=None, names=["value"])

df["ingest_time"] = pd.Timestamp.now(tz="UTC")


out_file = os.path.join(staging_path, "part-00000.parquet")

print("Writing parquet with pyarrow (no Spark, no Hadoop)...")
df.to_parquet(out_file, engine="pyarrow", compression="snappy")

print("✅ SUCCESS: Parquet written to:", out_file)
print("Row count:", len(df))
