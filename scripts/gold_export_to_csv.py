import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

gold_parquet_path = r"D:\data-engineering-project\data\gold\endpoint_metrics_v1\part-00000.parquet"
gold_csv_path = r"D:\data-engineering-project\data\gold\endpoint_metrics_v1\endpoint_metrics.csv"

print("Reading Gold parquet...")
table = pq.read_table(gold_parquet_path)
df = table.to_pandas()

print("Writing Gold CSV for Power BI...")
df.to_csv(gold_csv_path, index=False)

print("✅ Gold CSV ready for Power BI:")
print(gold_csv_path)
print(df.head())
