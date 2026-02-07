import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

silver_path = r"D:\data-engineering-project\data\silver\logs_clean_v1"
gold_path = r"D:\data-engineering-project\data\gold\endpoint_metrics_v1"

Path(gold_path).mkdir(parents=True, exist_ok=True)

print("Reading Silver parquet...")
df = pd.read_parquet(silver_path)

print("Computing Gold metrics...")

df["is_error"] = df["status"] >= 400

agg = (
    df.groupby("endpoint")
      .agg(
          total_requests=("endpoint", "count"),
          avg_response_time_ms=("response_time_ms", "mean"),
          p95_response_time_ms=("response_time_ms", lambda x: x.quantile(0.95)),
          error_count=("is_error", "sum")
      )
      .reset_index()
)

agg["error_rate_pct"] = (agg["error_count"] / agg["total_requests"]) * 100
agg["avg_response_time_ms"] = agg["avg_response_time_ms"].round(2)
agg["p95_response_time_ms"] = agg["p95_response_time_ms"].round(2)
agg["error_rate_pct"] = agg["error_rate_pct"].round(2)

print("Gold sample:")
print(agg)

print("Writing Gold parquet...")
pq.write_table(
    pa.Table.from_pandas(agg),
    f"{gold_path}/part-00000.parquet"
)

print(f"✅ SUCCESS: Gold metrics written to {gold_path}")
