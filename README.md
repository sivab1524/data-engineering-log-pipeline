\# Data Engineering Log Pipeline (Bronze → Silver → Gold)



\## Overview

This project implements an end-to-end data engineering pipeline using Medallion Architecture:



Bronze (Raw) → Silver (Cleaned) → Gold (Analytics)



The pipeline processes application log files and produces analytics-ready datasets for BI tools like Power BI.



\## Architecture



Raw Logs (.log)

&nbsp;  ↓

Bronze (Parquet)

&nbsp;  ↓

Silver (Parsed \& Cleaned Parquet)

&nbsp;  ↓

Gold (Aggregated Metrics Parquet + CSV)

&nbsp;  ↓

Power BI Dashboard



\## Tech Stack

\- Python

\- Pandas

\- PyArrow

\- Apache Spark (Windows-safe)

\- Parquet

\- Power BI

\- Git \& GitHub



\## Pipeline Steps



\### 1. Bronze Ingestion

Script: staging\_ingest\_pandas.py  

\- Reads raw .log files

\- Writes Bronze Parquet



\### 2. Silver Transformation

Script: silver\_transform\_logs.py  

\- Parses log lines

\- Extracts structured columns

\- Cleans and standardizes data



\### 3. Gold Analytics

Script: gold\_analytics\_logs.py  

\- Computes:

&nbsp; - Total requests

&nbsp; - Avg response time

&nbsp; - P95 latency

&nbsp; - Error counts

&nbsp; - Error rates



\### 4. BI Export

Script: gold\_export\_to\_csv.py  

\- Exports Gold metrics to CSV for Power BI



\## Example Metrics

\- Endpoint performance

\- Error rates per endpoint

\- Latency percentiles



\## Why This Project

This project demonstrates:

\- Medallion Architecture

\- Batch data pipelines

\- Analytics engineering

\- BI integration

\- Production-style folder structure



\## Author

Sivakavi B



