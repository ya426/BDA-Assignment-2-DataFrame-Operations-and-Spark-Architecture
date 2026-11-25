# BDA Assignment 2 — Sales Data Processing and Business Intelligence Using Spark DataFrames

## Setup (Windows / VS Code)
Install Java 17 and Python 3.10 or 3.11. Ensure java and python are on PATH. Open this project folder in VS Code.

```bash
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe sales_analysis.py --ui
```

On Linux/macOS use `.venv/bin/python` instead. The script uses Spark local[4] (four worker threads, not four machines). No separate Spark installation is required. Open the Spark UI address printed by the script (usually http://localhost:4040) before pressing Enter. Capture Jobs > select a job > DAG Visualization; Stages; SQL/DataFrame > select a query > execution DAG; Executors. The program prints the extended execution plan and all analytics. To save output on Windows PowerShell, run `.venv\Scripts\python.exe sales_analysis.py --ui | Tee-Object -FilePath actual_output.txt`.

## Dataset and validation
The CSV is synthetic, with 25 transactions and seven required fields. Five rows deliberately contain invalid values: T021 missing quantity, T022 negative quantity, T023 nonnumeric price, T024 missing location, T025 malformed date. Input columns are read as strings and safely converted to integer, decimal and date. Missing/blank fields and nonpositive quantities/prices are rejected. Revenue is quantity multiplied by unit price; this is not profit (cost data is unavailable). Best-selling means greatest total units, with product name as tie breaker. The dataset has no external source URL.

## RDD vs DataFrame
RDDs represent collections of objects and expose low-level transformations; DataFrames are distributed rows with named, typed columns. Both evaluate transformations lazily. RDD category aggregation uses map and reduceByKey; DataFrame uses groupBy and sum. Both results are checked for equality by the program. Catalyst can optimize DataFrame logical plans (projection pruning, filter placement, etc.), then choose a physical plan. The RDD's arbitrary Python functions are not optimized by Catalyst. The RDD example starts from cleaned DataFrame rows, so it is a comparison of aggregation approaches, not an independent end-to-end benchmark. No runtime performance claims are made.

## Spark architecture and DAG
Driver builds a logical plan; Catalyst analyzes and optimizes it, and Spark selects a physical plan. An action triggers a job; the DAG scheduler divides work into stages at shuffle boundaries. Tasks operate on partitions using executor resources. Local mode runs on one machine; in cluster mode tasks run on distributed executors. For grouped revenue, expect partial aggregation, a shuffle by category and final aggregation, with a possible additional exchange for sorting. Adaptive Query Execution can change the physical plan at runtime. Inspect actual execution in Spark UI rather than assuming the exact stage count.

## Submission
Upload this project to a public GitHub repository (exclude .venv, __pycache__, Spark logs). Upload ZIP and data/sales.csv separately to Google Drive; set viewer access as required. Use the GitHub CSV URL as dataset link and disclose it is self-generated. expected_output.txt contains independently calculated results, not an actual Spark run. Capture your actual UI and output after running locally.
