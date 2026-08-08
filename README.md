# Late Transaction Revenue Correction

> **A Databricks + PySpark + Delta Lake project for detecting
> late-arriving transactions and correcting only the affected historical
> revenue.**

## 1. Project at a Glance

In a real transaction system, a transaction does not always reach the
data platform on the same day it happens.

For example:

``` text
Transaction Date : 2024-02-10
Ingestion Date   : 2024-02-18
```

The transaction belongs to **February 10**, but the system receives it
on **February 18**.

If the February 10 revenue report has already been generated, the report
can become incomplete.

The purpose of this project is to handle that situation without
unnecessarily rebuilding the complete historical report.

### My approach

``` text
Incoming Files
      ↓
   Bronze
      ↓
   Silver
      ↓
Initial Gold Revenue
      ↓
Detect Late Transactions
      ↓
Find Affected Dates
      ↓
Recalculate Revenue
      ↓
Delta MERGE
      ↓
Corrected Gold
```

------------------------------------------------------------------------

## 2. What I Built

I built a small end-to-end data pipeline in **Databricks** using:

-   **PySpark** for data processing
-   **Auto Loader** for file ingestion
-   **Delta Lake** for reliable storage and `MERGE`
-   **Bronze / Silver / Gold** layers for organizing the data

The main part of the project is the **historical correction logic**.

Instead of doing a full refresh whenever late transactions appear, I
first find **which dates are actually affected**, recalculate those
dates from the trusted Silver data, and then update the Gold table.

------------------------------------------------------------------------

## 3. Architecture

``` text
                   Transaction CSV
                         │
                         ▼
                  ┌─────────────┐
                  │ Auto Loader │
                  └──────┬──────┘
                         │
                         ▼
                ┌─────────────────┐
                │     BRONZE      │
                │ Raw transaction │
                │ + arrival info  │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │     SILVER      │
                │ Clean / Validate│
                │  / Deduplicate  │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │      GOLD       │
                │  Daily Revenue  │
                └────────┬────────┘
                         │
                         ▼
               Compare txn_date
              with ingestion_date
                         │
                         ▼
                 Late Transactions
                         │
                         ▼
                  Affected Dates
                         │
                         ▼
              Recalculate from Silver
                         │
                         ▼
                    Delta MERGE
                         │
                         ▼
                 Corrected Revenue
```

------------------------------------------------------------------------

## 4. Layer-wise Work

### Bronze --- Capture what arrived

The Bronze layer keeps the incoming data close to the source.

Auto Loader reads the CSV files and writes the data in Delta format.
Ingestion information is retained because it is needed later to
understand when a transaction actually reached the platform.

**Result: 2,000 Bronze records**

------------------------------------------------------------------------

### Silver --- Make the data trustworthy

In Silver, I prepare the data for reporting.

The pipeline handles:

-   Date type conversion
-   Amount type conversion
-   Required-field validation
-   Duplicate transaction IDs
-   Invalid transaction records
-   Quarantine handling

For duplicate transaction IDs, the latest ingested version is retained.

**Result:**

-   Silver records: **2,000**
-   Quarantined records: **0**

------------------------------------------------------------------------

### Gold --- Generate daily revenue

The Gold layer contains the reporting-level data.

Daily revenue is calculated from the trusted Silver data:

``` text
daily_revenue = SUM(amount)
```

This creates the initial historical revenue report.

------------------------------------------------------------------------

## 5. The Main Logic --- Late Transactions

A transaction is considered late when:

``` text
ingestion_date > txn_date
```

I also calculate the delay in days so that the impact can be measured.

### Current run

  Metric                        Result
  ------------------------ -----------
  Total transactions             2,000
  Late transactions              1,415
  Affected dates                    60
  Average delay              6.32 days
  Maximum delay                15 days
  Late transaction value     3,708,520

These results show that the late-arrival problem is actually present in
the dataset and has a measurable historical impact.

------------------------------------------------------------------------

## 6. How the Historical Correction Works

Once late transactions are identified, I extract their distinct
`txn_date` values.

For the current run, **60 historical dates** were identified.

I then recalculate the complete revenue for those dates using the Silver
data.

I intentionally do **not** just do:

``` text
Old Gold Revenue + Late Transaction Amount
```

Instead, I calculate the complete daily revenue again:

``` text
SUM(all valid Silver transactions for the affected date)
```

This gives the corrected total for that business date.

------------------------------------------------------------------------

## 7. Delta MERGE

The corrected daily revenue is then applied to the Gold Delta table
using `MERGE`.

The logic is:

``` text
Existing affected date → UPDATE
New date              → INSERT
Unaffected date       → No change
```

This is the key part of the project because only the affected historical
records need to be changed.

### Why I used MERGE

A full overwrite would process the complete Gold history even when only
a small part needs correction.

With this approach:

``` text
Detect → Identify → Recalculate → MERGE
```

the correction stays focused on the affected dates.

------------------------------------------------------------------------

## 8. Data Quality Checks

I added checks to make sure the transaction data remains valid.

The project checks for:

-   Null transaction IDs
-   Duplicate transaction IDs
-   Negative transaction amounts
-   Invalid records that need quarantine

The initial source-quality check showed:

``` text
Total records       : 2,000
Unique transaction IDs : 2,000
Null transaction IDs : 0
Non-positive amounts : 0
```

------------------------------------------------------------------------

## 9. Processing Control

The project also maintains a `watermark_control` table containing:

``` text
table_name
last_processed_date
last_run_timestamp
```

This provides a simple way to track processing progress and can be
extended later for more advanced incremental processing.

------------------------------------------------------------------------

## 10. Project Results

The complete run demonstrated:

-   **2,000** source transactions
-   **2,000** Bronze records
-   **2,000** Silver records
-   **0** quarantined records
-   **1,415** late transactions detected
-   **60** historical dates affected
-   **6.32 days** average delay
-   **15 days** maximum delay
-   **3,708,520** value associated with late transactions

The important result is not just detecting late transactions. The
pipeline also identifies their historical impact and applies a targeted
correction to Gold.

------------------------------------------------------------------------

## 11. Project Structure

``` text
Late-Transaction-Revenue-Correction/
│
├── README.md
│
├── notebook/
│   └── Late_Transaction_Revenue_Correction.py
│
├── data/
│   └── sales_2000_rows.csv
│
├── screenshots/
│   ├── 01_source_data_quality.png
│   ├── 02_bronze_layer.png
│   ├── 03_silver_layer.png
│   ├── 04_gold_initial_report.png
│   ├── 05_late_transaction_detection.png
│   ├── 06_late_arrival_impact.png
│   ├── 07_affected_dates.png
│   ├── 08_delta_merge_correction.png
│   ├── 09_final_quality_checks.png
│   └── 10_watermark_control.png
│
└── docs/
    └── PROJECT_DOCUMENTATION.md
```

------------------------------------------------------------------------

## 12. How to Run

1.  Open the notebook in Databricks.
2.  Provide the source CSV in the configured input location.
3.  Run the notebook from top to bottom.
4.  Verify the Bronze ingestion.
5.  Verify Silver validation and deduplication.
6.  Generate the initial Gold revenue.
7.  Detect late transactions.
8.  Identify affected dates.
9.  Recalculate revenue for those dates.
10. Run the Delta `MERGE`.
11. Run the final data-quality checks.
12. Review the processing-control table.

------------------------------------------------------------------------

## 13. What This Project Demonstrates

### Technical

-   Databricks
-   PySpark
-   Auto Loader
-   Delta Lake
-   Delta `MERGE`
-   Medallion Architecture
-   Data validation
-   Deduplication
-   Quarantine handling
-   Incremental processing concepts

### Problem-solving

The main learning from this project was that **finding the late data is
only one part of the problem**.

The more important part is deciding how to correct the historical report
without unnecessarily processing everything again.

That is why the project uses:

``` text
Late Detection
      ↓
Affected Date Identification
      ↓
Targeted Recalculation
      ↓
Delta MERGE
```

------------------------------------------------------------------------

## 14. Short Interview Explanation

> **I built a Databricks pipeline to handle late-arriving transactions
> and their impact on historical revenue. I used Auto Loader for
> ingestion, Bronze for raw data, Silver for cleaning and deduplication,
> and Gold for daily revenue reporting. Then I compared the transaction
> date with the ingestion date to identify late transactions. The main
> part of my project was the correction logic. Instead of rebuilding the
> complete historical Gold table, I identified only the affected dates,
> recalculated their complete revenue from Silver, and used Delta MERGE
> to update those dates. I also added data-quality checks and a
> watermark control table to make the pipeline easier to monitor.**

------------------------------------------------------------------------

## 15. Future Improvements

If I extend this project further, I would add:

-   Automated alerts for unusual late-arrival volumes
-   A correction-history/audit table
-   Pipeline monitoring and failure alerts
-   A dashboard showing late-arrival trends
-   Larger-volume testing
-   More automated data-quality tests

------------------------------------------------------------------------

## Final Takeaway

This project is built around one simple idea:

> **When late data changes historical reporting, identify what actually
> changed and correct only that part.**

That makes the pipeline easier to understand, avoids unnecessary full
historical processing, and gives a clear path from raw transactions to
corrected revenue.
