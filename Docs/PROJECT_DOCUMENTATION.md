# Project Documentation

## 1. Project Purpose

This project demonstrates a Databricks pipeline for handling
late-arriving transactions and correcting historical daily revenue.

The implementation uses PySpark, Delta Lake, Auto Loader and a
Bronze/Silver/Gold Medallion Architecture.

The central design goal is to correct historical revenue without
rebuilding the complete Gold table.

------------------------------------------------------------------------

## 2. Layer Responsibilities

### Bronze

Bronze captures the incoming data close to its original form.

Its responsibility is ingestion and traceability, not business
transformation.

### Silver

Silver is the trusted processing layer.

It standardizes types, validates important fields, removes duplicate
transaction IDs and filters invalid amounts.

### Gold

Gold represents the business-facing daily revenue result.

It is initially generated from Silver and can later be corrected when
late transactions are detected.

------------------------------------------------------------------------

## 3. Late-Arrival Rule

The project defines a late transaction as:

``` text
ingestion_date > txn_date
```

The difference between these dates is also calculated as the delay in
days.

This converts the late-arrival problem into a clear and measurable rule.

------------------------------------------------------------------------

## 4. Why Recalculate From Complete Silver Data?

The project deliberately does not calculate the correction by adding
only the late transaction amount.

Instead:

1.  Detect late transactions.
2.  Extract their distinct transaction dates.
3.  Filter the complete Silver dataset to those dates.
4.  Recalculate the complete daily revenue.
5.  Merge those corrected values into Gold.

This matters because the Gold value for an affected date needs to
represent the complete transaction population for that date.

------------------------------------------------------------------------

## 5. Why Use Delta MERGE?

The corrected daily revenue is merged into Gold using `txn_date`.

The intended behavior is:

``` text
Matched affected date → update
New date              → insert
Unaffected date       → leave unchanged
```

This avoids replacing the entire Gold table for a targeted historical
correction.

------------------------------------------------------------------------

## 6. Current Run

The current project run produced:

-   **2,000** source records
-   **2,000** Bronze records
-   **2,000** Silver records
-   **0** quarantined records
-   **1,415** late transactions
-   **60** affected historical dates
-   **6.32 days** average delay
-   **15 days** maximum delay
-   **3,708,520** late transaction value

These numbers demonstrate that the dataset contains a significant
late-arrival scenario and that the pipeline can quantify its historical
impact.

------------------------------------------------------------------------

## 7. Data Quality

The final checks cover:

``` text
Null transaction IDs
Duplicate transaction IDs
Negative transaction amounts
```

The project is designed so that invalid transaction records are handled
before Gold reporting.

------------------------------------------------------------------------

## 8. Watermark Control

The `watermark_control` table stores:

``` text
table_name
last_processed_date
last_run_timestamp
```

This provides a simple processing-control mechanism for tracking the
latest processed business date and the time of the pipeline run.

------------------------------------------------------------------------

## 9. Evaluation Story

A reviewer can understand the project through five questions.

### What problem did you solve?

Historical daily revenue can become incomplete when transactions arrive
after their actual transaction date.

### How did you detect it?

By comparing `ingestion_date` with `txn_date`.

### What happened after detection?

The pipeline identified only the historical dates affected by late
transactions.

### How was revenue corrected?

The complete Silver data for those dates was aggregated again and the
corrected values were applied to Gold with Delta MERGE.

### How did you make the pipeline trustworthy?

Raw data was preserved in Bronze, validation and deduplication were
performed in Silver, final sanity checks were added, and processing
progress was recorded in a watermark table.

------------------------------------------------------------------------

## 10. One-Minute Explanation

> I built a Databricks pipeline to handle late-arriving transactions and
> correct historical daily revenue. I used Auto Loader to ingest the raw
> CSV into Bronze, cleaned and deduplicated the data in Silver, and
> generated a first-pass daily revenue report in Gold. Then I compared
> the transaction date with the ingestion date to find late
> transactions. Instead of rebuilding the complete historical Gold
> table, I identified only the affected dates and recalculated their
> complete revenue from Silver. Finally, I used Delta MERGE to update
> those dates while leaving unaffected dates alone. I also added
> data-quality checks and a watermark control table for processing
> tracking.

------------------------------------------------------------------------

## 11. Technical Terms

  -----------------------------------------------------------------------
  Term                                Meaning in this project
  ----------------------------------- -----------------------------------
  Auto Loader                         Incremental file ingestion used for
                                      Bronze

  Bronze                              Raw/near-source transaction layer

  Silver                              Cleaned, validated and deduplicated
                                      layer

  Gold                                Daily revenue reporting layer

  Late transaction                    `ingestion_date > txn_date`

  Affected date                       A transaction date associated with
                                      a late transaction

  Recalculation                       Recomputing complete daily revenue
                                      from Silver

  Delta MERGE                         Targeted update/insert into Gold

  Quarantine                          Separate location for invalid
                                      records

  Watermark control                   Tracking table for processing
                                      progress
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 12. Final Architecture

``` text
Raw CSV
   │
   ▼
Auto Loader
   │
   ▼
BRONZE
Raw transaction data
   │
   ▼
SILVER
Type conversion
Validation
Deduplication
   │
   ▼
GOLD
Initial daily revenue
   │
   ▼
Late detection
ingestion_date > txn_date
   │
   ▼
Affected dates
   │
   ▼
Complete revenue recalculation
from Silver
   │
   ▼
Delta MERGE
   │
   ▼
Corrected Gold
   │
   ├──► Data quality checks
   │
   └──► Watermark control
```

------------------------------------------------------------------------

## 13. Final Outcome

The project demonstrates a practical pattern for historical data
correction:

``` text
Detect the problem
        ↓
Measure the impact
        ↓
Locate affected dates
        ↓
Recalculate trusted data
        ↓
Apply a targeted Delta update
        ↓
Validate the result
        ↓
Track processing progress
```

The focus is on solving the business problem with a controlled and
explainable data-engineering workflow.
