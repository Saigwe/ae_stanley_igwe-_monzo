# Monzo Analytics Engineering Take-Home

**Author:** Stanley Igwe

**Role:** Senior Analytics Engineer

**Warehouse:** BigQuery

**Transformation Layer:** dbt


## Overview

This repository contains an end-to-end analytics engineering solution for modelling Monzo account lifecycles and user activity metrics.

The primary goals of this project are:

* **Correctness over cleverness**
* **Explicit, inspectable business logic**
* **Backfill-safe, reproducible models**
* **Clear documentation and governance**
* **Production-ready analytics engineering practices**

Every design decision in this project is intentional and documented so that another analytics engineer can safely extend or maintain the models without additional context.


## High-Level Architecture

The project follows a **layered analytics engineering architecture**:

```
Raw Sources (BigQuery)
        ↓
Staging Models (stg_)
        ↓
Intermediate Models (int_)
        ↓
Mart Models (dim_ / fct_)
```

Each layer has a **clear responsibility** and **strict boundaries**.


## Source Data

The upstream dataset (`monzo_datawarehouse`) contains append-only event logs that are **fully refreshed nightly**:

* `account_created`
* `account_closed`
* `account_reopened`
* `account_transactions`

### Key Characteristics of the Source Data

* No enforced contracts
* Event-driven (lifecycle-based)
* Fully refreshed (not incremental)
* Subject to change as backend systems evolve

**Design implication:** downstream models must be resilient to change, reorderings, and late-arriving events.


## Staging Layer (`stg_*`)

### Purpose

The staging layer provides a **clean, stable interface** to raw source tables.

### What happens here

* Column renaming and standardisation
* Data type normalisation
* Light semantic cleanup
* No business logic
* One-to-one mapping with source tables

### Why this is best practice

* Shields downstream models from upstream schema changes
* Makes raw data easier to reason about
* Keeps business logic out of ingestion concerns
* Enables targeted data quality tests early in the pipeline

Staging models are materialised as **views** to avoid unnecessary storage while keeping logic inspectable.


## Intermediate Layer (`int_*`)

### Purpose

The intermediate layer contains **core business logic** and **semantic transformations** that should not be duplicated.

This is where the data starts to mean something.

### Key Models

#### `int_account_lifecycle_events`

* Canonical event stream for account lifecycle events
* Normalises `created`, `closed`, and `reopened` into a single ordered sequence
* Deterministic ordering using event timestamps and row sequencing

**Why this exists:**
Lifecycle logic is easier to reason about when all events live in one table.


#### `int_account_state_transitions`

* Converts lifecycle events into **state validity windows**
* Produces a slowly changing dimension backbone
* Guarantees non-overlapping, contiguous state periods per account

**Why this exists:**
State-based questions (“was this account open on date X?”) are extremely common and should not be re-implemented ad hoc by analysts.


#### `int_user_open_status_daily`

* Derives whether a user had at least one open account on each day
* Provides point-in-time eligibility logic for metrics

**Why this exists:**
Eligibility logic should be centralised and reused to avoid metric drift.


#### `int_user_daily_activity`

* Derives daily user activity based on transaction presence
* Separates “activity detection” from metric aggregation

**Why this exists:**
This allows multiple engagement metrics to share the same activity definition.


## Mart Layer (`dim_*`, `fct_*`)

The mart layer contains **analytics-ready tables** intended for direct consumption.

These models are documented with:

* Purpose
* Grain
* Assumptions
* Limitations
* Ownership and governance metadata


### `dim_accounts` (Current-State Dimension)

* One row per account
* Represents the latest known account state
* Intended for dashboards, reporting, and metric denominators

**Why separate from history?**

Separating current-state and historical dimensions:

* Makes usage intent explicit
* Prevents accidental misuse
* Simplifies joins for common reporting use cases


### `dim_accounts_history` (Historical SCD)

* One row per account per continuous validity period
* Enables point-in-time and lifecycle analysis

**Why full refresh instead of incremental?**

* Source data is fully refreshed nightly
* Lifecycle logic is event-order dependent
* Full refresh guarantees correctness and simplifies backfills
* Incremental logic would introduce unnecessary risk


### `fct_7d_active_users_daily`

* Daily 7-day rolling active users metric
* Includes both numerator and denominator
* Point-in-time correct and historically reproducible

**Metric definition:**

* **Eligible user:** user with ≥1 open account on the reporting date
* **Active user:** eligible user with ≥1 transaction in the last 7 days

**Why include numerator and denominator together?**

* Prevents inconsistent metric definitions
* Makes the metric auditable
* Enables downstream rate calculations without ambiguity


## Why Not Incremental Models?

Incremental models require careful reasoning about:

* Unique keys
* Merge logic
* Late-arriving data
* Backfill strategy
* State transitions

Given:

* Fully refreshed source tables
* Event-driven lifecycle logic
* High correctness requirements

**Full refresh models were the safest and most appropriate choice.**

This prioritises:

* Determinism
* Backfill safety
* Simplicity
* Reviewer confidence


## Schema Management (Multi-Developer Safe)

A custom `generate_schema_name` macro is used to ensure:

* Each developer writes to their own schema in `dev`
* No accidental overwrites
* Production schemas remain stable

This is standard practice in mature dbt teams.


## Data Quality & Testing

The project includes:

* Not null tests
* Uniqueness tests
* Accepted values tests
* Semantic integrity checks

Tests are applied at:

* Staging (basic sanity)
* Intermediate (business logic validation)
* Mart (contract enforcement)


## SQL Quality & Style

SQL quality is enforced using **sqlfluff** with dbt templating:

* Consistent indentation
* Explicit joins
* No hidden subqueries
* Readable, reviewable SQL

All models are expected to be lint-clean before review.


## Documentation Philosophy

Documentation is treated as a **first-class deliverable**, not an afterthought.

Every mart model documents:

* **Purpose** – why the model exists
* **Grain** – exactly what one row represents
* **Assumptions** – what must be true for correctness
* **Limitations** – when *not* to use the model

This ensures:

* Safe reuse
* Faster onboarding
* Fewer downstream questions
* Better long-term maintainability


## How to Work With This Project

1. Clone the repository
2. Configure your `profiles.yml`
3. Run:

   ```bash
   dbt run
   dbt test
   dbt docs generate
   dbt docs serve
   ```
4. Explore models via dbt Docs

The documentation and lineage graphs should provide everything needed to understand and extend the project.


## Final Notes

This project is intentionally designed to reflect **real-world fintech analytics engineering standards**:

* Correctness over speed
* Explicit over implicit
* Safe defaults
* Clear ownership
* Backfill-ready logic

The goal is not just to answer the task, but to demonstrate how analytics engineering should be done **in production at scale**.


If you have questions while reviewing this project, kindly reach out and we can collaborate.
