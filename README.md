# Glamira Data Pipeline

An automated system for scraping product data, resolving IP geolocation, exporting data to Google Cloud Storage, and loading it into BigQuery.

---

## Features

- **Product Crawling:** Collects product information (price, type, SKU, etc.) based on user interaction IDs.
- **Anti-bot Handling (403/429):** Two-phase approach — Phase 1 for fast scanning, Phase 2 for slow retries using Exponential Backoff and AsyncLimiter to reliably bypass Cloudflare protection.
- **IP Geolocation:** Batch-processes IP addresses from the database and maps them to Country/City using the IP2Location library.
- **GCS Export:** Dumps MongoDB collections into partitioned `.jsonl` files and uploads them to Google Cloud Storage.
- **BigQuery Load:** A Cloud Function automatically triggers on each uploaded file, transforms the data (flatten MongoDB ObjectIds, parse price strings to float, flatten nested fields), and loads it into BigQuery — one file per load job with automatic retry on rate limits.

---

## Prerequisites

- Python 3.9 or higher
- [Poetry](https://python-poetry.org/) — dependency and virtual environment management
- MongoDB
- IP2Location binary file: `IP-COUNTRY-REGION-CITY.BIN`
- Google Cloud project with the following APIs enabled:
  - Cloud Functions
  - Cloud Build
  - BigQuery
  - Cloud Storage

---

## Setup

**Step 1 — Install dependencies**

Navigate to the project root and run the following command. Poetry will automatically create a virtual environment and install all required packages:

```bash
poetry install
```

**Step 2 — Configure environment variables**

Create a `.env` file at the project root:

```
MONGO_URI=mongodb://127.0.0.1:27017/
```
And change database name (DB_NAME) and bucket name (BUCKET_NAME) with yours in `src/config.py`.

**Step 3 — Prepare the IP data file**

Place the IP2Location BIN file at the following path:

```
data/ip_geo/IP-COUNTRY-REGION-CITY.BIN
```

**Step 4 — Authenticate with Google Cloud**

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

---

## Usage

Activate the Poetry virtual environment before running any command:

```bash
poetry shell
```

Use the `--job` flag to specify which pipeline to run:

**Crawl product data:**

```bash
python src/main.py --job crawl
```

Crawl logs are saved to `logs/pipeline.log`.

**Process IP geolocation:**

```bash
python src/main.py --job geo
```

**Export MongoDB collections to GCS:**

```bash
python src/export_gcs.py
```

Exports the following collections as partitioned `.jsonl` files to `gs://{BUCKET_NAME}/exports/{collection}/`:

| Collection | GCS folder | Rows per file |
|---|---|---------------|
| `ip_locations` | `exports/ip_locations/` | 200,000       |
| `product_dictionary` | `exports/product_dictionary/` | 100,000       |
| `summary` | `exports/summary_raw/` | 200,000       |

**Deploy the Cloud Function and backfill all data:**

```bash
cd cloud_function
bash deploy.sh
```

This script will:
1. Enable required GCP APIs
2. Grant IAM permissions to the Cloud Function service account
3. Deploy the `gcs_to_bq_trigger` Cloud Function
4. Truncate existing BigQuery tables
5. Re-trigger all files in GCS sequentially (35s apart) to avoid BigQuery rate limits

---

## Architecture

```
MongoDB
  │
  │  export_gcs.py (partitioned .jsonl, batch upload)
  ▼
GCS bucket: exports/{table}/*.jsonl
  │
  │  google.storage.object.finalize (event trigger)
  ▼
Cloud Function: gcs_to_bq_trigger
  │  - flatten _id.$oid → STRING
  │  - parse price STRING → FLOAT64
  │  - flatten option{} → alloy, diamond, shapediamond columns
  │  - 1 file = 1 BQ load job (retry on 429)
  ▼
BigQuery dataset: raw_layer
  ├── ip_locations
  ├── product_dictionary
  └── summary_raw
```

