# Glamira Data Pipeline

An automated system for scraping product data and resolving IP geolocation from the Glamira platform.

---

## Features

- **Product Crawling:** Collects product information (price, type, SKU, etc.) based on user interaction IDs.
- **Anti-bot Handling (403/429):** Two-phase approach — Phase 1 for fast scanning, Phase 2 for slow retries using Exponential Backoff and AsyncLimiter to reliably bypass Cloudflare protection.
- **IP Geolocation:** Batch-processes IP addresses from the database and maps them to Country/City using the IP2Location library.

---

## Prerequisites

- Python 3.9 or higher
- [Poetry](https://python-poetry.org/) — dependency and virtual environment management
- MongoDB
- IP2Location binary file: `IP-COUNTRY-REGION-CITY.BIN`

---

## Setup

**Step 1 — Install dependencies**

Navigate to the project root and run the following command. Poetry will automatically create a virtual environment and install all required packages:

```bash
poetry install
```

**Step 2 — Configure environment variables**

Create a `.env` file at the project root and add your MongoDB connection string:

```
MONGO_URI=mongodb://127.0.0.1:27017/
```

**Step 3 — Prepare the IP data file**

Place the IP2Location BIN file at the following path:

```
data/ip_geo/IP-COUNTRY-REGION-CITY.BIN
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

---

## Project Structure

```
glamira_pipeline/
├── data/               # Raw data files (IP2Location .BIN, etc.)
├── logs/               # Runtime log files
├── src/
│   ├── config.py       # Configuration and environment variable loader
│   ├── database.py     # MongoDB connection and query functions
│   ├── crawler/        # Crawling logic, HTML/JSON parsing
│   ├── geo/            # IP-to-location processing logic
│   └── main.py         # Entry point — orchestrates all pipelines
├── .env                # Environment variables
├── .gitignore
├── pyproject.toml      # Poetry dependency manifest
└── README.md
```
