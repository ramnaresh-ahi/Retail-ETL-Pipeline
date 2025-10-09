<<<<<<< development
# Retail ETL Pipeline

An end-to-end ETL project demonstrating extraction from Kaggle, transformation with Polars, and loading into PostgreSQL via Docker. Built as a portfolio showcase of modern data engineering best practices.

---

## Table of Contents

- [Problem Statement](#problem-statement)  
- [Solution Overview](#solution-overview)  
- [Key Business Problems Solved](#key-business-problems-solved)  
- [Architecture & Components](#architecture--components)  
- [Tech Stack](#tech-stack)  
- [Setup & Installation](#setup--installation)  
- [Running the Pipeline](#running-the-pipeline)  
- [Challenges & Resolutions](#challenges--resolutions)  
- [Features](#features)  
- [Project Structure](#project-structure)  
- [License](#license)  

---

## Problem Statement

Many retail businesses struggle to integrate data from public sources into analytics databases. This project ingests an online sales dataset from Kaggle, cleans and normalizes it, and loads it into PostgreSQL for analysis. The goal was to build a production-grade ETL pipeline that:

1. Downloads and caches raw data  
2. Applies complex data-quality rules  
3. Normalizes into a star schema  
4. Validates and bulk-loads into PostgreSQL  
5. Provides monitoring, logging, and automated orchestration  

---

## Solution Overview

1. **Extract**:  
   - Load environment variables from `.env`  
   - Authenticate and download from Kaggle API  
   - Validate file integrity and metadata  

2. **Transform**:  
   - Use Polars for high-performance data processing  
   - Deduplicate orders and fix financial calculation errors  
   - Filter out invalid records (negative prices, zero quantities)  
   - Normalize into `customers`, `products`, and `orders` tables  

3. **Load**:  
   - Run schema SQL in PostgreSQL (Docker container)  
   - Bulk insert data with optimized statements  
   - Create indexes and analytical views  
   - Verify referential integrity and business metrics  

4. **Orchestration**:  
   - Single `main.py` entry point  
   - Command-line flags for force extract  
   - Comprehensive logging and JSON summary  

---

## Key Business Problems Solved

This project tackled several major real-world data challenges:

- **Removing duplicate records:**  
  Removed millions of exact and partial duplicate order lines while preserving multi-item orders intact. This prevents double-counting and incorrect reporting.

- **Fixing miscalculations in financial columns:**  
  Corrected misaligned data where:
  - `value` column did not match `qty_ordered * price`
  - `total` column was inconsistent with `corrected_value - discount_amount`

  Fixing these ensures financial metrics like revenue and discounts are accurate, vital for trustable business decisions.

- **Removing unnecessary or less important columns:**  
  Dropped redundant or irrelevant columns from the raw data, streamlining processing and reducing storage costs without losing analytic value.

These transformations form the heart of the project’s business impact and demonstrate maturity handling complex, messy retail data.

---

## Architecture & Components

- **Extraction**: `src/extract.py`  
- **Transformation**: `src/transform.py`  
- **Loading**: `src/load.py`  
- **Orchestration**: `main.py`  
- **SQL Schema & Views**: `sql/create_tables.sql`  
- **Docker**: PostgreSQL container for isolation  
- **Config**: `.env` for credentials  

---

## Tech Stack

- Python 3.12
- Polars for data processing  
- psycopg2 for PostgreSQL connectivity  
- python-dotenv for environment management  
- Kaggle API for dataset download  
- Docker & Docker Compose for PostgreSQL  
- Git & Bash/PowerShell for automation  

---

## Setup & Installation

1. **Clone the repository** 

```bash
git clone https://github.com/ramnaresh-ahi/Retail-ETL-Pipeline.git
cd Retail-ETL-Pipeline.git
```

2. **Create and activate virtual environment**  
```
python -m venv retailenv
source retailenv/bin/activate # Linux/Mac
retailenv\Scripts\activate # Windows
```
3. **Install dependencies**
```
pip install -r requirements.txt
```

5. **Copy `.env.example` to `.env` and fill in credentials**  

cp .env.example .env
```
KAGGLE_USERNAME=<your_username>
KAGGLE_KEY=<your_key>
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=yourpassword
```

### Start PostgreSQL Docker container
```
docker-compose up -d
```
---

## Running the Pipeline

**To run end-to-end (extract → transform → load):**
```
python main.py
```

**To force re-download raw data:**
```
python main.py --force-extract
```

**On success, expect:**
{ "success": true, "duration": 78.85 }

---

## Challenges & Resolutions

### Column Case Sensitivity
- **Issue:** "Gender" vs gender mismatch in PostgreSQL  
- **Fix:** Quoted identifiers or normalize CSV columns

### Mixed Types in Orders CSV
- **Issue:** `order_id` like `100468520-1` inferred as integer  
- **Fix:** Force string dtype in Polars

### Kaggle API Parameter Change
- **Issue:** `dataset_list(page_size=…)` error after API upgrade  
- **Fix:** Remove unsupported params & fallback to authenticate()

### Orchestrator Scope Error
- **Issue:** `validate_extracted_data` nested inside constructor  
- **Fix:** Move method out; ensure `extract_results` in scope

---

## Features

- Automated caching and backup of raw data  
- Comprehensive data-quality reporting and metadata  
- High-performance transformation and cleaning with Polars  
- Bulk loading and referential integrity checks  
- Dockerized PostgreSQL for reproducibility  
- Command-line flags for flexible operation  
- Detailed logging and JSON summary  

---

## Project Structure
```
├── data/
│   ├── raw/ # Raw sales.csv
│   ├── processed/ # Transformed CSVs
│   ├── metadata/ # Extraction metadata JSON
│   └── backup/ # Backups of raw data
├── images/ # Project related images and diagrams
├── logs/ # Pipeline logs
├── results/ # JSON run results
├── sql/
│   └── create_tables.sql
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── main.py # Orchestration script
├── docker-compose.yml # PostgreSQL Docker setup
├── requirements.txt
├── .env.example
└── README.md
```
---

## License

MIT License
=======
>>>>>>> main

