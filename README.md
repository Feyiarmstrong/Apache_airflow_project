# Wikipedia Pageviews ETL Pipeline

An automated **Apache Airflow ETL pipeline** that ingests Wikipedia pageview data and analyzes public interest trends for major tech companies.

This pipeline to explore how Wikipedia traffic can act as a **proxy for market sentiment**.

---

## Table of Contents

- Project Overview  
- Business Context  
- Architecture  
- Tech Stack  
- Project Structure  
- Pipeline Workflow  
- Setup  
- Configuration  
- Usage  
- Results  
- Key Features  
- Challenges  
- Future Enhancements  

---

## Project Overview

Built a full end-to-end ETL pipeline that:

- Downloads hourly Wikipedia pageview dumps  
- Processes large compressed files efficiently  
- Filters data for selected tech companies  
- Loads clean results into PostgreSQL  
- Produces ranked sentiment insights  

**Companies tracked**

- Amazon  
- Apple  
- Google  
- Microsoft  
- Facebook (Meta)  

---

## Business Context

**Use case: Sentiment-driven market insights**

Assumptions:

- Rising Wikipedia pageviews = growing public interest  
- Falling pageviews = declining attention  

This pipeline supports a hypothetical **stock sentiment analytics tool** for a data consulting firm.

**Data source**

- Wikimedia Foundation public pageview dumps  
- Hourly aggregates  
- Available from 2015 onward  

---

## Architecture

```

Wikipedia Dumps
      │
      ▼
Apache Airflow (Docker)

Download → Extract → Filter → Load → Analyze
                              │
                              ▼
                        PostgreSQL
```

---

## Tech Stack

- **Orchestration:** Apache Airflow 3.1  
- **Language:** Python 3.12  
- **Database:** PostgreSQL 16  
- **Containerization:** Docker & Docker Compose  

---

## Project Structure

```

wikipedia-pageviews-pipeline/

├── dags/

│   └── wikipedia_pageviews_dag.py

├── wikipedia_project/

│   ├── tasks.py

│   ├── config/

│   │   └── companies.json

│   ├── scripts/

│   │   ├── download.py

│   │   ├── extract.py

│   │   ├── filter.py

│   │   └── load.py

│   └── sql/

│       └── create_tables.sql

├── data/

│   └── wikipedia_project/

│       ├── raw/

│       └── processed/

├── docker-compose.yaml

├── requirements.txt

└── README.md

```

---

## Pipeline Workflow

1. **Download**  
   Pulls hourly Wikimedia dumps. Skips if file exists.

2. **Extract**  
   Decompresses `.gz` files using chunked processing.

3. **Filter**  
   Keeps only tracked company pages.

4. **Load**  
   Bulk inserts into PostgreSQL with deduplication.

5. **Analyze**  
   Ranks companies by pageviews and logs results.

---

## Setup

```bash

git clone https://github.com/yourusername/wikipedia-pageviews-pipeline.git

cd wikipedia-pageviews-pipeline

docker-compose up -d

```

Airflow UI  

- http://localhost:8080  

- airflow / airflow  

---

## Configuration

Edit tracked companies in `companies.json`.

Set the target date in `tasks.py`.

---

## Results

Got ranked pageviews, a top company indicator, and persisted historical data.

---

## Key Features

- Idempotent design  

- Modular Python logic  

- Chunked file processing  

- Bulk database inserts  

- Strong logging  

---

## Future Enhancements

- Data quality checks  

- Dashboards  

- Stock price integration  

- Machine learning sentiment models  
