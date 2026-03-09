# RedditPulse – Incremental Social Media Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-red)
![AWS](https://img.shields.io/badge/AWS-Data%20Engineering-orange)
![Docker](https://img.shields.io/badge/Docker-Container-blue)
![Redshift](https://img.shields.io/badge/Amazon%20Redshift-Data%20Warehouse-purple)

RedditPulse is a scalable **data engineering pipeline** that ingests, processes, analyzes, and visualizes Reddit-style social media data using modern data engineering tools.

The pipeline demonstrates **incremental ingestion, sentiment analysis, and analytics dashboards** built using a cloud-based architecture.

---

# Project Overview

Social media platforms generate massive volumes of user-generated content every second. Extracting insights from this data requires scalable data pipelines capable of ingesting, processing, and analyzing continuously arriving data.

This project implements an **end-to-end data engineering pipeline** that processes Reddit-style posts using incremental ingestion, performs sentiment analysis, and generates analytics dashboards.

The system uses **Apache Airflow for orchestration, AWS S3 as a data lake, Amazon Redshift as the data warehouse, and Amazon QuickSight for visualization**.

---

# Architecture
                     +-----------------------+
                     |  Reddit Post Generator |
                     |  (Simulated Streaming) |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  Apache Airflow DAG   |
                     |  (Pipeline Scheduler) |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |   AWS S3 Data Lake    |
                     |                       |
                     |  raw/reddit/incoming  |
                     |  raw/reddit/archive   |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  Amazon Redshift      |
                     |                       |
                     |  Staging Tables       |
                     |  Final Curated Tables |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |  Sentiment Analysis   |
                     |  (NLP Processing)     |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |   Analytics Layer     |
                     |   SQL Views & Metrics |
                     +-----------+-----------+
                                 |
                                 v
                     +-----------------------+
                     |   Amazon QuickSight   |
                     |   Visualization       |
                     +-----------------------+
---

# Technology Stack

| Layer | Technology |
|------|-------------|
| Programming | Python |
| Orchestration | Apache Airflow |
| Containerization | Docker |
| Data Lake | AWS S3 |
| Data Warehouse | Amazon Redshift |
| Data Processing | SQL + Python |
| Analytics | SQL Views |
| Visualization | Amazon QuickSight |

---

# Key Features

- Incremental data ingestion using watermark tracking
- Automated Airflow pipeline orchestration
- Sentiment analysis of Reddit posts
- Data quality validation checks
- Cloud-based analytics dashboards
- Micro-batch streaming simulation

---

# Airflow DAG Workflow

                      +----------------------+
                      |   extract_incremental |
                      |   (Generate new posts)|
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |     upload_to_s3     |
                      |  Upload data to S3   |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |    copy_to_staging   |
                      | Load data to Redshift|
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |  transform_to_final  |
                      | Clean & deduplicate  |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |  sentiment_transform |
                      | Sentiment analysis   |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |   insert_run_metrics |
                      | Track pipeline runs  |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |   validate_sentiment |
                      | Validate sentiment   |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      | dq_post_id_not_null  |
                      | Ensure valid IDs     |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      | dq_no_duplicate_ids  |
                      | Detect duplicates    |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      | create_analytics_views|
                      | Generate analytics   |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |   update_watermark   |
                      | Update ingestion TS  |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |  unload_curated_to_s3|
                      | Export curated data  |
                      +----------+-----------+
                                 |
                                 v
                      +----------------------+
                      |    cleanup_staging   |
                      | Clear temp tables    |
                      +----------------------+

---

# Incremental Data Ingestion

The pipeline implements **incremental ingestion using a watermark mechanism**.

Instead of processing the entire dataset during every pipeline run, the system processes only new records whose timestamps are greater than the last processed timestamp.

This improves pipeline efficiency and prevents duplicate processing.

---

# Sentiment Analysis

Sentiment analysis is applied to Reddit post titles to classify them into three categories:

- Positive
- Neutral
- Negative

Each post receives a **sentiment score** and **sentiment label**, enabling sentiment trend analysis.

Example sentiment table:

| Post ID | Sentiment Score | Sentiment Label |
|-------|------|------|
post_1001 | 0.6 | Positive |
post_1002 | -0.4 | Negative |

---

# Analytics Layer

The analytics layer generates aggregated insights using SQL queries and analytical views.

# QuickSight Dashboard

The analytics views are connected to Amazon QuickSight to build interactive dashboards.

Visualizations include:
	•	Daily post activity
	•	Sentiment distribution
	•	Sentiment trends
	•	Post engagement metrics

# Data Quality Validation

The pipeline performs automated data quality checks to ensure reliability.

Examples include:
	•	Detecting duplicate post IDs
	•	Validating non-null fields
	•	Ensuring data consistency

If data quality checks fail, the Airflow DAG stops execution.

# Example Analytics Insights

The analytics layer enables insights such as:
	•	Overall sentiment distribution
	•	Most active authors
	•	Post engagement levels
	•	Sentiment trends over time

These insights can help organizations understand public opinion and community engagement.

