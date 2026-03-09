# Reddit Data Engineering Pipeline

This project implements an incremental Reddit data pipeline using modern data engineering tools.

## Technologies Used
- Apache Airflow
- Docker
- AWS S3
- Amazon Redshift
- Python
- Amazon QuickSight

## Pipeline Features
- Incremental ingestion using watermark
- Sentiment analysis of Reddit posts
- Automated Airflow DAG orchestration
- Data quality validation
- Analytics views and dashboards

## Architecture
Reddit Generator → Airflow → S3 → Redshift → Sentiment Analysis → QuickSight