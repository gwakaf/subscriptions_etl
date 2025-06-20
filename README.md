# EMR SPARK ETL Pipeline
Data pipeline handling daily data ingestion with spark jobs on AWS infrastructure applying SDC (slow changing dimensions) type 2 data modeling approach.


## Table of Contents
- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
  - [ETL DAG](#etl-dag)
  - [Data Modeling Approach](#data-modeling-approach)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)


## Overview
This pipeline processes meadia application subscriptions data, tracking changes in transactions, subscriptions and users information.
Data is updated on daily basis, ingested by incoming .csv files, validated, transformed and stored.


## Tech Stack 
AWS S3, EMR, Spark, AWS Glue Crawler, AWS Glue Data Catalog, Airflow.

## Architecture
![Alt Text](https://github.com/user-attachments/assets/1259933c-a65a-4363-b6b1-00ded81380ee)

### ETL DAG
![Alt Text](https://github.com/user-attachments/assets/e7571268-6d32-407e-9130-637a8d3141ac)

+ Pipeline is orchestrated with Airflow 
+ Sensor is waiting for new files to be uploaded to S3 bucket 
+ New files are copied to SSOT S3 location
+ Spark job reads the files, checks the schemas
+ Spark job transformes the data, applies SCD approach
+ Spark job updates the existed records
+ Spark job write partitioned data as parquet files
+ AWS Glue Crawler is triggered to reference new files in the existing AWS Glue Catalog
+ Clean up
  
### Data Modeling Approach
#### Incoming Data
![Screenshot 2025-06-10 at 12 53 27 PM](https://github.com/user-attachments/assets/6cf5512b-a563-433a-bb7b-c6f1f8422696)
For dimensional tables this project uses SCD type 2 approach to have active records available as the latest snapshot and retain historical data, implemented with pyspark jobs:
+ Load incoming .csv to a data frame df1
+ Load existed data to a data frame df2 (the latest snapshot of all active records based on batch_date -1 day)
+ Add necessary fields to df1: 
	- eff_start_date (DateType()) - The date when the record becomes valid.
	- eff_end_date (DateType(), nullable) - The timestamp when the record is replaced by a new version. If NULL, the record is the current active version.
#### Data with SCD modifications applied
![Screenshot 2025-06-10 at 12 53 35 PM](https://github.com/user-attachments/assets/eebf5ca1-4a49-4016-8009-ee9804665857)
+ Union both dataframes
+ Create Window function to define row_numbers for repeated fields that should be unique
+ Add the row_numbers column to the union data frame
+ Filter the active records based on row_number and other applied conditions -> creating the latest snapshot
+ Filter the inactive records based on row_number and other applied conditions -> creating the historic data snapshot

## Getting Started
### Prerequisites
- docker to run airflow
- AWS account
- AWS S3 bucket with existed data
- AWS Glue Data Catalog
- AWS Glue Crawler
  
### Configuration
User has to set up environmental variables for:
- AWS S3
- AWS EMR
- AWS Glue

The required  dependencies are defined in docker-compose.yaml file as PIP_ADDITIONAL_REQUIREMENTS

### Installation
1. Clone the repository  
2. Add your .env file with environment variables (refer to the configuration section for details).
3. Start the Docker containers using the docker-compose.yaml file:
   ```bash
   docker-compose up
4. Trigger airflow dag manually or schedule it

## Usage
+ ETL pipeline runs daily orchestrated by airflow, getting new data files, transforming and saving them with pyspark jobs

+ AWS cost estimate, assuming that user uses free tier account, and configuration is minimal.
  
| **Service** | **Cost Breakdown**                                                                                                                                                     | **Total Monthly Cost**  |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| **EMR**     | m5.xlarge (4 vCPU, 16 GB RAM) (1 core node only) x $0.096/hour x 15 hours/month (30 min/day)            |  $1.44 |
| **Glue Crawler**    | 2 minutes per run on 1 DPU,1 DPU-hour × $0.44/hour  (2 min × 30 days = 60 min = 1 DPU-hour)                                                  | $0.44   |
| **Glue Data Catalog**    | First 1M objects/month → free, First 1M requests/month → free                                       | $0 |
| **S3 for storage**    | Assume that the data < 5 GB and doesn’t exceed a few thousand requests/day → free                                       | $0 |

  Total: ~$1.88/month





   
