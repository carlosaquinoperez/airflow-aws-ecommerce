# 🚀 Enterprise E-commerce Data Pipeline (AWS + Airflow)

![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACD5F2?style=for-the-badge&logo=databricks&logoColor=black)

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Business Impact](#-business-impact)
- [Architecture](#️-architecture)
- [Tech Stack](#️-tech-stack)
- [Repository Structure](#-repository-structure)
- [Deployment Guide](#-deployment--execution-guide)
- [Pipeline Flow](#-pipeline-flow)

---

## 📋 Project Overview

### The Problem

A rapidly growing US-based E-commerce platform generates massive amounts of sales and logistical data daily. This data is **siloed and processed manually**, leading to:

- ❌ Delayed business insights
- ❌ Inconsistent reporting across departments
- ❌ Lack of scalability during high-volume seasons (e.g., Black Friday)

### The Solution

A **Production-Grade Automated Data Pipeline** built on the Modern Data Stack (AWS + Airflow + Delta Lake), implementing the following key requirements:

| # | Requirement | Implementation |
|---|-------------|----------------|
| 1 | **Automated Ingestion** | Apache Airflow ingests raw CSV data from On-Premise sources into S3 Bronze Layer |
| 2 | **Standardized Processing** | Medallion Architecture with AWS Glue (PySpark) for cleaning, deduplication, and ACID transactions |
| 3 | **Infrastructure as Code** | 100% automated cloud provisioning via Terraform with S3 Remote State |
| 4 | **CI/CD Pipeline** | GitOps workflow using GitHub Actions for secure infrastructure deployment on merge |

---

## 📈 Business Impact

- ⚡ **Reduces time-to-insight** from days to minutes
- ✅ **Ensures data consistency** across Marketing, Logistics, and Finance
- 📦 **Provides a scalable foundation** for future Machine Learning initiatives (Demand Forecasting & Churn Prediction)

---

## 🏗️ Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE FLOW                           │
│                                                                     │
│  On-Premise CSV  ──►  S3 Bronze Layer  ──►  S3 Silver Layer         │
│  (Raw Source)         (Raw CSV)             (Cleaned Delta)         │
│                                                     │               │
│        Apache Airflow (Orchestration)               ▼               │
│        AWS Glue PySpark (Transformation)      S3 Gold Layer         │
│        Terraform + GitHub (IaC + CI/CD)       (Metrics Delta)       │
└─────────────────────────────────────────────────────────────────────┘
```

**Medallion Architecture:**

```text
Bronze Layer  →  Raw data as-is (CSV ingested from source)
Silver Layer  →  Cleaned, validated, deduplicated, and typed (Delta format)
Gold Layer    →  Business-ready aggregations & metrics (Delta format)
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| **Orchestration** | Apache Airflow 2.7 (Dockerized) |
| **Processing Engine** | AWS Glue — Serverless PySpark |
| **Data Lake Storage** | Amazon S3 & Delta Lake Framework |
| **Infrastructure as Code** | Terraform |
| **CI/CD & Version Control** | GitHub Actions & Git |
| **Language** | Python 3.8+ |

---

## 📂 Repository Structure

```text
airflow-aws-ecommerce/
├── .github/
│   └── workflows/
│       └── terraform_deploy.yml     # CI/CD: GitOps pipeline for Terraform
├── dags/
│   └── ecommerce_sales_dag.py       # Airflow DAG — pipeline orchestration
├── data/
│   └── olist_orders_dataset.csv     # Simulated On-Premise source data (git-ignored)
├── docker/
│   ├── docker-compose.yaml          # Local container environment
│   └── .env                         # Local AWS credentials (git-ignored)
├── scripts/
│   ├── transform_sales_data.py      # PySpark ETL script (Bronze to Silver)
│   └── create_gold_metrics.py       # PySpark ETL script (Silver to Gold)
├── terraform/
│   ├── main.tf                      # S3 Backend, Providers, Medallion Buckets
│   ├── iam.tf                       # IAM Users, Policies, and Access Keys
│   └── etl.tf                       # AWS Glue Jobs and IAM Roles
├── .gitignore                       # Security and cache exclusion rules
└── README.md                        # Project documentation
```

---

## 🚦 Deployment & Execution Guide

### Step 1: Prerequisites & Security

1. You must have an **AWS Account** with administrative access.
2. Generate an **Access Key ID** and **Secret Access Key** from IAM in AWS.
3. In your GitHub repository, go to **Settings → Secrets and variables → Actions** and add:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
4. Locally, inside the `docker/` folder, create a `.env` file:

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

---

### Step 2: Infrastructure as Code (GitOps)

This project uses **Remote State** for Terraform.

1. Manually create an S3 bucket in AWS to store Terraform state (e.g., `ecommerce-tfstate-yourname`).
2. Update the `backend "s3"` block in `terraform/main.tf` with your bucket name.
3. Push code to the `main` branch.

GitHub Actions will automatically trigger and:
- Initialize Terraform
- Provision S3 Medallion buckets
- Create IAM Roles
- Deploy the AWS Glue Jobs

---

### Step 3: Local Orchestration Setup (Dockerized Airflow)

Navigate to the `docker/` directory and run the following commands:

```bash
cd docker

# Initialize the Airflow Database
docker compose run --rm webserver airflow db migrate

# Create the Admin User
docker compose run --rm webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start all services in detached mode
docker compose up -d
```

Access the Airflow UI at **http://localhost:8080** (or the forwarded port in Codespaces) and log in with the credentials created above.

---

### Step 4: Executing the Pipeline

1. Place your raw dataset (`olist_orders_dataset.csv`) inside the `/data` folder.
2. In the Airflow UI, locate the DAG named **`ecommerce_ingestion_bronze_to_silver`**.
3. **Unpause** the DAG (toggle the switch to active).
4. Click the **Trigger DAG** ▶️ button to start the automated process.

---

### Step 5: Data Catalog & Querying (Amazon Athena)

Once the pipeline successfully processes the data into the Silver and Gold layers (Delta Lake format), you need to register them in the **AWS Glue Data Catalog** to enable SQL querying.

> 💡 **Note:** This is a one-time foundational step. Thanks to Delta Lake's native **Schema Evolution**, future column additions will be handled automatically without needing to rerun this DDL.

1. Navigate to **Amazon Athena** in the AWS Console.
2. Select the `ecommerce_db` database.
3. Execute the following DDL commands to register the Delta tables:

**Register Silver Layer (Transactional Data):**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.silver_orders
LOCATION 's3://YOUR_SILVER_BUCKET_NAME/cleaned/orders/'
TBLPROPERTIES ('table_type'='DELTA');
```

**Register Gold Layer (Business Metrics):**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.gold_orders_by_status
LOCATION 's3://YOUR_GOLD_BUCKET_NAME/metrics/orders_by_status/'
TBLPROPERTIES ('table_type'='DELTA');
```

4. You can now query your Lakehouse! Example — querying aggregated business value:

```sql
SELECT * FROM ecommerce_db.gold_orders_by_status
ORDER BY total_orders DESC;
```

---

## 🔄 Pipeline Flow

```text
Task 1: validate_source_file
    └── Verifies local file existence

Task 2: upload_to_s3_bronze
    └── Authenticates via boto3
    └── Uploads raw CSV to S3 Bronze Layer
    └── Applies dynamic daily partitioning

Task 3: trigger_glue_etl_job
    └── Triggers Serverless AWS Glue Job (GlueJobOperator)
    └── Cleans nulls, casts timestamps, and deduplicates via Window Functions
    └── Upserts (MERGE) refined data to S3 Silver Layer (Delta format)

Task 4: calculate_gold_metrics
    └── Triggers Serverless AWS Glue Job (GlueJobOperator)
    └── Reads Silver data and aggregates metrics by order_status
    └── Overwrites S3 Gold Layer (Delta format) for BI consumption
```

---

## 🔒 Security Notes

- **Never commit** your `.env` file or any AWS credentials to version control.
- The `.gitignore` is pre-configured to exclude credentials and raw data files.
- All IAM roles follow the principle of **least privilege**.

---

## 📄 License

Carlos Aquino Pérez — Senior Data Engineer