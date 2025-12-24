# End-to-End Business Intelligence Pipeline for Banking

This repository implements an **end-to-end Business Intelligence (BI) pipeline** designed for **banking transaction data** (e.g., card / debit transactions).  
The project demonstrates how raw transactional data can be transformed into **analytics-ready datasets** using distributed, containerized data engineering tools.

The goal is to provide a **reproducible, extensible, and production-inspired BI architecture** that can be run locally and adapted for real-world banking analytics use cases.

---

## Architecture Overview

**ER Diagram for Data Warehouse**

<img width="903" height="585" alt="Screenshot 2025-08-28 at 14 47 57" src="https://github.com/user-attachments/assets/31e2744c-7afd-4fd5-a7c6-0ac7df01c9a1" />

The pipeline follows a classic BI flow:

**Source Systems → ETL Processing → Analytical Storage**

- **PostgreSQL** acts as the source / OLTP-style database
- **Apache Spark** performs ETL and transformations
- **ClickHouse** serves as the OLAP / analytical database
- **Docker Compose** orchestrates the full stack
- **Data, diagrams, and database table definitions to document the model and pipeline.


All components run in isolated containers to ensure consistency and reproducibility.

---

## Tech Stack

- **Docker & Docker Compose** – containerized infrastructure
- **PostgreSQL** – transactional/source database
- **Apache Spark** – ETL and data transformations
- **ClickHouse** – analytical (OLAP) database
- **JDBC Drivers** – Spark ↔ database connectivity

---

## Repository Structure

```bash
├── .devcontainer/ # VS Code devcontainer configuration
├── DB_Tables/ # Database table definitions & references
├── Diagrams/ # Architecture & data model diagrams
├── Docker_Spark_in_Container/ # Spark container setup & notes
├── postgres-init/ # PostgreSQL initialization scripts
├── clickhouse-init/ # ClickHouse initialization scripts
├── jars/ # JDBC / connector JAR files
├── data/ # Sample or generated datasets
├── work/ # ETL jobs & working scripts
├── docker-compose.yml # Full stack orchestration
├── .env # Environment variables 
└── README.md
```


---

## Getting Started

### Prerequisites
- Docker
- Docker Compose
- (Optional) VS Code + Dev Containers extension

---

### Environment Configuration

Create a `.env` file in the root directory and define database credentials, ports, and service variables as needed.

---

### Start the Full Stack

```bash
docker compose up -d
