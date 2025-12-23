# 📚 End-to-End Books ETL Pipeline with Airflow

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.0+-orange.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-336791.svg)

## 📖 Project Overview
This project demonstrates a complete **End-to-End Data Engineering Pipeline** managed by **Apache Airflow** in a Dockerized environment. 

The pipeline automates the process of extracting book data from a website, transforming and loading it into a Data Warehouse (PostgreSQL), and finally generating a business-ready Excel report.

## 🎯 Data Source
* **Target Website:** [Books to Scrape](http://books.toscrape.com/)
* **Description:** A fictional bookstore website specifically designed for web scraping practice.
* **Data Points Extracted:** Book Title, Price, Rating, and Availability.

## ⚙️ Workflow & Architecture
The pipeline follows the standard **ETL (Extract, Transform, Load)** process:

1.  **Extract:** * Uses `Python (Requests + BeautifulSoup4)` to scrape HTML data from the source.
    * Parses book details (Title, Price, Rating).
2.  **Transform & Validate:**
    * Cleans and formats data using `Pandas`.
    * Passes processed data between tasks using **Airflow XCom**.
3.  **Load (Data Warehouse):**
    * Checks if the destination table exists in **PostgreSQL**.
    * Inserts new records using `PostgresHook` and `SQLAlchemy`.
4.  **Reporting:**
    * Queries the Data Warehouse to retrieve the latest dataset.
    * Exports the final report to an Excel file (`.xlsx`) using `OpenPyXL`.

## 📊 Pipeline Flowchart


<img width="531" height="875" alt="image" src="https://github.com/user-attachments/assets/bb832a19-05aa-407f-b249-22714583aca1" />

  

---

## Tech Stack
Orchestration: Apache Airflow

Containerization: Docker & Docker Compose

Database: PostgreSQL

Language: Python 3.x

Libraries: Pandas, BeautifulSoup4, Requests, SQLAlchemy, OpenPyXL, Psycopg2

<img width="1121" height="334" alt="image" src="https://github.com/user-attachments/assets/ed0f0aae-5644-4f83-bed9-16aca2078440" />

