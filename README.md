# Supply Chain Data Model

A comprehensive data engineering project implementing a Medallion Architecture (Bronze, Silver, Gold) for supply chain analytics using Databricks, PySpark, and Unity Catalog.

## Overview

This project ingests raw supply chain data from SAP ERP and SAP Integrated Business Planning (IBP), processes it through a multi-hop data architecture, and models it into a star schema (Facts and Dimensions) optimized for business intelligence and reporting in Power BI.

## Architecture

The data pipeline follows the Databricks Medallion Architecture:

1. **Setup (Unity Catalog)**: Initializes the `supply_chain_catalog` and the necessary schemas (`bronze`, `silver`, `gold`).
2. **Bronze Layer (Raw)**: Ingests raw CSV files from SAP and SAP IBP into Delta tables without applying transformations. This layer acts as a historical archive of raw data.
3. **Silver Layer (Cleansed)**: Cleanses, filters, and standardizes the data. It handles data type casting, null handling, and basic joins to prepare the data for business-level modeling.
4. **Gold Layer (Curated)**: Transforms the cleansed data into a star schema consisting of Dimension tables (e.g., Material, Plant, Vendor) and Fact tables (e.g., Purchasing, Inventory, Quality). This layer is optimized for querying and reporting.

## Data Sources

The project processes data from two primary sources:
* **SAP Raw Data**: Core ERP tables including Material Master (MARA, MARC), Purchasing (EKKO, EKPO), Inventory (MARD, MBEW), Quality Management (QALS, QAVE), and Vendor Master (LFA1).
* **SAP IBP Raw Data**: Integrated Business Planning data including Demand Actuals, Demand Forecasts, and Master Data (Customer, Location, Product).

## Project Structure

```text
supply-chain-data-model/
├── 00_setup_unity_catalog.py         # Initializes Unity Catalog and schemas
├── 01_bronze_layer_ingestion.py      # Ingests raw CSVs into Bronze Delta tables
├── 02_silver_layer_transformation.py # Cleanses and transforms data into Silver layer
├── 03_gold_layer_dimensions.py       # Creates Dimension tables in Gold layer
├── 04_gold_layer_facts.py            # Creates Fact tables in Gold layer
└── usecase1(problem statement)/      # Contains raw data and documentation
    ├── POWER_BI_SETUP_GUIDE/         # Instructions for Power BI integration
    ├── SAP_IBP_RAW_DATA/             # Raw CSV files for SAP IBP
    ├── SAP_RAW_DATA/                 # Raw CSV files for SAP ERP
    └── TABLE_MAPPING_SHEETS/         # Data mapping and transformation rules
```

## How to Run

To execute this data pipeline in your Databricks environment:

1. **Prerequisites**: Ensure you have a Databricks workspace with Unity Catalog enabled.
2. **Data Upload**: Upload the contents of the `usecase1(problem statement)/SAP_RAW_DATA` and `SAP_IBP_RAW_DATA` folders to a Databricks Volume or DBFS location accessible by your cluster. Update the file paths in `01_bronze_layer_ingestion.py` if necessary.
3. **Execution Order**: Import the Python scripts as Databricks Notebooks and run them sequentially:
   * Run `00_setup_unity_catalog.py` first to create the catalog and schemas.
   * Run `01_bronze_layer_ingestion.py` to load the raw data.
   * Run `02_silver_layer_transformation.py` to cleanse the data.
   * Run `03_gold_layer_dimensions.py` to build the dimensions.
   * Run `04_gold_layer_facts.py` to build the facts.

## Reporting & Visualization

Once the Gold layer tables are populated, you can connect Power BI to your Databricks SQL Warehouse or cluster. Refer to the `POWER_BI_SETUP_GUIDE` folder for detailed instructions on setting up the dashboards and visualizing the supply chain metrics.
