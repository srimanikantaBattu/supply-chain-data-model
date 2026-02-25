# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE LAYER - Raw Data Ingestion
# MAGIC ## Supply Chain Data Model
# MAGIC
# MAGIC **Purpose**: Ingest all raw CSV files from SAP and SAP IBP into Bronze layer using Unity Catalog
# MAGIC
# MAGIC **Data Sources**:
# MAGIC - SAP Raw Data (42 tables)
# MAGIC - SAP IBP Data (6 tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re

# Create widgets for user input
dbutils.widgets.text("sap_raw_path", "/Volumes/workspace/default/sparkwars/SAP_RAW_DATA/", "SAP CSV Files Path")
dbutils.widgets.text("ibp_raw_path", "/Volumes/workspace/default/sparkwars/SAP_IBP_RAW_DATA/", "IBP CSV Files Path")

# Get paths from widgets
SAP_RAW_PATH = dbutils.widgets.get("sap_raw_path")
IBP_RAW_PATH = dbutils.widgets.get("ibp_raw_path")

# Set Unity Catalog context
spark.sql("USE CATALOG supply_chain_catalog")
spark.sql("USE SCHEMA bronze")

print("=" * 80)
print("CONFIGURATION")
print("=" * 80)
print(f"📁 SAP CSV Path: {SAP_RAW_PATH}")
print(f"📁 IBP CSV Path: {IBP_RAW_PATH}")
print(f"📊 Target: Unity Catalog Tables (supply_chain_catalog.bronze.*)")
print("=" * 80)
print("\n⚠️  If files are not found, update the paths using the widgets above")
print("=" * 80)

# Tables configuration - SAP Tables
sap_tables = [
    "AUSP", "AUSP_BATCH", "EKBE", "EKET", "EKKO", "EKPO",
    "LFA1", "LFB1", "LFM1", "MAKT", "MARA", "MARC", "MARCH",
    "MARD", "MARDH", "MBEW", "MBEWH", "MCH1", "MCHB", "MCHBH",
    "MSLB", "MSLBH", "PLKO", "QALS", "QAVE",
    "T001", "T001K", "T001L", "T001W", "T023T", "T024", "T024E",
    "T077K", "T134T", "TCURF", "TQ30T", "TQ31T"
]

# IBP Tables configuration
ibp_tables = [
    "demand_actual", "demand_forcast", "master_customer",
    "master_customer_product", "master_location", "master_location_product"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic Ingestion Function (Metadata-Driven)

# COMMAND ----------

def clean_column_name(col_name):
    """
    Sanitize column names to be Delta Lake compatible
    Removes/replaces: spaces, slashes, parentheses, commas, semicolons, etc.
    """
    # Replace invalid characters with underscores
    clean_name = re.sub(r'[\s,;{}()/\n\t=]', '_', col_name)
    # Remove duplicate underscores
    clean_name = re.sub(r'_{2,}', '_', clean_name)
    # Remove leading/trailing underscores
    clean_name = clean_name.strip('_')
    return clean_name

def ingest_to_bronze(source_path, table_name, source_system):
    """
    Generic function to ingest CSV files to Bronze layer using Unity Catalog
    
    Parameters:
    - source_path: Path to source CSV file
    - table_name: Name of the table
    - source_system: Source system identifier (SAP or IBP)
    """
    
    try:
        # Construct full table name for Unity Catalog
        bronze_table = f"supply_chain_catalog.bronze.{source_system.lower()}_{table_name.lower()}"
        
        # Read CSV file
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ",") \
            .load(f"{source_path}{table_name}.csv")
        
        # Clean column names to be Delta Lake compatible
        for col_name in df.columns:
            clean_name = clean_column_name(col_name)
            if clean_name != col_name:
                df = df.withColumnRenamed(col_name, clean_name)
        
        # Add metadata columns
        df_with_metadata = df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_file", lit(f"{table_name}.csv")) \
            .withColumn("_source_system", lit(source_system)) \
            .withColumn("_bronze_load_date", lit(current_date()))
        
        # Write to Bronze layer as Unity Catalog managed table
        df_with_metadata.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(bronze_table)
        
        # Count records
        record_count = df_with_metadata.count()
        
        # Log success
        print(f"✅ Successfully ingested {table_name}: {record_count} records → {bronze_table}")
        
        return True, record_count
        
    except Exception as e:
        print(f"❌ Error ingesting {table_name}: {str(e)}")
        return False, 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest SAP Raw Data

# COMMAND ----------

# Create ingestion log
ingestion_log = []

print("=" * 80)
print("STARTING SAP RAW DATA INGESTION")
print("=" * 80)

for table in sap_tables:
    status, count = ingest_to_bronze(
        source_path=SAP_RAW_PATH,
        table_name=table,
        source_system="SAP"
    )
    
    ingestion_log.append({
        "table_name": table,
        "source_system": "SAP",
        "status": "SUCCESS" if status else "FAILED",
        "record_count": count,
        "timestamp": datetime.now()
    })

print("\n" + "=" * 80)
print("SAP DATA INGESTION COMPLETED")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest SAP IBP Data

# COMMAND ----------

print("\n" + "=" * 80)
print("STARTING SAP IBP DATA INGESTION")
print("=" * 80)

for table in ibp_tables:
    status, count = ingest_to_bronze(
        source_path=IBP_RAW_PATH,
        table_name=table,
        source_system="IBP"
    )
    
    ingestion_log.append({
        "table_name": table,
        "source_system": "IBP",
        "status": "SUCCESS" if status else "FAILED",
        "record_count": count,
        "timestamp": datetime.now()
    })

print("\n" + "=" * 80)
print("IBP DATA INGESTION COMPLETED")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Ingestion Summary Report

# COMMAND ----------

from pyspark.sql.functions import count, sum

# Convert log to DataFrame
log_df = spark.createDataFrame(ingestion_log)

# Display summary
print("\n" + "=" * 80)
print("BRONZE LAYER INGESTION SUMMARY")
print("=" * 80)

display(
    log_df.groupBy("source_system", "status")
    .agg(
        count("*").alias("table_count"),
        sum("record_count").alias("total_records")
    )
)

# Show detailed log
display(log_df.orderBy("source_system", "table_name"))

# Save ingestion log to Unity Catalog
log_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain_catalog.bronze.ingestion_log")

print("\n✅ BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

def check_bronze_table(bronze_table_name, display_name):
    """Basic data quality check for Bronze tables"""
    
    try:
        # Read from Unity Catalog table
        full_table_name = f"supply_chain_catalog.bronze.{bronze_table_name}"
        df = spark.table(full_table_name)
        
        print(f"\n📊 Table: {display_name}")
        print(f"   Total Records: {df.count():,}")
        print(f"   Total Columns: {len(df.columns)}")
        print(f"   Schema:")
        df.printSchema()
        
        # Check for null counts
        null_counts = df.select([
            count(when(col(c).isNull(), c)).alias(c) 
            for c in df.columns if not c.startswith("_")
        ])
        
        print(f"\n   Sample Data:")
        df.select([c for c in df.columns if not c.startswith("_")]).show(5, truncate=False)
        
    except Exception as e:
        print(f"❌ Error checking {display_name}: {str(e)}")

# COMMAND ----------

# Sample checks on key tables
print("=" * 80)
print("BRONZE LAYER DATA QUALITY CHECKS")
print("=" * 80)

# Check key SAP tables
for table in ["MARA", "EKKO", "EKPO"]:
    check_bronze_table(f"sap_{table.lower()}", table)

# Check IBP tables
for table in ["demand_actual", "demand_forcast"]:
    check_bronze_table(f"ibp_{table}", table)

print("\n✅ BRONZE LAYER QUALITY CHECKS COMPLETED!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC ✅ Bronze Layer Completed!
# MAGIC
# MAGIC **Next**: Proceed to Silver Layer (02_silver_layer_transformation.py)
# MAGIC - Data cleansing
# MAGIC - Standardization
# MAGIC - Type conversions
# MAGIC - Deduplication
