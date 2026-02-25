# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LAYER - Data Cleansing & Transformation
# MAGIC ## Supply Chain Data Model
# MAGIC
# MAGIC **Purpose**: Clean, standardize, and prepare data for Gold layer using Unity Catalog
# MAGIC
# MAGIC **Transformations**:
# MAGIC - Data type standardization
# MAGIC - Null handling
# MAGIC - Date formatting
# MAGIC - String trimming & case standardization
# MAGIC - Deduplication
# MAGIC - Data quality validations
# MAGIC
# MAGIC **Prerequisites**: Run `01_bronze_layer_ingestion` notebook first

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from datetime import datetime

# Set Unity Catalog context
spark.sql("USE CATALOG supply_chain_catalog")

print("=" * 80)
print("SILVER LAYER TRANSFORMATION")
print("=" * 80)
print("📊 Source: supply_chain_catalog.bronze.*")
print("📊 Target: supply_chain_catalog.silver.*")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic Silver Transformation Framework

# COMMAND ----------

def transform_to_silver(bronze_table_name, silver_table_name, dedupe_columns=None, date_columns=None):
    """
    Generic transformation function for Silver layer with Unity Catalog
    
    Parameters:
    - bronze_table_name: Name of Bronze table (e.g., 'sap_ausp')
    - silver_table_name: Name for Silver table (e.g., 'sap_ausp')
    - dedupe_columns: List of columns to use for deduplication
    - date_columns: List of date columns to standardize
    """
    
    try:
        # Construct full table names
        bronze_table = f"supply_chain_catalog.bronze.{bronze_table_name}"
        silver_table = f"supply_chain_catalog.silver.{silver_table_name}"
        
        # Read from Bronze Unity Catalog table
        df = spark.table(bronze_table)
        
        #Remove Bronze metadata columns
        data_columns = [c for c in df.columns if not c.startswith("_")]
        df_clean = df.select(data_columns)
        
        # Trim all string columns and handle nulls
        for col_name, col_type in df_clean.dtypes:
            if col_type == 'string':
                df_clean = df_clean.withColumn(col_name, trim(col(col_name)))
                df_clean = df_clean.withColumn(
                    col_name, 
                    when(col(col_name) == "", None).otherwise(col(col_name))
                )
        
        # Standardize date columns if provided (with error handling)
        if date_columns:
            for date_col in date_columns:
                if date_col in df_clean.columns:
                    # Use when/otherwise to handle invalid dates gracefully
                    df_clean = df_clean.withColumn(
                        date_col,
                        when(
                            (col(date_col).isNotNull()) & 
                            (length(col(date_col)) == 8) &
                            (col(date_col).rlike("^[0-9]{8}$")),
                            to_date(col(date_col), "yyyyMMdd")
                        ).otherwise(None)
                    )
        
        # Deduplication if dedupe columns provided
        if dedupe_columns:
            # Keep only the latest record based on all columns
            window_spec = Window.partitionBy(dedupe_columns).orderBy(col(data_columns[0]).desc())
            df_clean = df_clean.withColumn("row_num", row_number().over(window_spec))
            df_clean = df_clean.filter(col("row_num") == 1).drop("row_num")
        
        # Add Silver metadata
        df_silver = df_clean \
            .withColumn("_silver_load_timestamp", current_timestamp()) \
            .withColumn("_silver_load_date", current_date()) \
            .withColumn("_is_current", lit(True))
        
        # Write to Silver Unity Catalog table
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(silver_table)
        
        record_count = df_silver.count()
        print(f"✅ {silver_table_name}: {record_count} records transformed → {silver_table}")
        
        return True, record_count
        
    except Exception as e:
        print(f"❌ Error transforming {bronze_table_name}: {str(e)}")
        return False, 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Transformation - All Tables

# COMMAND ----------

# Define all table transformations in a configuration dictionary
transformations_config = [
    # SAP Master Data
    {"bronze": "sap_mara", "silver": "mara", "dedupe": ["MATNR"], "dates": ["ERSDA", "LAEDA"]},
    {"bronze": "sap_makt", "silver": "makt", "dedupe": ["MATNR", "SPRAS"]},
    {"bronze": "sap_marc", "silver": "marc", "dedupe": ["MATNR", "WERKS"]},
    {"bronze": "sap_mard", "silver": "mard", "dedupe": ["MATNR", "WERKS", "LGORT"]},
    {"bronze": "sap_mbew", "silver": "mbew", "dedupe": ["MATNR", "BWKEY"]},
    {"bronze": "sap_ausp", "silver": "ausp", "dedupe": ["OBJEK", "ATINN"]},
    {"bronze": "sap_ausp_batch", "silver": "ausp_batch", "dedupe": ["OBJEK", "ATNAM"]},
    
    # SAP Inventory
    {"bronze": "sap_mchb", "silver": "mchb", "dedupe": ["MATNR", "WERKS", "LGORT", "CHARG"]},
    {"bronze": "sap_mch1", "silver": "mch1", "dedupe": ["MATNR", "CHARG"], "dates": ["ERSDA", "HSDAT", "VFDAT"]},
    {"bronze": "sap_mslb", "silver": "mslb", "dedupe": ["MATNR", "CHARG", "SOBKZ"]},
    {"bronze": "sap_march", "silver": "march"},
    {"bronze": "sap_mardh", "silver": "mardh"},
    {"bronze": "sap_mbewh", "silver": "mbewh"},
    {"bronze": "sap_mchbh", "silver": "mchbh"},
    {"bronze": "sap_mslbh", "silver": "mslbh"},
    
    # SAP Purchase Orders
    {"bronze": "sap_ekko", "silver": "ekko", "dedupe": ["EBELN"], "dates": ["AEDAT", "BEDAT"]},
    {"bronze": "sap_ekpo", "silver": "ekpo", "dedupe": ["EBELN", "EBELP"]},
    {"bronze": "sap_eket", "silver": "eket", "dedupe": ["EBELN", "EBELP", "ETENR"], "dates": ["EINDT", "SLFDT", "LPEIN"]},
    {"bronze": "sap_ekbe", "silver": "ekbe", "dedupe": ["EBELN", "EBELP", "ZEKKN", "VGABE", "GJAHR", "BELNR", "BUZEI"], "dates": ["BUDAT", "CPUDT"]},
    
    # SAP Supplier
    {"bronze": "sap_lfa1", "silver": "lfa1", "dedupe": ["LIFNR"], "dates": ["ERDAT"]},
    {"bronze": "sap_lfb1", "silver": "lfb1", "dedupe": ["LIFNR", "BUKRS"]},
    {"bronze": "sap_lfm1", "silver": "lfm1", "dedupe": ["LIFNR", "EKORG"]},
    
    # SAP Quality
    {"bronze": "sap_qals", "silver": "qals", "dates": ["PASTRTERM", "ERSTELLDAT"]},
    {"bronze": "sap_qave", "silver": "qave"},
    {"bronze": "sap_plko", "silver": "plko"},
    
    # SAP Reference Tables
    {"bronze": "sap_t001", "silver": "t001", "dedupe": ["BUKRS"]},
    {"bronze": "sap_t001k", "silver": "t001k", "dedupe": ["BWKEY"]},
    {"bronze": "sap_t001l", "silver": "t001l", "dedupe": ["WERKS", "LGORT"]},
    {"bronze": "sap_t001w", "silver": "t001w", "dedupe": ["WERKS"]},
    {"bronze": "sap_t023t", "silver": "t023t", "dedupe": ["MATKL", "SPRAS"]},
    {"bronze": "sap_t024", "silver": "t024", "dedupe": ["EKGRP"]},
    {"bronze": "sap_t024e", "silver": "t024e", "dedupe": ["EKORG"]},
    {"bronze": "sap_t077k", "silver": "t077k", "dedupe": ["KTOKK"]},
    {"bronze": "sap_t134t", "silver": "t134t", "dedupe": ["MTART", "SPRAS"]},
    {"bronze": "sap_tcurf", "silver": "tcurf", "dates": ["GDATU"]},
    {"bronze": "sap_tq30t", "silver": "tq30t"},
    {"bronze": "sap_tq31t", "silver": "tq31t"},
    
    # SAP IBP
    {"bronze": "ibp_demand_actual", "silver": "demand_actual", "dedupe": ["period", "material", "market"]},
    {"bronze": "ibp_demand_forcast", "silver": "demand_forecast", "dedupe": ["period", "material", "market"]},
    {"bronze": "ibp_master_customer", "silver": "master_customer", "dedupe": ["Customer_ID"]},
    {"bronze": "ibp_master_customer_product", "silver": "master_customer_product", "dedupe": ["Customer_ID", "Product_ID"]},
    {"bronze": "ibp_master_location", "silver": "master_location", "dedupe": ["Location_ID"]},
    {"bronze": "ibp_master_location_product", "silver": "master_location_product", "dedupe": ["Location_ID", "Product_ID"]},
]

# COMMAND ----------

print("=" * 80)
print("STARTING SILVER LAYER TRANSFORMATION")
print("=" * 80)
print(f"Total tables to transform: {len(transformations_config)}\n")

success_count = 0
failure_count = 0

for config in transformations_config:
    status, count = transform_to_silver(
        bronze_table_name=config["bronze"],
        silver_table_name=config["silver"],
        dedupe_columns=config.get("dedupe"),
        date_columns=config.get("dates")
    )
    
    if status:
        success_count += 1
    else:
        failure_count += 1

print("\n" + "=" * 80)
print("SILVER LAYER TRANSFORMATION SUMMARY")
print("=" * 80)
print(f"✅ Successful: {success_count}")
print(f"❌ Failed: {failure_count}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Validation

# COMMAND ----------

def validate_silver_table(table_name):
    """Validate Silver layer table"""
    
    full_table_name = f"supply_chain_catalog.silver.{table_name}"
    df = spark.table(full_table_name)
    
    print(f"\n📊 Silver Table: {full_table_name}")
    print(f"   Records: {df.count():,}")
    print(f"   Columns: {len(df.columns)}")
    
    # Check for nulls in key columns
    key_cols = [c for c in df.columns if not c.startswith("_")][:5]
    if key_cols:
        null_counts = df.select([
            sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls")
            for c in key_cols
        ]).collect()[0].asDict()
        
        print(f"   Null Counts (first 5 cols): {null_counts}")
    
    return df

# COMMAND ----------

print("\n" + "=" * 80)
print("SILVER LAYER VALIDATION")
print("=" * 80)

# Validate key tables
key_tables = ["mara", "ekko", "ekpo", "demand_actual", "demand_forecast"]

for table in key_tables:
    try:
        validate_silver_table(table)
    except Exception as e:
        print(f"❌ Error validating {table}: {str(e)}")

print("\n✅ SILVER LAYER TRANSFORMATION AND VALIDATION COMPLETED!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC ✅ Silver Layer Completed!
# MAGIC
# MAGIC **Next**: Proceed to Gold Layer
# MAGIC - 03_gold_layer_dimensions.py - Build dimension tables
# MAGIC - 04_gold_layer_facts.py - Build fact tables
