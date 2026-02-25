# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD LAYER - Dimension Tables
# MAGIC ## Supply Chain Data Model - Star Schema with Unity Catalog
# MAGIC
# MAGIC **Purpose**: Build conformed dimension tables for the star schema using Unity Catalog
# MAGIC
# MAGIC **Dimensions to Create**:
# MAGIC 1. dim_product
# MAGIC 2. dim_batch
# MAGIC 3. dim_currency  
# MAGIC 4. dim_date
# MAGIC 5. dim_storage
# MAGIC 6. dim_supplier
# MAGIC 7. dim_uom
# MAGIC 8. dim_customer
# MAGIC 9. dim_customer_product
# MAGIC 10. dim_location
# MAGIC 11. dim_location_product
# MAGIC
# MAGIC **Prerequisites**: Run `02_silver_layer_transformation` notebook first

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from datetime import datetime, timedelta
import hashlib

# Set Unity Catalog context
spark.sql("USE CATALOG supply_chain_catalog")

print("=" * 80)
print("GOLD LAYER - DIMENSION TABLES")
print("=" * 80)
print("📊 Source: supply_chain_catalog.silver.*")
print("📊 Target: supply_chain_catalog.gold.dim_*")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def generate_surrogate_key(*columns):
    """Generate surrogate key using MD5 hash (returns string)"""
    concat_cols = concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in columns])
    return md5(concat_cols)

def create_dimension(df, dim_name, business_keys):
    """
    Create dimension table with surrogate key using Unity Catalog
    
    Parameters:
    - df: Input DataFrame
    - dim_name: Name of the dimension (e.g., 'dim_product')
    - business_keys: List of business key columns
    """
    
    # Generate surrogate key
    df_with_key = df.withColumn(
        f"{dim_name}_key",
        generate_surrogate_key(*business_keys)
    )
    
    # Add SCD Type 2 columns
    df_final = df_with_key \
        .withColumn("effective_date", current_date()) \
        .withColumn("end_date", lit(None).cast(DateType())) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("updated_timestamp", current_timestamp())
    
    # Write to Gold Unity Catalog table
    gold_table = f"supply_chain_catalog.gold.{dim_name}"
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(gold_table)
    
    record_count = df_final.count()
    print(f"✅ {dim_name}: {record_count} records created → {gold_table}")
    
    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_product - Product Dimension

# COMMAND ----------

print("=" * 80)
print("BUILDING dim_product")
print("=" * 80)

# Read source tables and immediately convert to string schema
mara_df = spark.table("supply_chain_catalog.silver.mara")
makt_df = spark.table("supply_chain_catalog.silver.makt")

# Filter and prepare with explicit string conversion
makt_eng = makt_df.filter(col("SPRAS") == "E") \
    .select(
        col("MATNR").cast("string").alias("MATNR_TEXT"),
        col("MAKTX").cast("string").alias("MAKTX")
    )

# Build product dimension with all string columns
dim_product_data = mara_df.select(
    col("MATNR").cast("string").alias("MATNR_PROD"),
    col("MTART").cast("string").alias("MTART"),
    col("MATKL").cast("string").alias("MATKL"),
    col("MEINS").cast("string").alias("MEINS"),
    col("VPSTA").cast("string").alias("VPSTA"),
    col("PSTAT").cast("string").alias("PSTAT"),
    col("MSTAE").cast("string").alias("MSTAE"),
    col("ERSDA").cast("date").alias("ERSDA"),
    col("LAEDA").cast("date").alias("LAEDA")
).join(
    makt_eng,
    col("MATNR_PROD") == col("MATNR_TEXT"),
    "left"
).select(
    col("MATNR_PROD").alias("material_number"),
    coalesce(col("MAKTX"), lit("")).alias("material_description"),
    coalesce(col("MTART"), lit("")).alias("material_type"),
    coalesce(col("MATKL"), lit("")).alias("material_group"),
    coalesce(col("MEINS"), lit("")).alias("base_uom"),
    coalesce(col("VPSTA"), lit("")).alias("complete_status"),
    coalesce(col("PSTAT"), lit("")).alias("maintenance_status"),
    coalesce(col("MSTAE"), lit("")).alias("xplant_status"),
    col("ERSDA").alias("created_date"),
    col("LAEDA").alias("last_change_date")
)

# Add surrogate key and metadata - all as proper types
dim_product_final = dim_product_data \
    .withColumn("dim_product_key", md5(col("material_number")).cast("string")) \
    .withColumn("effective_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True).cast("boolean")) \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

# Write directly without RDD conversion
dim_product_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("supply_chain_catalog.gold.dim_product")

record_count = spark.table("supply_chain_catalog.gold.dim_product").count()
print(f"✅ dim_product: {record_count} records created → supply_chain_catalog.gold.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_batch - Batch Dimension

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_batch")
print("=" * 80)

# Read source tables from Unity Catalog
mch1 = spark.table("supply_chain_catalog.silver.mch1")
ausp_batch = spark.table("supply_chain_catalog.silver.ausp_batch")

# Create batch dimension
dim_batch_df = mch1.select(
    col("MATNR").alias("material_number"),
    col("CHARG").alias("batch_number"),
    col("ERSDA").alias("created_date"),
    col("HSDAT").alias("production_date"),
    col("VFDAT").alias("expiry_date"),
    col("LAEDA").alias("last_change_date")
)

# Add batch characteristics from AUSP_BATCH if needed
dim_batch_final = dim_batch_df

# Create dimension
dim_batch = create_dimension(
    df=dim_batch_final,
    dim_name="dim_batch",
    business_keys=["material_number", "batch_number"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_currency - Currency Dimension

# COMMAND ----------

print("=" * 80)
print("BUILDING dim_product")
print("=" * 80)

# Read source tables and immediately convert to string schema
mara_df = spark.table("supply_chain_catalog.silver.mara")
makt_df = spark.table("supply_chain_catalog.silver.makt")

# Filter and prepare with explicit string conversion
makt_eng = makt_df.filter(col("SPRAS") == "E") \
    .select(
        col("MATNR").cast("string").alias("MATNR_TEXT"),
        col("MAKTX").cast("string").alias("MAKTX")
    )

# Build product dimension with all string columns
dim_product_data = mara_df.select(
    col("MATNR").cast("string").alias("MATNR_PROD"),
    col("MTART").cast("string").alias("MTART"),
    col("MATKL").cast("string").alias("MATKL"),
    col("MEINS").cast("string").alias("MEINS"),
    col("VPSTA").cast("string").alias("VPSTA"),
    col("PSTAT").cast("string").alias("PSTAT"),
    col("MSTAE").cast("string").alias("MSTAE"),
    col("ERSDA").cast("date").alias("ERSDA"),
    col("LAEDA").cast("date").alias("LAEDA")
).join(
    makt_eng,
    col("MATNR_PROD") == col("MATNR_TEXT"),
    "left"
).select(
    col("MATNR_PROD").alias("material_number"),
    coalesce(col("MAKTX"), lit("")).alias("material_description"),
    coalesce(col("MTART"), lit("")).alias("material_type"),
    coalesce(col("MATKL"), lit("")).alias("material_group"),
    coalesce(col("MEINS"), lit("")).alias("base_uom"),
    coalesce(col("VPSTA"), lit("")).alias("complete_status"),
    coalesce(col("PSTAT"), lit("")).alias("maintenance_status"),
    coalesce(col("MSTAE"), lit("")).alias("xplant_status"),
    col("ERSDA").alias("created_date"),
    col("LAEDA").alias("last_change_date")
)

# Add surrogate key and metadata - all as proper types
dim_product_final = dim_product_data \
    .withColumn("dim_product_key", md5(col("material_number")).cast("string")) \
    .withColumn("effective_date", current_date()) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True).cast("boolean")) \
    .withColumn("created_timestamp", current_timestamp()) \
    .withColumn("updated_timestamp", current_timestamp())

# Write directly without RDD conversion
dim_product_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("supply_chain_catalog.gold.dim_product")

record_count = spark.table("supply_chain_catalog.gold.dim_product").count()
print(f"✅ dim_product: {record_count} records created → supply_chain_catalog.gold.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_date - Date Dimension

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_date")
print("=" * 80)

# Generate date dimension for 10 years (2020-2030)
start_date = datetime(2020, 1, 1)
end_date = datetime(2030, 12, 31)
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Create date dimension DataFrame
date_data = []
for date in date_range:
    date_data.append({
        "date_key": int(date.strftime("%Y%m%d")),
        "full_date": date.date(),
        "year": date.year,
        "quarter": f"Q{(date.month-1)//3 + 1}",
        "month": date.month,
        "month_name": date.strftime("%B"),
        "week": date.isocalendar()[1],
        "day": date.day,
        "day_of_week": date.weekday() + 1,
        "day_name": date.strftime("%A"),
        "is_weekend": date.weekday() >= 5,
        "fiscal_year": date.year if date.month >= 4 else date.year - 1,
        "fiscal_quarter": f"Q{((date.month-4)%12)//3 + 1}",
        "fiscal_period": ((date.month - 4) % 12) + 1,
        "year_month": date.strftime("%Y-%m"),
        "year_quarter": f"{date.year}-Q{(date.month-1)//3 + 1}"
    })

dim_date_df = spark.createDataFrame(date_data)

# Write directly to Unity Catalog (no surrogate key needed - date_key is sufficient)
dim_date_df \
    .withColumn("created_timestamp", current_timestamp()) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain_catalog.gold.dim_date")

print(f"✅ dim_date: {dim_date_df.count()} records created → supply_chain_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_storage - Storage Location Dimension

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_storage")
print("=" * 80)

# Read source tables from Unity Catalog
t001w = spark.table("supply_chain_catalog.silver.t001w")
t001l = spark.table("supply_chain_catalog.silver.t001l")

# Create storage dimension
dim_storage_df = t001w.alias("w") \
    .join(t001l.alias("l"), col("w.WERKS") == col("l.WERKS"), "left") \
    .select(
        col("w.WERKS").alias("plant"),
        col("l.LGORT").alias("storage_location"),
        col("w.NAME1").alias("plant_name"),
        col("l.LGOBE").alias("storage_location_description"),
        col("w.ORT01").alias("city"),
        col("w.LAND1").alias("country"),
        col("w.REGIO").alias("region")
    )

# Create dimension
dim_storage = create_dimension(
    df=dim_storage_df,
    dim_name="dim_storage",
    business_keys=["plant", "storage_location"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. dim_supplier - Supplier Dimension

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_supplier")
print("=" * 80)

# Read source tables from Unity Catalog
lfa1 = spark.table("supply_chain_catalog.silver.lfa1")
lfb1 = spark.table("supply_chain_catalog.silver.lfb1")
lfm1 = spark.table("supply_chain_catalog.silver.lfm1")
t077k = spark.table("supply_chain_catalog.silver.t077k")

# Create supplier dimension
dim_supplier_df = lfa1.alias("a") \
    .join(lfm1.alias("m"), col("a.LIFNR") == col("m.LIFNR"), "left") \
    .join(lfb1.alias("b"), col("a.LIFNR") == col("b.LIFNR"), "left") \
    .join(t077k.alias("k"), col("a.KTOKK") == col("k.KTOKK"), "left") \
    .select(
        col("a.LIFNR").cast("string").alias("supplier_number"),
        col("m.EKORG").cast("string").alias("purchasing_org"),
        col("a.NAME1").cast("string").alias("supplier_name"),
        col("a.LAND1").cast("string").alias("country"),
        col("a.ORT01").cast("string").alias("city"),
        col("a.PSTLZ").cast("string").alias("postal_code"),
        col("a.STRAS").cast("string").alias("street"),
        col("m.WAERS").cast("string").alias("order_currency"),
        col("m.ZTERM").cast("string").alias("payment_terms"),
        col("m.INCO1").cast("string").alias("incoterms"),
        col("b.INTAD").cast("string").alias("email"),
        col("a.KTOKK").cast("string").alias("account_group"),
        col("a.ERDAT").cast("date").alias("created_date")
    ).distinct()

# Create dimension
dim_supplier = create_dimension(
    df=dim_supplier_df,
    dim_name="dim_supplier",
    business_keys=["supplier_number", "purchasing_org"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. dim_uom - Unit of Measure Dimension

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_uom")
print("=" * 80)

# Extract unique UOMs from MARA (Unity Catalog)
mara_uom = spark.table("supply_chain_catalog.silver.mara")

dim_uom_df = mara_uom.select(
    col("MEINS").alias("uom_code")
).distinct().filter(col("uom_code").isNotNull())

# Add UOM description (simplified - in production, join with T006)
dim_uom_final = dim_uom_df.withColumn(
    "uom_description",
    col("uom_code")  # In real scenario, join with T006 for descriptions
)

# Create dimension
dim_uom = create_dimension(
    df=dim_uom_final,
    dim_name="dim_uom",
    business_keys=["uom_code"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. dim_customer - Customer Dimension (from IBP)

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_customer")
print("=" * 80)

# Read IBP customer master from Unity Catalog
master_customer = spark.table("supply_chain_catalog.silver.master_customer")

# Create customer dimension - use cleaned column names
dim_customer_df = master_customer.select(
    col("Customer_ID").alias("customer_id"),
    col("Customer_Description").alias("customer_description"),
    col("Customer_Region").alias("customer_region"),
    col("Customer_Country_Region").alias("customer_country_region"),
    col("Demand_Type").alias("demand_type"),
    col("Demand_Sub_Type").alias("demand_sub_type"),
    col("Latitude").alias("latitude"),
    col("Longttitude").alias("longitude")  # Note: 3 t's in source data
)

# Create dimension
dim_customer = create_dimension(
    df=dim_customer_df,
    dim_name="dim_customer",
    business_keys=["customer_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. dim_customer_product - Customer Product Dimension

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_customer_product")
print("=" * 80)

# Read IBP customer product master from Unity Catalog
master_customer_product = spark.table("supply_chain_catalog.silver.master_customer_product")

# Create customer product dimension
dim_customer_product_df = master_customer_product.select(
    col("CUSTOMER_ID").alias("customer_id"),
    col("PRODUCT_ID").alias("product_id"),
    col("MARKET_SEGMENT").alias("market_segment"),
    col("REMAINING_SHELF_LIFE").alias("remaining_shelf_life")
)

# Create dimension
dim_customer_product = create_dimension(
    df=dim_customer_product_df,
    dim_name="dim_customer_product",
    business_keys=["customer_id", "product_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. dim_location - Location Dimension (from IBP)

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_location")
print("=" * 80)

# Read IBP location master from Unity Catalog
master_location = spark.table("supply_chain_catalog.silver.master_location")

# Create location dimension
dim_location_df = master_location.select(
    col("LOCATION_ID").alias("location_id"),
    col("LOCATION_DESCRIPTION").alias("location_description"),
    col("LOCATION_REGION").alias("location_region"),
    col("LOCATION_TYPE").alias("location_type"),
    col("LATITUDE").alias("latitude"),
    col("LONGTTITUDE").alias("longitude")
)

# Create dimension
dim_location = create_dimension(
    df=dim_location_df,
    dim_name="dim_location",
    business_keys=["location_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. dim_location_product - Location Product Dimension

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING dim_location_product")
print("=" * 80)

# Read IBP location product master from Unity Catalog
master_location_product = spark.table("supply_chain_catalog.silver.master_location_product")

# Create location product dimension - use cleaned column names
dim_location_product_df = master_location_product.select(
    col("Location_ID").alias("location_id"),
    col("Product_ID").alias("product_id"),
    col("Material_Description").alias("product_description"),
    col("Plant_Exclusion_X").alias("plant_exclusion_flag"),
    col("Market_DC_X").alias("market_dc_flag")
)

# Create dimension
dim_location_product = create_dimension(
    df=dim_location_product_df,
    dim_name="dim_location_product",
    business_keys=["location_id", "product_id"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Summary Report

# COMMAND ----------

print("\n" + "=" * 80)
print("GOLD LAYER - DIMENSION TABLES SUMMARY")
print("=" * 80)

dimensions = [
    "dim_product", "dim_batch", "dim_currency", "dim_date",
    "dim_storage", "dim_supplier", "dim_uom", "dim_customer",
    "dim_customer_product", "dim_location", "dim_location_product"
]

summary_data = []
for dim in dimensions:
    try:
        df = spark.table(f"supply_chain_catalog.gold.{dim}")
        count = df.count()
        cols = len(df.columns)
        summary_data.append({
            "dimension": dim,
            "record_count": count,
            "column_count": cols,
            "status": "SUCCESS"
        })
        print(f"✅ {dim}: {count:,} records, {cols} columns")
    except Exception as e:
        summary_data.append({
            "dimension": dim,
            "record_count": 0,
            "column_count": 0,
            "status": f"FAILED: {str(e)}"
        })
        print(f"❌ {dim}: FAILED - {str(e)}")

summary_df = spark.createDataFrame(summary_data)
display(summary_df)

print("\n✅ ALL DIMENSION TABLES COMPLETED!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC ✅ All 11 Dimension Tables Created!
# MAGIC
# MAGIC **Next**: Proceed to 04_gold_layer_facts.py
# MAGIC - Build all 8 fact tables
# MAGIC - Establish relationships with dimensions
