# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog for Supply Chain Data Model
# MAGIC
# MAGIC This notebook creates the necessary catalog and schemas for the 3-layer medallion architecture.
# MAGIC
# MAGIC **Run this notebook FIRST before running any other notebooks.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schemas

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql("""
CREATE CATALOG IF NOT EXISTS supply_chain_catalog
COMMENT 'Supply Chain Data Model - Competition Project'
""")

print("✅ Created catalog: supply_chain_catalog")

# COMMAND ----------

# Set default catalog
spark.sql("USE CATALOG supply_chain_catalog")

# COMMAND ----------

# Create Bronze schema (raw data layer)
spark.sql("""
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Bronze Layer - Raw ingested data from SAP and IBP systems'
""")

print("✅ Created schema: bronze")

# COMMAND ----------

# Create Silver schema (cleaned data layer)
spark.sql("""
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver Layer - Cleaned and standardized data'
""")

print("✅ Created schema: silver")

# COMMAND ----------

# Create Gold schema (business layer)
spark.sql("""
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold Layer - Business-ready star schema with dimensions and facts'
""")

print("✅ Created schema: gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# Show all schemas
print("=" * 80)
print("CATALOG SETUP COMPLETE")
print("=" * 80)
print("\nCatalog: supply_chain_catalog")
print("\nSchemas:")
print("  - bronze (raw data)")
print("  - silver (cleaned data)")
print("  - gold (star schema)")
print("\n" + "=" * 80)

spark.sql("SHOW SCHEMAS IN supply_chain_catalog").show()

# COMMAND ----------

# Set default schema for convenience
spark.sql("USE supply_chain_catalog.bronze")
print("\n✅ Default schema set to: supply_chain_catalog.bronze")
print("\n🎯 Ready to run Bronze Layer Ingestion!")
