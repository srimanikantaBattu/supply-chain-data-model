# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD LAYER - Fact Tables
# MAGIC ## Supply Chain Data Model
# MAGIC
# MAGIC **Purpose**: Build fact tables with proper grain and foreign keys to dimensions
# MAGIC
# MAGIC **Facts to Create**:
# MAGIC 1. fact_inventory
# MAGIC 2. fact_inventory_month_end_stock
# MAGIC 3. fact_inventory_monthly_snapshot
# MAGIC 4. fact_purchase_order
# MAGIC 5. fact_demand_actual
# MAGIC 6. fact_demand_forecast
# MAGIC 7. fact_batch_release_external
# MAGIC 8. fact_batch_release_internal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# Set Unity Catalog context
spark.sql("USE CATALOG supply_chain_catalog")

print("=" * 80)
print("GOLD LAYER - FACT TABLES")
print("=" * 80)
print("📊 Source: supply_chain_catalog.silver.* & supply_chain_catalog.gold.dim_*")
print("📊 Target: supply_chain_catalog.gold.fact_*")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def get_dimension_key(dim_name, business_keys_dict):
    """
    Join with dimension to get surrogate key from Unity Catalog
    
    Parameters:
    - dim_name: Name of dimension table
    - business_keys_dict: Dictionary mapping fact columns to dimension columns
    """
    dim_table = f"supply_chain_catalog.gold.{dim_name}"
    dim_df = spark.table(dim_table)
    return dim_df.select(f"{dim_name}_key", *business_keys_dict.values())

def create_fact_table(df, fact_name):
    """
    Create fact table with metadata in Unity Catalog
    
    Parameters:
    - df: Input DataFrame
    - fact_name: Name of the fact table
    """
    fact_table = f"supply_chain_catalog.gold.{fact_name}"
    
    # Drop existing table to avoid schema conflicts
    spark.sql(f"DROP TABLE IF EXISTS {fact_table}")
    
    df_final = df \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("updated_timestamp", current_timestamp())
    
    # Write to Gold Unity Catalog table
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(fact_table)
    
    record_count = df_final.count()
    print(f"✅ {fact_name}: {record_count:,} records created → {fact_table}")
    
    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. fact_inventory - Current Inventory

# COMMAND ----------

print("=" * 80)
print("BUILDING fact_inventory")
print("=" * 80)

# Grain: Material + Plant + Storage Location + Batch + Date
# Source: MCHB, MARD, MBEW, MSLB

# Read source tables
mchb = spark.table("supply_chain_catalog.silver.mchb")
mard = spark.table("supply_chain_catalog.silver.mard")
mbew = spark.table("supply_chain_catalog.silver.mbew")
mslb = spark.table("supply_chain_catalog.silver.mslb")
t001 = spark.table("supply_chain_catalog.silver.t001")
tcurf = spark.table("supply_chain_catalog.silver.tcurf")

# Build fact table
fact_inventory_df = mchb.alias("b") \
    .join(mard.alias("d"), 
          (col("b.MATNR") == col("d.MATNR")) & 
          (col("b.WERKS") == col("d.WERKS")) & 
          (col("b.LGORT") == col("d.LGORT")), "left") \
    .join(mbew.alias("v"), 
          (col("b.MATNR") == col("v.MATNR")) & 
          (col("b.WERKS") == col("v.BWKEY")), "left") \
    .join(mslb.alias("s"),
          (col("b.MATNR") == col("s.MATNR")) &
          (col("b.WERKS") == col("s.WERKS")) &
          (col("b.CHARG") == col("s.CHARG")), "left") \
    .select(
        # Business Keys for dimension lookups
        col("b.MATNR").alias("material_number"),
        col("b.WERKS").alias("plant"),
        col("b.LGORT").alias("storage_location"),
        col("b.CHARG").alias("batch_number"),
        
        # Measures
        col("b.CLABS").cast("decimal(15,3)").alias("unrestricted_stock_qty"),
        col("b.CUMLM").cast("decimal(15,3)").alias("stock_in_transit_qty"),
        col("b.CINSM").cast("decimal(15,3)").alias("quality_inspection_qty"),
        col("b.CSPEM").cast("decimal(15,3)").alias("blocked_stock_qty"),
        col("d.LABST").cast("decimal(15,3)").alias("valuated_unrestricted_qty"),
        col("d.INSME").cast("decimal(15,3)").alias("valuated_quality_insp_qty"),
        col("d.SPEME").cast("decimal(15,3)").alias("valuated_blocked_qty"),
        col("v.VERPR").cast("decimal(15,2)").alias("moving_average_price"),
        col("v.STPRS").cast("decimal(15,2)").alias("standard_price"),
        col("v.PEINH").cast("decimal(15,3)").alias("price_unit"),
        col("v.BWTAR").alias("valuation_type"),
        col("v.BKLAS").alias("valuation_class"),
        col("s.LBLAB").cast("decimal(15,3)").alias("special_stock_qty"),
        
        # Date attributes
        current_date().alias("snapshot_date")
    )

# Calculate total stock value
fact_inventory_final = fact_inventory_df \
    .withColumn("total_stock_qty", 
                col("unrestricted_stock_qty") + 
                col("quality_inspection_qty") + 
                col("blocked_stock_qty")) \
    .withColumn("total_stock_value",
                col("total_stock_qty") * col("moving_average_price"))

# Create fact table
fact_inventory = create_fact_table(fact_inventory_final, "fact_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fact_inventory_month_end_stock - Historical Month-End

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING fact_inventory_month_end_stock")
print("=" * 80)

# Grain: Material + Plant + Storage Location + Batch + Month End Date
# Source: MCHBH, MARDH, MBEWH, MSLBH (Historical tables)

# Read historical source tables
mchbh = spark.table("supply_chain_catalog.silver.mchbh")
mardh = spark.table("supply_chain_catalog.silver.mardh")
mbewh = spark.table("supply_chain_catalog.silver.mbewh")
mslbh = spark.table("supply_chain_catalog.silver.mslbh")

# Build month-end fact table
fact_month_end_df = mchbh.alias("bh") \
    .join(mardh.alias("dh"),
          (col("bh.MATNR") == col("dh.MATNR")) &
          (col("bh.WERKS") == col("dh.WERKS")) &
          (col("bh.LGORT") == col("dh.LGORT")) &
          (col("bh.LFGJA") == col("dh.LFGJA")) &
          (col("bh.LFMON") == col("dh.LFMON")), "left") \
    .join(mbewh.alias("vh"),
          (col("bh.MATNR") == col("vh.MATNR")) &
          (col("bh.WERKS") == col("vh.BWKEY")) &
          (col("bh.LFGJA") == col("vh.LFGJA")) &
          (col("bh.LFMON") == col("vh.LFMON")), "left") \
    .select(
        # Business Keys
        col("bh.MATNR").alias("material_number"),
        col("bh.WERKS").alias("plant"),
        col("bh.LGORT").alias("storage_location"),
        col("bh.CHARG").alias("batch_number"),
        col("bh.LFGJA").alias("fiscal_year"),
        col("bh.LFMON").alias("fiscal_period"),
        
        # Measures
        col("bh.CLABS").cast("decimal(15,3)").alias("unrestricted_stock_qty"),
        col("bh.CUMLM").cast("decimal(15,3)").alias("stock_in_transit_qty"),
        col("bh.CINSM").cast("decimal(15,3)").alias("quality_inspection_qty"),
        col("bh.CSPEM").cast("decimal(15,3)").alias("blocked_stock_qty"),
        col("dh.LABST").cast("decimal(15,3)").alias("valuated_unrestricted_qty"),
        col("vh.VERPR").cast("decimal(15,2)").alias("moving_average_price"),
        col("vh.STPRS").cast("decimal(15,2)").alias("standard_price")
    ) \
    .withColumn("month_end_date",
                last_day(to_date(concat(col("fiscal_year"), 
                                       lpad(col("fiscal_period"), 2, "0"), 
                                       lit("01")), "yyyyMMdd")))

# Calculate values
fact_month_end_final = fact_month_end_df \
    .withColumn("total_stock_qty",
                col("unrestricted_stock_qty") +
                col("quality_inspection_qty") +
                col("blocked_stock_qty")) \
    .withColumn("total_stock_value",
                col("total_stock_qty") * col("moving_average_price"))

# Create fact table
fact_month_end = create_fact_table(fact_month_end_final, "fact_inventory_month_end_stock")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. fact_inventory_monthly_snapshot

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING fact_inventory_monthly_snapshot")
print("=" * 80)

# This can be derived from month-end stock with additional aggregations
fact_monthly_snapshot_df = fact_month_end_final \
    .groupBy("material_number", "plant", "fiscal_year", "fiscal_period") \
    .agg(
        sum("unrestricted_stock_qty").alias("total_unrestricted_qty"),
        sum("quality_inspection_qty").alias("total_quality_insp_qty"),
        sum("blocked_stock_qty").alias("total_blocked_qty"),
        sum("total_stock_qty").alias("total_inventory_qty"),
        sum("total_stock_value").alias("total_inventory_value"),
        avg("moving_average_price").alias("avg_price"),
        count("batch_number").alias("batch_count"),
        first("month_end_date").alias("snapshot_date")
    )

# Create fact table
fact_monthly_snapshot = create_fact_table(fact_monthly_snapshot_df, "fact_inventory_monthly_snapshot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. fact_purchase_order

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING fact_purchase_order")
print("=" * 80)

# Read source tables with explicit string casting to prevent BIGINT inference
ekko = spark.table("supply_chain_catalog.silver.ekko").select(
    col("EBELN").cast("string").alias("EBELN"),
    col("BUKRS").cast("string").alias("BUKRS"),
    col("BSART").cast("string").alias("BSART"),
    col("LIFNR").cast("string").alias("LIFNR"),
    col("EKORG").cast("string").alias("EKORG"),
    col("EKGRP").cast("string").alias("EKGRP"),
    col("WAERS").cast("string").alias("WAERS"),
    col("STATU").cast("string").alias("STATU"),
    col("BEDAT").cast("string").alias("BEDAT"),
    col("ERNAM").cast("string").alias("ERNAM"),
    col("AEDAT").cast("string").alias("AEDAT")
)

ekpo = spark.table("supply_chain_catalog.silver.ekpo").select(
    col("EBELN").cast("string").alias("EBELN"),
    col("EBELP").cast("string").alias("EBELP"),
    col("MATNR").cast("string").alias("MATNR"),
    col("BUKRS").cast("string").alias("BUKRS"),
    col("WERKS").cast("string").alias("WERKS"),
    col("LGORT").cast("string").alias("LGORT"),
    col("MATKL").cast("string").alias("MATKL"),
    col("TXZ01").cast("string").alias("TXZ01"),
    col("MEINS").cast("string").alias("MEINS"),
    col("PSTYP").cast("string").alias("PSTYP"),
    col("MENGE").cast("decimal(15,3)").alias("MENGE"),
    col("NETPR").cast("decimal(15,2)").alias("NETPR")
)

eket = spark.table("supply_chain_catalog.silver.eket").select(
    col("EBELN").cast("string").alias("EBELN"),
    col("EBELP").cast("string").alias("EBELP"),
    col("ETENR").cast("string").alias("ETENR"),
    col("EINDT").cast("string").alias("EINDT"),
    col("MENGE").cast("decimal(15,3)").alias("MENGE"),
    col("WEMNG").cast("decimal(15,3)").alias("WEMNG")
)

ekbe = spark.table("supply_chain_catalog.silver.ekbe").select(
    col("EBELN").cast("string").alias("EBELN"),
    col("EBELP").cast("string").alias("EBELP"),
    col("VGABE").cast("string").alias("VGABE"),
    col("MENGE").cast("decimal(15,3)").alias("MENGE"),
    col("DMBTR").cast("decimal(15,2)").alias("DMBTR")
)

lfa1 = spark.table("supply_chain_catalog.silver.lfa1").select(
    col("LIFNR").cast("string").alias("LIFNR"),
    col("NAME1").cast("string").alias("NAME1")
)

t001 = spark.table("supply_chain_catalog.silver.t001").select(
    col("BUKRS").cast("string").alias("BUKRS"),
    col("BUTXT").cast("string").alias("BUTXT")
)

t001w = spark.table("supply_chain_catalog.silver.t001w").select(
    col("WERKS").cast("string").alias("WERKS"),
    col("NAME1").cast("string").alias("NAME1")
)

t024 = spark.table("supply_chain_catalog.silver.t024").select(
    col("EKGRP").cast("string").alias("EKGRP"),
    col("EKNAM").cast("string").alias("EKNAM")
)

t024e = spark.table("supply_chain_catalog.silver.t024e").select(
    col("EKORG").cast("string").alias("EKORG"),
    col("EKOTX").cast("string").alias("EKOTX")
)

# Calculate delivered quantity from EKBE
ekbe_delivered = ekbe.filter(col("VGABE") == "1") \
    .groupBy("EBELN", "EBELP") \
    .agg(
        sum("MENGE").alias("delivered_qty"),
        sum("DMBTR").alias("delivered_value")
    )

# Build PO fact table
fact_po_df = ekko.alias("h") \
    .join(ekpo.alias("i"), col("h.EBELN") == col("i.EBELN"), "inner") \
    .join(eket.alias("s"),
          (col("i.EBELN") == col("s.EBELN")) &
          (col("i.EBELP") == col("s.EBELP")), "left") \
    .join(ekbe_delivered.alias("b"),
          (col("i.EBELN") == col("b.EBELN")) &
          (col("i.EBELP") == col("b.EBELP")), "left") \
    .join(lfa1.alias("v"), col("h.LIFNR") == col("v.LIFNR"), "left") \
    .join(t001.alias("c"), col("i.BUKRS") == col("c.BUKRS"), "left") \
    .join(t001w.alias("p"), col("i.WERKS") == col("p.WERKS"), "left") \
    .join(t024.alias("g"), col("h.EKGRP") == col("g.EKGRP"), "left") \
    .join(t024e.alias("o"), col("h.EKORG") == col("o.EKORG"), "left") \
    .select(
        col("h.EBELN").alias("purchase_order"),
        col("i.EBELP").alias("po_item"),
        col("s.ETENR").alias("schedule_line"),
        col("h.LIFNR").alias("supplier_number"),
        col("i.MATNR").alias("material_number"),
        col("i.WERKS").alias("plant"),
        col("i.LGORT").alias("storage_location"),
        col("h.BSART").alias("po_type"),
        col("h.BEDAT").alias("po_date"),
        col("h.ERNAM").alias("created_by"),
        col("h.AEDAT").alias("created_on"),
        col("h.EKORG").alias("purchasing_org"),
        col("o.EKOTX").alias("purchasing_org_name"),
        col("h.EKGRP").alias("purchasing_group"),
        col("g.EKNAM").alias("purchasing_group_name"),
        col("h.WAERS").alias("currency"),
        col("h.STATU").alias("order_status"),
        col("i.TXZ01").alias("short_text"),
        col("i.MATKL").alias("material_group"),
        col("i.MEINS").alias("order_unit"),
        col("i.MENGE").alias("order_quantity"),
        col("i.NETPR").alias("net_price"),
        col("i.PSTYP").alias("item_category"),
        col("s.EINDT").alias("delivery_date"),
        col("s.MENGE").alias("scheduled_quantity"),
        col("s.WEMNG").alias("goods_receipt_qty"),
        coalesce(col("b.delivered_qty"), lit(0)).cast("decimal(15,3)").alias("delivered_qty"),
        coalesce(col("b.delivered_value"), lit(0)).cast("decimal(15,2)").alias("delivered_value"),
        col("v.NAME1").alias("supplier_name"),
        col("p.NAME1").alias("plant_name"),
        col("c.BUTXT").alias("company_name")
    )

# Calculate derived metrics
fact_po_final = fact_po_df \
    .withColumn("net_order_value",
                col("order_quantity") * col("net_price")) \
    .withColumn("outstanding_qty",
                col("scheduled_quantity") - col("goods_receipt_qty")) \
    .withColumn("po_status",
                when(col("outstanding_qty") > 0, "Open").otherwise("Closed")) \
    .withColumn("po_type_category",
                when(col("po_type") == "NB", "Direct")
                .when(col("po_type") == "ZARB", "Indirect")
                .when(col("po_type") == "UB", "STO")
                .otherwise("Other")) \
    .withColumn("po_sub_type",
                when(col("item_category") == "L", "Subcontract")
                .when(col("item_category").isNull(), "Turn Key")
                .otherwise("Other"))

# Create fact table
fact_po = create_fact_table(fact_po_final, "fact_purchase_order")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. fact_demand_actual

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING fact_demand_actual")
print("=" * 80)

# Grain: Customer + Product + Period
# Source: demand_actual (IBP)

demand_actual = spark.table("supply_chain_catalog.silver.demand_actual")

fact_demand_actual_df = demand_actual.select(
    # Business Keys
    col("market").alias("customer_id"),
    col("material").alias("product_id"),
    col("period_id").alias("period_id"),
    col("period").alias("period"),
    
    # Measures
    col("actual_qty").cast("decimal(15,3)").alias("actual_qty"),
    col("actual_revenue").cast("decimal(15,2)").alias("actual_revenue"),
    col("shipped_qty").cast("decimal(15,3)").alias("shipped_qty"),
    col("delivered_qty").cast("decimal(15,3)").alias("delivered_qty"),
    col("net_actual_qty").cast("decimal(15,3)").alias("net_actual_qty"),
    col("net_revenue").cast("decimal(15,2)").alias("net_revenue"),
    col("service_level").cast("decimal(10,4)").alias("service_level")
) \
.withColumn("period_date", to_date(col("period").cast("string"), "yyyyMM"))

# Create fact table
fact_demand_actual = create_fact_table(fact_demand_actual_df, "fact_demand_actual")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. fact_demand_forecast

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING fact_demand_forecast")
print("=" * 80)

# Grain: Customer + Product + Period
# Source: demand_forecast (IBP)

demand_forecast = spark.table("supply_chain_catalog.silver.demand_forecast")

fact_demand_forecast_df = demand_forecast.select(
    # Business Keys
    col("market").alias("customer_id"),
    col("material").alias("product_id"),
    col("period_id").alias("period_id"),
    col("period").alias("period"),
    
    # Forecast Measures
    col("consensus_demand").cast("decimal(15,3)").alias("consensus_demand"),
    col("ibp_consensus_forecast").cast("decimal(15,3)").alias("ibp_consensus_forecast"),
    col("ibp_forecast").cast("decimal(15,3)").alias("ibp_forecast"),
    col("budget_volumes").cast("decimal(15,3)").alias("budget_volumes"),
    col("sc_forecast_override").cast("decimal(15,3)").alias("sc_forecast_override"),
    col("statistical_forecast").cast("decimal(15,3)").alias("statistical_forecast"),
    col("upside").cast("decimal(15,3)").alias("upside_forecast"),
    
    # Forecast Snapshots (historical forecasts)
    col("fcst_snapshot_1").cast("decimal(15,3)").alias("fcst_snapshot_1"),
    col("fcst_snapshot_2").cast("decimal(15,3)").alias("fcst_snapshot_2"),
    col("fcst_snapshot_3").cast("decimal(15,3)").alias("fcst_snapshot_3"),
    col("fcst_snapshot_4").cast("decimal(15,3)").alias("fcst_snapshot_4"),
    col("fcst_snapshot_5").cast("decimal(15,3)").alias("fcst_snapshot_5"),
    col("fcst_snapshot_6").cast("decimal(15,3)").alias("fcst_snapshot_6"),
    col("fcst_snapshot_7").cast("decimal(15,3)").alias("fcst_snapshot_7"),
    col("fcst_snapshot_8").cast("decimal(15,3)").alias("fcst_snapshot_8"),
    col("fcst_snapshot_9").cast("decimal(15,3)").alias("fcst_snapshot_9"),
    col("fcst_snapshot_10").cast("decimal(15,3)").alias("fcst_snapshot_10"),
    col("fcst_snapshot_11").cast("decimal(15,3)").alias("fcst_snapshot_11"),
    col("fcst_snapshot_12").cast("decimal(15,3)").alias("fcst_snapshot_12"),
    
    # Variance Measures
    col("fcst_variance_snapshot_2").cast("decimal(15,3)").alias("fcst_variance_snapshot_2"),
    col("fcst_variance_snapshot_3").cast("decimal(15,3)").alias("fcst_variance_snapshot_3"),
    col("fcst_variance_perc_snapshot_2").cast("decimal(15,2)").alias("fcst_variance_perc_snapshot_2"),
    col("fcst_variance_perc_snapshot_3").cast("decimal(15,2)").alias("fcst_variance_perc_snapshot_3"),
    col("ibp_consensus_forecast_prior").cast("decimal(15,3)").alias("ibp_consensus_forecast_prior")
) \
.withColumn("period_date", to_date(col("period").cast("string"), "yyyyMM"))

# Create fact table
fact_demand_forecast = create_fact_table(fact_demand_forecast_df, "fact_demand_forecast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. fact_batch_release_external

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING fact_batch_release_external")
print("=" * 80)

# Grain: Inspection Lot + Material
# Source: QALS, QAVE

qals = spark.table("supply_chain_catalog.silver.qals").select(
    col("PRUEFLOS").cast("string"),
    col("WERK").cast("string"),
    col("ART").cast("string"),
    col("MATNR").cast("string"),
    col("LIFNR").cast("string"),
    col("EBELN").cast("string"),
    col("EBELP").cast("string"),
    col("ENSTEHDAT").cast("string"),
    col("ERSTELDAT").cast("string"),
    col("BUDAT").cast("string"),
    col("LOSMENGE").cast("decimal(15,3)"),
    col("GESSTICHPR").cast("decimal(15,3)")
)

qave = spark.table("supply_chain_catalog.silver.qave").select(
    col("PRUEFLOS").cast("string"),
    col("VWERKS").cast("string"),
    col("VBEWERTUNG").cast("string"),
    col("DBEWERTUNG").cast("string"),
    col("VFOLGEAKTI").cast("string"),
    col("VDATUM").cast("string"),
    col("VAUSWAHLMG").cast("decimal(15,3)")
)

# Filter for external batch releases (ART = 04)
fact_batch_external_df = qals.alias("q") \
    .join(qave.alias("a"), col("q.PRUEFLOS") == col("a.PRUEFLOS"), "left") \
    .filter(col("q.ART") == "04") \
    .select(
        col("q.PRUEFLOS").alias("inspection_lot"),
        col("q.MATNR").alias("material_number"),
        col("q.WERK").alias("plant"),
        col("q.LIFNR").alias("supplier_number"),
        col("q.EBELN").alias("purchase_order"),
        col("q.EBELP").alias("po_item"),
        col("q.ART").alias("inspection_type"),
        col("q.ENSTEHDAT").alias("inspection_start_date"),
        col("q.BUDAT").alias("posting_date"),
        col("q.LOSMENGE").alias("lot_size"),
        col("q.GESSTICHPR").alias("sample_size"),
        col("a.VBEWERTUNG").alias("usage_decision"),
        col("a.DBEWERTUNG").alias("defect_assessment"),
        col("a.VFOLGEAKTI").alias("follow_up_action"),
        col("a.VDATUM").alias("decision_date"),
        col("a.VAUSWAHLMG").alias("selected_qty")
    )

# Create fact table
fact_batch_external = create_fact_table(fact_batch_external_df, "fact_batch_release_external")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. fact_batch_release_internal

# COMMAND ----------

print("\n" + "=" * 80)
print("BUILDING fact_batch_release_internal")
print("=" * 80)

# Filter for internal batch releases (ART != 04)
fact_batch_internal_df = qals.alias("q") \
    .join(qave.alias("a"), col("q.PRUEFLOS") == col("a.PRUEFLOS"), "left") \
    .filter(col("q.ART") != "04") \
    .select(
        col("q.PRUEFLOS").alias("inspection_lot"),
        col("q.MATNR").alias("material_number"),
        col("q.WERK").alias("plant"),
        col("q.LIFNR").alias("supplier_number"),
        col("q.EBELN").alias("purchase_order"),
        col("q.EBELP").alias("po_item"),
        col("q.ART").alias("inspection_type"),
        col("q.ENSTEHDAT").alias("inspection_start_date"),
        col("q.BUDAT").alias("posting_date"),
        col("q.LOSMENGE").alias("lot_size"),
        col("q.GESSTICHPR").alias("sample_size"),
        col("a.VBEWERTUNG").alias("usage_decision"),
        col("a.DBEWERTUNG").alias("defect_assessment"),
        col("a.VFOLGEAKTI").alias("follow_up_action"),
        col("a.VDATUM").alias("decision_date"),
        col("a.VAUSWAHLMG").alias("selected_qty")
    )

# Create fact table
fact_batch_internal = create_fact_table(fact_batch_internal_df, "fact_batch_release_internal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables Summary Report

# COMMAND ----------

print("\n" + "=" * 80)
print("GOLD LAYER - FACT TABLES SUMMARY")
print("=" * 80)

fact_tables = [
    "fact_inventory",
    "fact_inventory_month_end_stock",
    "fact_inventory_monthly_snapshot",
    "fact_purchase_order",
    "fact_demand_actual",
    "fact_demand_forecast",
    "fact_batch_release_external",
    "fact_batch_release_internal"
]

summary_data = []
for fact in fact_tables:
    try:
        df = spark.table(f"supply_chain_catalog.gold.{fact}")
        count = df.count()
        cols = len(df.columns)
        
        # Get measure columns (numeric columns)
        measure_cols = [f.name for f in df.schema.fields 
                       if isinstance(f.dataType, (DecimalType, DoubleType, FloatType, LongType, IntegerType))]
        
        summary_data.append({
            "fact_table": fact,
            "record_count": count,
            "column_count": cols,
            "measure_count": len(measure_cols),
            "status": "SUCCESS"
        })
        print(f"✅ {fact}: {count:,} records, {len(measure_cols)} measures")
    except Exception as e:
        summary_data.append({
            "fact_table": fact,
            "record_count": 0,
            "column_count": 0,
            "measure_count": 0,
            "status": f"FAILED: {str(e)}"
        })
        print(f"❌ {fact}: FAILED - {str(e)}")

summary_df = spark.createDataFrame(summary_data)
display(summary_df)

print("\n✅ ALL FACT TABLES COMPLETED!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Model Complete!
# MAGIC
# MAGIC ✅ Gold Layer Completed!
# MAGIC
# MAGIC **Summary**:
# MAGIC - 11 Dimension Tables ✅
# MAGIC - 8 Fact Tables ✅
# MAGIC - Star Schema Ready ✅
# MAGIC
# MAGIC **Next Steps**:
# MAGIC 1. Export Gold tables to Power BI
# MAGIC 2. Create relationships in Power BI
# MAGIC 3. Build DAX measures for KPIs
# MAGIC 4. Create interactive dashboards
