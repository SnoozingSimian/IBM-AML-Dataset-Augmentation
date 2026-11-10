# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md 
# MAGIC # 01 - Bronze Ingestion (LI-Small) 
# MAGIC Ingest raw CSVs from Unity Catalog Volume to Delta Bronze tables with strict schema enforcement.

# COMMAND ----------

# 1. Parameterize dataset size and catalog 
dbutils.widgets.text("catalog", "ibm-aml") 
dbutils.widgets.text("schema", "bronze") 
dbutils.widgets.dropdown("dataset_size", "Small", ["Small", "Medium"])

catalog = dbutils.widgets.get("catalog") 
schema = dbutils.widgets.get("schema") 
dataset_size = dbutils.widgets.get("dataset_size")

volume_base = f"/Volumes/{catalog}/{schema}/raw-store/"

# COMMAND ----------

# 2. Define Explicit Schemas (Avoids slow and error-prone schema inference) 
from pyspark.sql.types import ( StructType, StructField, StringType, DecimalType, TimestampType, IntegerType )

# 1. Schema for LI-Medium_Trans.csv 
transactions_schema = StructType(
    [
        StructField("timestamp", TimestampType(), True), 
        StructField("from_bank", StringType(), True), 
        StructField("from_account", StringType(), True), 
        StructField("to_bank", StringType(), True), 
        StructField("to_account", StringType(), True), 
        StructField("amount_received", DecimalType(18, 2), True), 
        StructField("receiving_currency", StringType(), True), 
        StructField("amount_paid", DecimalType(18, 2), True), 
        StructField("payment_currency", StringType(), True), 
        StructField("payment_format", StringType(), True),
        StructField("is_laundering", IntegerType(), True)
    ])

# 2. Schema for LI-Medium_accounts.csv 
accounts_schema = StructType(
    [
        StructField("bank_name", StringType(), True), 
        StructField("bank_id", StringType(), True), 
        StructField("account_number", StringType(), True), 
        StructField("entity_id", StringType(), True), 
        StructField("entity_name", StringType(), True)
    ])

# COMMAND ----------

# 3. Read and write Transactions 
trans_csv_path = f"{volume_base}LI-{dataset_size}_Trans.csv" 
df_trans = (
    spark.read.format("csv") 
    .option("header", "true") 
    .option("timestampFormat", "yyyy/MM/dd HH:mm") 
    .schema(transactions_schema) 
    .load(trans_csv_path)
    ) 
    
(
    df_trans.write.format("delta") 
    .mode("overwrite") 
    .saveAsTable(f"`{catalog}`.`{schema}`.aml_bronze_transactions_{dataset_size.lower()}")
)

# COMMAND ----------

# 4. Read and write Accounts 
accounts_csv_path = f"{volume_base}LI-{dataset_size}_accounts.csv" 
df_accounts = (
    spark.read.format("csv") 
    .option("header", "true") 
    .schema(accounts_schema) 
    .load(accounts_csv_path)
    ) 
(
    df_accounts.write.format("delta") 
    .mode("overwrite") 
    .saveAsTable(f"`{catalog}`.`{schema}`.aml_bronze_accounts_{dataset_size.lower()}")
)