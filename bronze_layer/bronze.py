# Databricks notebook source
# DBTITLE 1,Bronze Layer - Raw Data Ingestion
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC This notebook loads raw data from the source hospital_db schema into bronze tables.
# MAGIC
# MAGIC **Purpose**: Create bronze tables that mirror the source data structure for audit and reprocessing.

# COMMAND ----------

# DBTITLE 1,Load Patients - Bronze
# Create bronze_hospital schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS dbutils_catalog.bronze_hospital")

# Load raw patients data into bronze layer
patients_bronze_df = spark.table("dbutils_catalog.hospital_db.patients")

# Write to bronze table
patients_bronze_df.write.mode("overwrite").saveAsTable("dbutils_catalog.bronze_hospital.patients_raw")

print(f"✓ Loaded {patients_bronze_df.count()} patient records to bronze layer")
display(patients_bronze_df.limit(5))

# COMMAND ----------

#Check Schema
patients_bronze_df.printSchema()

# COMMAND ----------

#Total Records Count

print("Total Records :", patients_bronze_df.count())

# COMMAND ----------

#Total Columns Count
print("Total Columns :", len(patients_bronze_df.columns))


# COMMAND ----------

from pyspark.sql.functions import count, when, isnull, col

#Column Names
print("Column Names :", patients_bronze_df.columns)
#Distinct Records Count
print("Distinct Records :", patients_bronze_df.distinct().count())
#Check for Null Values
patients_bronze_df.select([count(when(isnull(c), c)).alias(c) for c in patients_bronze_df.columns]).display()
#Check for Duplicates
patients_bronze_df.groupBy(patients_bronze_df.columns).count().filter("count > 1").display()
#Check for Empty Values
patients_bronze_df.filter(col("data").isNull()).display()
#Check for Empty Values
patients_bronze_df.filter(col("data") == "").display()


# COMMAND ----------

#Duplicate Record Check.
print(
    "Duplicate Records :",
  patients_bronze_df.count()
    -patients_bronze_df.dropDuplicates(["_id"]).count()
)

# COMMAND ----------

#Create Bronze View
patients_bronze_df.createOrReplaceTempView("patients_bronze")


# COMMAND ----------

spark.sql("""
select *
from patients_bronze
limit 10
""").show()

# COMMAND ----------

#Bronze Layer Summary

print("================================")
print("Bronze Layer Completed")
print("Raw Data Successfully Loaded")
print("================================")
