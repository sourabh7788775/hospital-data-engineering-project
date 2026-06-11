# Databricks notebook source
# DBTITLE 1,Silver Layer - Data Transformation
# MAGIC %md
# MAGIC # Silver Layer - Data Transformation
# MAGIC
# MAGIC This notebook transforms bronze data into clean, structured silver tables.
# MAGIC
# MAGIC **Purpose**: Parse JSON data fields, standardize formats, and create analytics-ready tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleaning and Transformation

# COMMAND ----------

# DBTITLE 1,Transform Patients - Silver
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create silver_hospital schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS dbutils_catalog.silver_hospital")

# Read bronze patients data
patients_bronze = spark.table("dbutils_catalog.bronze_hospital.patients_raw")

# Define schema for JSON data field
patients_schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("patient_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("_id", StringType(), True),
    StructField("disease", StringType(), True),
    StructField("doctor_name", StringType(), True),
    StructField("hospital_name", StringType(), True),
    StructField("admission_date", StringType(), True),
    StructField("discharge_date", StringType(), True),
    StructField("bill_amount", IntegerType(), True),
    StructField("payment_method", StringType(), True)
])

# Parse JSON and flatten structure
patients_silver = patients_bronze \
    .filter(col("_fivetran_deleted") == False) \
    .withColumn("parsed_data", from_json(col("data"), patients_schema)) \
    .select(
        col("parsed_data.patient_id").alias("patient_id"),
        col("parsed_data.patient_name").alias("patient_name"),
        col("parsed_data.age").alias("age"),
        col("parsed_data.gender").alias("gender"),
        col("parsed_data.city").alias("city"),
        col("parsed_data.disease").alias("disease"),
        col("parsed_data.doctor_name").alias("doctor_name"),
        col("parsed_data.hospital_name").alias("hospital_name"),
        col("parsed_data.admission_date").alias("admission_date"),
        col("parsed_data.discharge_date").alias("discharge_date"),
        col("parsed_data.bill_amount").alias("bill_amount"),
        col("parsed_data.payment_method").alias("payment_method"),
        col("_fivetran_synced").alias("loaded_at")
    )

# Write to silver table with schema overwrite
patients_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbutils_catalog.silver_hospital.patients_clean")

print(f"✓ Transformed {patients_silver.count()} patient records to silver layer")
display(patients_silver)

# COMMAND ----------

#Total Record Count
display(patients_silver.count())

# COMMAND ----------

#Standardize Text
from pyspark.sql.functions import upper, initcap

patients_silver = (
    patients_silver
    .withColumn(
        "gender",
        upper(col("gender"))
    )
    .withColumn(
        "city",
        initcap(col("city"))
    )
    .withColumn(
        "disease",
        initcap(col("disease"))
    )
)

display(patients_silver)


# COMMAND ----------

from pyspark.sql.functions import when

patients_silver = patients_silver.withColumn(
    "age_group",
    when(col("age") < 18, "Child")
    .when(col("age") < 60, "Adult")
    .otherwise("Senior Citizen")
).show()

# COMMAND ----------

#Create Temporary View
patients_silver.createOrReplaceTempView(
    "vw_patients_silver"
)

# COMMAND ----------

display(
    spark.sql("""
    SELECT city,
           COUNT(*) total_patients
    FROM vw_patients_silver
    GROUP BY city
    """)
)

# COMMAND ----------

print("===================================")
print("Silver Layer Completed")
print("Data Cleaning Successful")
print("===================================")
