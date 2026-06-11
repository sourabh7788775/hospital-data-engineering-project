# Databricks notebook source
# DBTITLE 1,Gold Layer - Analytics
# MAGIC %md
# MAGIC # Gold Layer - Analytics
# MAGIC
# MAGIC This notebook creates aggregated analytics tables from silver data.
# MAGIC
# MAGIC **Purpose**: Generate business metrics, KPIs, and analytical views for reporting and dashboards.

# COMMAND ----------

# DBTITLE 1,Patient Demographics Summary
from pyspark.sql.functions import count, avg, min, max, col

# Create gold_hospital schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS dbutils_catalog.gold_hospital")

# Read silver patients data
patients = spark.table("dbutils_catalog.silver_hospital.patients_clean")

# Create demographics summary
demographics_summary = patients.groupBy("gender").agg(
    count("*").alias("total_patients"),
    avg("age").alias("avg_age"),
    min("age").alias("min_age"),
    max("age").alias("max_age")
).orderBy("gender")

# Write to gold table
demographics_summary.write.mode("overwrite").saveAsTable("dbutils_catalog.gold_hospital.patient_demographics")

print("✓ Created patient demographics summary")
display(demographics_summary)

# COMMAND ----------

# DBTITLE 1,City-wise Patient Distribution
# City-wise patient distribution
city_distribution = patients.groupBy("city").agg(
    count("*").alias("patient_count"),
    avg("age").alias("avg_age")
).orderBy(col("patient_count").desc())

# Write to gold table
city_distribution.write.mode("overwrite").saveAsTable("dbutils_catalog.gold_hospital.city_patient_distribution")

print("✓ Created city-wise patient distribution")
display(city_distribution)

# COMMAND ----------

# DBTITLE 1,Age Group Analysis
from pyspark.sql.functions import when

# Create age groups and analyze
age_groups = patients.withColumn(
    "age_group",
    when(col("age") < 18, "0-17")
    .when((col("age") >= 18) & (col("age") < 30), "18-29")
    .when((col("age") >= 30) & (col("age") < 45), "30-44")
    .when((col("age") >= 45) & (col("age") < 60), "45-59")
    .otherwise("60+")
)

age_group_summary = age_groups.groupBy("age_group", "gender").agg(
    count("*").alias("patient_count")
).orderBy("age_group", "gender")

# Write to gold table
age_group_summary.write.mode("overwrite").saveAsTable("dbutils_catalog.gold_hospital.age_group_analysis")

print("✓ Created age group analysis")
display(age_group_summary)

# COMMAND ----------

#Disease Wise Analysis
from pyspark.sql.functions import count

disease_summary = patients.groupBy("disease").agg(
    count("*").alias("patient_count")
).orderBy(col("patient_count").desc())

disease_summary.write.mode("overwrite").saveAsTable(
    "dbutils_catalog.gold_hospital.disease_analysis"
)

display(disease_summary)

# COMMAND ----------

#Hospital Wise Analysis

hospital_summary = patients.groupBy("hospital_name").agg(
    count("*").alias("patient_count"),
    avg("bill_amount").alias("avg_bill")
).orderBy(col("patient_count").desc())

hospital_summary.write.mode("overwrite").saveAsTable(
    "dbutils_catalog.gold_hospital.hospital_analysis"
)

display(hospital_summary)

# COMMAND ----------

#Doctor Wise Analysis
doctor_summary = patients.groupBy("doctor_name").agg(
    count("*").alias("patient_count")
).orderBy(col("patient_count").desc())

doctor_summary.write.mode("overwrite").saveAsTable(
    "dbutils_catalog.gold_hospital.doctor_analysis"
)

display(doctor_summary)

# COMMAND ----------

#Payment Method Analysis

payment_summary = patients.groupBy("payment_method").agg(
    count("*").alias("total_transactions"),
    avg("bill_amount").alias("avg_bill")
)

payment_summary.write.mode("overwrite").saveAsTable(
    "dbutils_catalog.gold_hospital.payment_analysis"
)

display(payment_summary)

# COMMAND ----------

#Average Bill by City

bill_summary = patients.groupBy("city").agg(
    avg("bill_amount").alias("average_bill")
).orderBy(col("average_bill").desc())

bill_summary.write.mode("overwrite").saveAsTable(
    "dbutils_catalog.gold_hospital.city_bill_analysis"
)

display(bill_summary)

# COMMAND ----------

#Monthly Admissions

from pyspark.sql.functions import month

monthly_summary = patients.withColumn(
    "admission_month",
    month("admission_date")
).groupBy("admission_month").agg(
    count("*").alias("patient_count")
)

display(monthly_summary)

# COMMAND ----------

#Top 10 Highest Bills

top_bill_patients = patients.orderBy(
    col("bill_amount").desc()
).limit(10)

display(top_bill_patients)

# COMMAND ----------

#Gold Layer Validation

print("Total Patients :", patients.count())
print("Total Columns :", len(patients.columns))

# COMMAND ----------

#Create Temporary Views

demographics_summary.createOrReplaceTempView("vw_demographics")
disease_summary.createOrReplaceTempView("vw_disease")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city,
# MAGIC        patient_count
# MAGIC FROM dbutils_catalog.gold_hospital.city_patient_distribution
# MAGIC ORDER BY patient_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city,
# MAGIC        patient_count
# MAGIC FROM dbutils_catalog.gold_hospital.city_patient_distribution
# MAGIC ORDER BY patient_count DESC
