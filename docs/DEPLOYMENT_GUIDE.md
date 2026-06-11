# Deployment Guide

Comprehensive guide for deploying and maintaining the Hospital Management Analytics System.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Initial Setup](#initial-setup)
3. [Deployment Steps](#deployment-steps)
4. [Testing & Validation](#testing--validation)
5. [Monitoring](#monitoring)
6. [Troubleshooting](#troubleshooting)
7. [Rollback Procedures](#rollback-procedures)

---

## Prerequisites

### Required Access
* Databricks Workspace access
* Unity Catalog permissions:
  * `CREATE SCHEMA` on catalog `dbutils_catalog`
  * `CREATE TABLE` on schemas: `bronze_hospital`, `silver_hospital`, `gold_hospital`
  * `SELECT` on source schema: `hospital_db`
  * `EXECUTE` permissions for notebooks

### Compute Resources
* **Option 1**: Serverless compute (Recommended)
  * Auto-scaling
  * No cluster management
  * Pay-per-use

* **Option 2**: Dedicated cluster
  * Runtime: DBR 13.3 LTS or higher
  * Minimum: 2 workers (8 cores, 32 GB RAM each)
  * Photon enabled (recommended)

### Dependencies
* PySpark (built-in)
* No additional libraries required

---

## Initial Setup

### Step 1: Verify Source Data

Ensure source table exists and contains data:

```sql
SELECT COUNT(*) FROM dbutils_catalog.hospital_db.patients;
```

Expected: > 0 records

### Step 2: Create Schemas

Run the following SQL commands:

```sql
CREATE SCHEMA IF NOT EXISTS dbutils_catalog.bronze_hospital;
CREATE SCHEMA IF NOT EXISTS dbutils_catalog.silver_hospital;
CREATE SCHEMA IF NOT EXISTS dbutils_catalog.gold_hospital;
```

### Step 3: Set Up Permissions

Grant necessary permissions to execution service principal or user group:

```sql
-- Grant schema usage
GRANT USE SCHEMA ON SCHEMA dbutils_catalog.bronze_hospital TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA dbutils_catalog.silver_hospital TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA dbutils_catalog.gold_hospital TO `data_engineers`;

-- Grant table creation
GRANT CREATE TABLE ON SCHEMA dbutils_catalog.bronze_hospital TO `data_engineers`;
GRANT CREATE TABLE ON SCHEMA dbutils_catalog.silver_hospital TO `data_engineers`;
GRANT CREATE TABLE ON SCHEMA dbutils_catalog.gold_hospital TO `data_engineers`;
```

### Step 4: Configure Pipeline

Update configuration in `/config/pipeline_config.py` if needed:
* Catalog name
* Schema names
* Performance settings

---

## Deployment Steps

### Development Environment

#### Step 1: Deploy Bronze Layer

1. Navigate to `/pipelines/bronze_layer/bronze` notebook
2. Attach to serverless compute or development cluster
3. Run all cells
4. Verify output:
   * Schema `bronze_hospital` created
   * Table `patients_raw` populated
   * Record count matches source

#### Step 2: Deploy Silver Layer

1. Navigate to `/pipelines/silver_layer/silver` notebook
2. Run all cells
3. Verify output:
   * Schema `silver_hospital` created
   * Table `patients_clean` populated
   * Data quality checks pass
   * Transformations applied correctly

#### Step 3: Deploy Gold Layer

1. Navigate to `/pipelines/gold_layer/gold` notebook
2. Run all cells
3. Verify output:
   * Schema `gold_hospital` created
   * All analytical tables populated:
     * `patient_demographics`
     * `city_patient_distribution`
     * `age_group_analysis`
     * `disease_analysis`
     * `hospital_analysis`
     * `payment_analysis`
     * And others

#### Step 4: Deploy Dashboard

1. Navigate to `/dashboards/Hospital Management Analytics Dashboard`
2. Publish dashboard
3. Verify all visualizations load correctly
4. Test interactivity and filters

### Production Environment

#### Option 1: Manual Execution

**Schedule**:
* Frequency: Daily at 2:00 AM
* Execution: Manual trigger or scheduled notebook runs

**Steps**:
1. Run Bronze notebook
2. Wait for completion
3. Run Silver notebook
4. Wait for completion
5. Run Gold notebook
6. Refresh dashboards

#### Option 2: Workflow Orchestration (Recommended)

**Create Databricks Job**:

```yaml
name: Hospital Management ETL Pipeline
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/New_York"

tasks:
  - task_key: bronze_ingestion
    notebook_task:
      notebook_path: /Users/sourabh5423043@gmail.com/hospital_management/pipelines/bronze_layer/bronze
      source: WORKSPACE
    new_cluster:
      num_workers: 2
      spark_version: 13.3.x-scala2.12
      node_type_id: i3.xlarge
    timeout_seconds: 1800
    
  - task_key: silver_transformation
    depends_on:
      - task_key: bronze_ingestion
    notebook_task:
      notebook_path: /Users/sourabh5423043@gmail.com/hospital_management/pipelines/silver_layer/silver
      source: WORKSPACE
    new_cluster:
      num_workers: 2
      spark_version: 13.3.x-scala2.12
      node_type_id: i3.xlarge
    timeout_seconds: 1800
    
  - task_key: gold_aggregation
    depends_on:
      - task_key: silver_transformation
    notebook_task:
      notebook_path: /Users/sourabh5423043@gmail.com/hospital_management/pipelines/gold_layer/gold
      source: WORKSPACE
    new_cluster:
      num_workers: 2
      spark_version: 13.3.x-scala2.12
      node_type_id: i3.xlarge
    timeout_seconds: 1800

email_notifications:
  on_failure:
    - data-engineering@hospital.com
  on_success:
    - data-engineering@hospital.com
```

**Create Job via UI**:
1. Go to Workflows → Create Job
2. Add three tasks (Bronze, Silver, Gold)
3. Configure dependencies
4. Set schedule
5. Add email notifications
6. Save and run

---

## Testing & Validation

### Unit Tests

Run data quality checks using utility functions:

```python
from utils.data_quality_checks import (
    run_bronze_quality_checks,
    run_silver_quality_checks,
    run_gold_quality_checks
)

# Test Bronze Layer
bronze_df = spark.table("dbutils_catalog.bronze_hospital.patients_raw")
results = run_bronze_quality_checks(bronze_df, "patients_raw")

# Test Silver Layer
silver_df = spark.table("dbutils_catalog.silver_hospital.patients_clean")
results = run_silver_quality_checks(silver_df, "patients_clean")

# Test Gold Layer
gold_df = spark.table("dbutils_catalog.gold_hospital.patient_demographics")
results = run_gold_quality_checks(gold_df, "patient_demographics")
```

### Integration Tests

1. **End-to-End Test**:
   * Run all three notebooks in sequence
   * Verify record counts at each layer
   * Validate dashboard loads

2. **Data Consistency Test**:
   ```sql
   -- Verify totals match across layers
   SELECT COUNT(*) FROM dbutils_catalog.bronze_hospital.patients_raw;
   SELECT COUNT(*) FROM dbutils_catalog.silver_hospital.patients_clean;
   
   -- Should match (minus deleted records)
   ```

3. **Dashboard Test**:
   * Open dashboard
   * Verify all widgets load
   * Test filters and interactions
   * Validate KPI calculations

---

## Monitoring

### Key Metrics

1. **Pipeline Health**:
   * Execution success rate
   * Average execution time
   * Failure count and reasons

2. **Data Quality**:
   * Null value percentages
   * Duplicate record count
   * Schema drift detection

3. **Performance**:
   * Query execution times
   * Resource utilization
   * Cost per run

### Monitoring Dashboard

Create a monitoring dashboard with:
* Pipeline run history
* Record count trends
* Data quality scores
* Performance metrics

### Alerts

Set up alerts for:
* Pipeline failures
* Data quality issues (> 5% null values)
* Record count anomalies (> 20% change)
* Execution time exceeds threshold (> 15 min)

---

## Troubleshooting

### Common Issues

#### Issue 1: Source Table Not Found

**Error**: `Table or view not found: dbutils_catalog.hospital_db.patients`

**Solution**:
1. Verify Fivetran connection is active
2. Check table name spelling
3. Verify Unity Catalog permissions

#### Issue 2: JSON Parsing Errors

**Error**: `AnalysisException: Cannot resolve column name "data"`

**Solution**:
1. Verify schema definition matches source data
2. Check for schema changes in source
3. Update JSON schema in Silver notebook

#### Issue 3: Permission Denied

**Error**: `Permission denied: User does not have CREATE TABLE`

**Solution**:
1. Check Unity Catalog permissions
2. Request necessary grants from admin
3. Use service principal with proper permissions

#### Issue 4: Slow Performance

**Symptom**: Pipeline takes > 15 minutes

**Solution**:
1. Enable Photon on cluster
2. Increase worker count
3. Optimize shuffle partitions
4. Review query plans for bottlenecks

---

## Rollback Procedures

### Rollback Bronze Layer

```sql
-- Restore from previous version
RESTORE TABLE dbutils_catalog.bronze_hospital.patients_raw 
TO VERSION AS OF <version_number>;
```

### Rollback Silver Layer

```sql
RESTORE TABLE dbutils_catalog.silver_hospital.patients_clean 
TO VERSION AS OF <version_number>;
```

### Rollback Gold Layer

```sql
-- Restore each gold table
RESTORE TABLE dbutils_catalog.gold_hospital.patient_demographics 
TO VERSION AS OF <version_number>;

-- Repeat for other gold tables
```

### Emergency Stop

If pipeline is causing issues:

1. **Cancel Running Jobs**:
   * Navigate to Workflows
   * Find running job
   * Click "Cancel Run"

2. **Disable Schedule**:
   * Edit job
   * Remove schedule
   * Save

3. **Investigate Root Cause**:
   * Check execution logs
   * Review recent changes
   * Validate source data

---

## Maintenance

### Weekly Tasks
* Review pipeline execution logs
* Check data quality reports
* Monitor resource usage

### Monthly Tasks
* Review and optimize query performance
* Update documentation for any changes
* Vacuum old table versions
* Review and update permissions

### Quarterly Tasks
* Conduct security audit
* Review cost optimization opportunities
* Update disaster recovery procedures
* Training refresh for team members

---

## Support Contacts

* **Data Engineering Team**: data-engineering@hospital.com
* **Databricks Support**: support.databricks.com
* **On-Call**: [Insert on-call rotation]

---

**Version**: 1.0.0  
**Last Updated**: June 2026  
**Maintained By**: Data Engineering Team