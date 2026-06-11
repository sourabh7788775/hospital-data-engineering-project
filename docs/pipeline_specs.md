# Pipeline Specifications

Technical specifications for the Hospital Management data pipeline.

---

## Pipeline Overview

### Architecture Pattern
**Medallion Architecture** (Bronze-Silver-Gold)

* **Bronze**: Raw data ingestion
* **Silver**: Cleaned and transformed data
* **Gold**: Business-level aggregations

### Execution Model
* **Batch Processing**: Daily/On-demand
* **Compute**: Serverless or dedicated clusters
* **Orchestration**: Manual or workflow-based

---

## Bronze Layer Pipeline

### Purpose
Ingest raw patient data from source systems into Unity Catalog bronze tables.

### Source
* **Table**: `dbutils_catalog.hospital_db.patients`
* **Format**: MongoDB-style documents with JSON data field
* **Sync Method**: Fivetran connector

### Target
* **Table**: `dbutils_catalog.bronze_hospital.patients_raw`
* **Write Mode**: Overwrite
* **Schema**: Preserves source schema

### Processing Steps
1. Create bronze_hospital schema if not exists
2. Read source table
3. Write to bronze table with overwrite
4. Validate record count
5. Display sample records

### Data Quality Checks
* Total record count validation
* Schema structure verification
* Null value detection in critical fields
* Duplicate record identification

### Performance Considerations
* No caching required for initial load
* No partitioning (full refresh pattern)
* Estimated execution time: 1-2 minutes for 10K records

---

## Silver Layer Pipeline

### Purpose
Parse JSON data, standardize formats, and create analytics-ready structured tables.

### Source
* **Table**: `dbutils_catalog.bronze_hospital.patients_raw`
* **Format**: Raw records with JSON data field

### Target
* **Table**: `dbutils_catalog.silver_hospital.patients_clean`
* **Write Mode**: Overwrite with schema overwrite enabled
* **Schema**: Structured relational format

### Transformations

#### 1. JSON Parsing
* Parse `data` field containing patient record JSON
* Extract all fields into structured columns
* Apply schema validation

#### 2. Data Filtering
* Filter out deleted records (`_fivetran_deleted == false`)
* Remove invalid or corrupt records

#### 3. Data Standardization
* **Gender**: Convert to UPPERCASE ("MALE", "FEMALE")
* **City**: Apply Title Case ("Mumbai", "Delhi")
* **Disease**: Apply Title Case ("Diabetes", "Hypertension")

#### 4. Derived Fields
* **Age Group**: Categorize age into groups
  * Child: 0-17 years
  * Adult: 18-59 years
  * Senior Citizen: 60+ years

### Processing Steps
1. Create silver_hospital schema if not exists
2. Read bronze table
3. Define JSON schema structure
4. Parse JSON data field
5. Apply transformations and standardizations
6. Create derived fields
7. Write to silver table with schema overwrite
8. Run data quality checks
9. Create temporary views for validation

### Data Quality Checks
* Null value validation in critical fields
* Duplicate detection by patient_id
* Age range validation (0-120)
* Bill amount range validation (0-1M)
* Empty string detection
* Schema completeness check

### Performance Considerations
* JSON parsing can be compute-intensive
* Consider adaptive query execution
* Estimated execution time: 2-3 minutes for 10K records

---

## Gold Layer Pipeline

### Purpose
Create business-level aggregations and analytics tables for reporting and dashboards.

### Source
* **Table**: `dbutils_catalog.silver_hospital.patients_clean`
* **Format**: Structured relational data

### Target Tables

#### 1. Patient Demographics (`patient_demographics`)
**Purpose**: Gender-wise demographic summary

**Aggregations**:
* Total patients by gender
* Average age by gender
* Min/Max age by gender

#### 2. City Distribution (`city_patient_distribution`)
**Purpose**: Geographic patient distribution

**Aggregations**:
* Patient count by city
* Average age by city

#### 3. Age Group Analysis (`age_group_analysis`)
**Purpose**: Age demographics by gender

**Aggregations**:
* Patient count by age group and gender
* Age groups: 0-17, 18-29, 30-44, 45-59, 60+

#### 4. Disease Analysis (`disease_analysis`)
**Purpose**: Disease prevalence tracking

**Aggregations**:
* Patient count by disease
* Sorted by frequency (descending)

#### 5. Hospital Performance (`hospital_analysis`)
**Purpose**: Hospital-level metrics

**Aggregations**:
* Patient count by hospital
* Average bill amount by hospital

#### 6. Doctor Analysis (`doctor_analysis`)
**Purpose**: Doctor productivity metrics

**Aggregations**:
* Patient count by doctor
* Sorted by patient volume

#### 7. Payment Analysis (`payment_analysis`)
**Purpose**: Payment method distribution

**Aggregations**:
* Transaction count by payment method
* Average bill by payment method

#### 8. City Bill Analysis (`city_bill_analysis`)
**Purpose**: Regional billing patterns

**Aggregations**:
* Average bill amount by city
* Sorted by average bill (descending)

### Processing Steps
1. Create gold_hospital schema if not exists
2. Read silver table
3. For each analytics table:
   * Perform group-by aggregations
   * Apply sorting where applicable
   * Write to gold table with overwrite
   * Display sample results
4. Run data quality validation

### Data Quality Checks
* Verify all aggregation tables exist
* Validate record counts are non-zero
* Check for null values in dimension columns
* Verify aggregation totals match source

### Performance Considerations
* Multiple aggregations can run in parallel
* Use broadcast joins where applicable
* Estimated execution time: 1-2 minutes for 10K source records

---

## End-to-End Pipeline Execution

### Sequential Execution
```
Bronze Pipeline (2 min)
   ↓
Silver Pipeline (3 min)
   ↓
Gold Pipeline (2 min)
   ↓
Total: ~7 minutes
```

### Execution Commands

#### Option 1: Manual Execution
1. Open and run `pipelines/bronze_layer/bronze` notebook
2. Open and run `pipelines/silver_layer/silver` notebook
3. Open and run `pipelines/gold_layer/gold` notebook
4. Refresh dashboard

#### Option 2: Workflow Orchestration
Create a Databricks Job with three tasks:
```yaml
tasks:
  - task_key: bronze_ingestion
    notebook_path: /pipelines/bronze_layer/bronze
    
  - task_key: silver_transformation
    depends_on:
      - task_key: bronze_ingestion
    notebook_path: /pipelines/silver_layer/silver
    
  - task_key: gold_aggregation
    depends_on:
      - task_key: silver_transformation
    notebook_path: /pipelines/gold_layer/gold
```

---

## Error Handling

### Bronze Layer
* **Source table not found**: Verify Fivetran connection
* **Permission denied**: Check Unity Catalog grants
* **Zero records**: Investigate source system

### Silver Layer
* **JSON parsing errors**: Validate schema definition
* **Schema mismatch**: Enable `overwriteSchema` option
* **Null critical fields**: Review data quality at source

### Gold Layer
* **Empty aggregations**: Verify silver table population
* **Incorrect totals**: Review filter conditions

---

## Monitoring & Alerts

### Key Metrics
* Pipeline execution time
* Record counts at each layer
* Data quality check results
* Failure/retry counts

### Alert Conditions
* Pipeline execution time > 15 minutes
* Record count < expected threshold
* Data quality checks fail
* Schema validation errors

---

## Future Enhancements

1. **Incremental Processing**
   * Implement Delta Lake merge operations
   * Track watermarks for incremental loads

2. **Data Quality Framework**
   * Automated quality checks
   * Quality score dashboard
   * Alert integration

3. **Performance Optimization**
   * Implement Z-ordering
   * Optimize partitioning strategy
   * Enable caching for frequently accessed tables

4. **Real-time Processing**
   * Implement streaming pipelines
   * Near real-time dashboard updates

5. **Advanced Analytics**
   * Predictive modeling
   * Anomaly detection
   * Trend forecasting

---

**Version**: 1.0.0  
**Last Updated**: June 2026  
**Owner**: Data Engineering Team