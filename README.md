# Pipelines Directory

This directory contains all data pipeline notebooks organized by Medallion Architecture layers.

## Folder Structure

```
pipelines/
├── bronze_layer/
│   └── bronze         # Raw data ingestion notebook
├── silver_layer/
│   └── silver         # Data transformation & cleaning notebook
└── gold_layer/
    └── gold           # Analytics & aggregation notebook
```

## Notebook Organization

### Bronze Layer (`bronze_layer/`)
**Purpose**: Raw data ingestion from source systems

**Notebooks**:
* `bronze` - Ingests raw patient data from `hospital_db.patients` into bronze tables

**Key Tables**:
* `dbutils_catalog.bronze_hospital.patients_raw`

### Silver Layer (`silver_layer/`)
**Purpose**: Data cleaning, transformation, and standardization

**Notebooks**:
* `silver` - Parses JSON, standardizes formats, creates structured tables

**Key Tables**:
* `dbutils_catalog.silver_hospital.patients_clean`

### Gold Layer (`gold_layer/`)
**Purpose**: Business-level aggregations and analytics

**Notebooks**:
* `gold` - Creates analytical tables for dashboards and reporting

**Key Tables**:
* `dbutils_catalog.gold_hospital.patient_demographics`
* `dbutils_catalog.gold_hospital.city_patient_distribution`
* `dbutils_catalog.gold_hospital.age_group_analysis`
* `dbutils_catalog.gold_hospital.disease_analysis`
* `dbutils_catalog.gold_hospital.hospital_analysis`
* And more...

## Execution Order

Always execute notebooks in sequence:

1. **Bronze Layer** → Ingest raw data
2. **Silver Layer** → Transform and clean
3. **Gold Layer** → Create analytics tables

## Migration Instructions

Your existing notebooks are currently in the root `hospital_management/` folder:
* `/hospital_management/bronze`
* `/hospital_management/silver`
* `/hospital_management/gold`

### Option 1: Move Notebooks (Recommended)
Manually move or copy your existing notebooks into the appropriate layer folders:

1. Move `bronze` notebook → `pipelines/bronze_layer/bronze`
2. Move `silver` notebook → `pipelines/silver_layer/silver`
3. Move `gold` notebook → `pipelines/gold_layer/gold`

### Option 2: Keep Current Location
You can continue using the notebooks in their current location. The folder structure is organizational guidance.

## Adding New Pipelines

When adding new data sources or pipelines:

1. Create subdirectories in the appropriate layer folder
2. Follow naming convention: `{source}_{entity}`
3. Example: `bronze_layer/hr_employees/`, `silver_layer/hr_employees/`

## Best Practices

1. **One notebook per layer per entity** - Keep notebooks focused
2. **Clear naming** - Use descriptive names for notebooks
3. **Documentation** - Add markdown cells explaining logic
4. **Error handling** - Include try-catch blocks for production
5. **Logging** - Print execution summaries and record counts
6. **Idempotency** - Ensure notebooks can be re-run safely

## Configuration

Pipeline configurations are stored in `/config/pipeline_config.py`:
* Database names
* Schema names
* Table names
* Data quality rules
* Performance settings

## Data Quality

Utility functions for data quality checks are available in `/utils/data_quality_checks.py`:
* Null value checks
* Duplicate detection
* Range validation
* Schema verification

## Support

For questions or issues:
* See main project README: `/hospital_management/README.md`
* Check pipeline specifications: `/docs/pipeline_specs.md`
* Review data dictionary: `/docs/data_dictionary.md`

---

**Version**: 1.0.0  
**Last Updated**: June 2026