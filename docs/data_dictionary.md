# Data Dictionary

Comprehensive data dictionary for the Hospital Management Analytics System.

---

## Bronze Layer Tables

### `dbutils_catalog.bronze_hospital.patients_raw`

Raw patient data ingested from source systems without transformation.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| `_id` | string | Unique MongoDB document ID | Source System |
| `data` | string | JSON string containing patient record | Source System |
| `_fivetran_deleted` | boolean | Fivetran deletion flag | Fivetran |
| `_fivetran_synced` | timestamp | Fivetran sync timestamp | Fivetran |

---

## Silver Layer Tables

### `dbutils_catalog.silver_hospital.patients_clean`

Cleaned and transformed patient data with structured schema.

| Column Name | Data Type | Description | Transformation | Example |
|-------------|-----------|-------------|----------------|----------|
| `patient_id` | integer | Unique patient identifier | Parsed from JSON | 107691 |
| `patient_name` | string | Full name of patient | Parsed from JSON | "John Doe" |
| `age` | integer | Patient age in years | Parsed from JSON | 45 |
| `gender` | string | Patient gender | Uppercase standardization | "MALE", "FEMALE" |
| `city` | string | Patient city of residence | Title case standardization | "Mumbai", "Delhi" |
| `disease` | string | Diagnosed disease/condition | Title case standardization | "Diabetes", "Hypertension" |
| `doctor_name` | string | Attending physician name | Parsed from JSON | "Dr. Smith" |
| `hospital_name` | string | Hospital name | Parsed from JSON | "Apollo Care" |
| `admission_date` | string | Date of admission | Parsed from JSON | "2023-06-24" |
| `discharge_date` | string | Date of discharge | Parsed from JSON | "2023-07-07" |
| `bill_amount` | integer | Total billing amount | Parsed from JSON | 125000 |
| `payment_method` | string | Payment mode | Parsed from JSON | "Cash", "Card", "Insurance", "UPI" |
| `loaded_at` | timestamp | Data load timestamp | From Fivetran sync | 2026-06-11 02:39:36 |

---

## Gold Layer Tables

### `dbutils_catalog.gold_hospital.patient_demographics`

Gender-wise demographic summary.

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `gender` | string | Patient gender | Dimension |
| `total_patients` | long | Total patient count | COUNT(*) |
| `avg_age` | double | Average patient age | AVG(age) |
| `min_age` | integer | Minimum age | MIN(age) |
| `max_age` | integer | Maximum age | MAX(age) |

### `dbutils_catalog.gold_hospital.city_patient_distribution`

City-wise patient distribution.

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `city` | string | City name | Dimension |
| `patient_count` | long | Total patients from city | COUNT(*) |
| `avg_age` | double | Average age in city | AVG(age) |

### `dbutils_catalog.gold_hospital.age_group_analysis`

Age group analysis by gender.

| Column Name | Data Type | Description | Definition |
|-------------|-----------|-------------|------------|
| `age_group` | string | Age category | 0-17, 18-29, 30-44, 45-59, 60+ |
| `gender` | string | Patient gender | MALE, FEMALE |
| `patient_count` | long | Count of patients | COUNT(*) |

### `dbutils_catalog.gold_hospital.disease_analysis`

Disease-wise patient counts.

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `disease` | string | Disease/condition name | Dimension |
| `patient_count` | long | Number of patients | COUNT(*) |

### `dbutils_catalog.gold_hospital.hospital_analysis`

Hospital performance metrics.

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `hospital_name` | string | Hospital name | Dimension |
| `patient_count` | long | Total patients treated | COUNT(*) |
| `avg_bill` | double | Average bill amount | AVG(bill_amount) |

### `dbutils_catalog.gold_hospital.doctor_analysis`

Doctor-wise patient counts.

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `doctor_name` | string | Doctor name | Dimension |
| `patient_count` | long | Patients treated | COUNT(*) |

### `dbutils_catalog.gold_hospital.payment_analysis`

Payment method analysis.

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `payment_method` | string | Payment mode | Dimension |
| `total_transactions` | long | Number of transactions | COUNT(*) |
| `avg_bill` | double | Average transaction amount | AVG(bill_amount) |

### `dbutils_catalog.gold_hospital.city_bill_analysis`

Average billing by city.

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `city` | string | City name | Dimension |
| `average_bill` | double | Average bill in city | AVG(bill_amount) |

---

## Data Types Reference

| Type | Description | Range/Format |
|------|-------------|-------------|
| `integer` | Whole numbers | -2,147,483,648 to 2,147,483,647 |
| `long` | Large integers | -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 |
| `double` | Floating point | 64-bit IEEE 754 |
| `string` | Text data | Variable length |
| `boolean` | True/False | true, false |
| `timestamp` | Date and time | YYYY-MM-DD HH:MM:SS |

---

## Business Rules

### Age Groups
* **Child**: 0-17 years
* **Adult**: 18-59 years
* **Senior Citizen**: 60+ years

### Gender Values
* Standardized to: "MALE", "FEMALE"
* Source variations normalized to uppercase

### Payment Methods
* Cash
* Card
* Insurance
* UPI

### Cities Covered
* Mumbai
* Delhi
* Kolkata
* Chennai
* Bangalore
* Hyderabad
* Pune
* Ahmedabad
* Jaipur
* And more...

---

**Version**: 1.0.0  
**Last Updated**: June 2026  
**Maintained By**: Data Engineering Team