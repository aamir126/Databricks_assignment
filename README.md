# 📊 Education Analytics Pipeline (Databricks + Unity Catalog)

## 🚀 Project Overview

This project implements an end-to-end **Education Analytics Data
Pipeline** using:

-   Databricks
-   PySpark
-   Delta Lake
-   Unity Catalog
-   Medallion Architecture (Bronze → Silver → Gold)

The pipeline simulates real-world education data processing and produces
business-ready analytical tables for dashboards and reporting.

------------------------------------------------------------------------

# 🏗️ Architecture

Raw Layer (CSV in Unity Catalog Volume) ↓ Bronze Layer (Raw Delta Table)
↓ Silver Layer (Cleaned & Standardized Data) ↓ Gold Layer (Aggregated
Business Metrics) ↓ Dashboard

------------------------------------------------------------------------

# 🗂️ Unity Catalog Structure

**Catalog:** education_catalog\
**Schema:** education_schema\
**Volume (Raw Files):** raw_volume

All tables are created under: education_catalog.education_schema

------------------------------------------------------------------------

# 📥 Raw Layer (Unity Catalog Volume)

Raw CSV files are stored in:
/Volumes/education_catalog/education_schema/raw_volume/

Purpose: - Store incoming CSV files - Govern file-level access - Act as
landing zone for ingestion

------------------------------------------------------------------------

# 🥉 Bronze Layer

Table: bronze_education1

Description: - Ingests raw CSV data - Stored as Delta table -
Partitioned by year - Preserves raw structure

------------------------------------------------------------------------

# 🥈 Silver Layer

Table: silver_education1

Transformations: - Removed invalid enrollments - Standardized text
fields - Standardized gender values (M → MALE, F → FEMALE) - Ensured
grade between 1--12 - Derived academic_category - Added processing
timestamp

Purpose: Create clean, analytics-ready dataset.

------------------------------------------------------------------------

# 🥇 Gold Layer (Business Metrics)

## Year-over-Year Enrollment Growth

Table: gold_yoy_enrollment - Aggregates total enrollment per year -
Calculates YoY growth percentage

## Gender & Grade Distribution

Table: gold_gender_grade_distribution - Enrollment segmented by year,
grade, gender

## Regional Comparison

Table: gold_regional_comparison - Regional enrollment by year

## School Growth Patterns

Table: gold_school_growth_patterns - Classifies schools as GROWTH /
DECLINE / STABLE

## School Performance Analysis

Table: gold_school_performance - Average performance score per school &
region

------------------------------------------------------------------------

# ⚙️ Technologies Used

-   PySpark
-   Delta Lake
-   Unity Catalog
-   Partitioning Strategy

------------------------------------------------------------------------

