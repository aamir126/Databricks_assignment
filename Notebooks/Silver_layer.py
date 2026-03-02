%python
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp
)

# Use correct catalog & schema
spark.sql("USE CATALOG education_catalog")
spark.sql("USE SCHEMA education_schema")

# Read Bronze
bronze_df = spark.table("bronze_education1")

# ----------------------------
# SILVER TRANSFORMATION
# ----------------------------

silver_df = (
    bronze_df

    # Remove invalid enrollments
    .filter(col("enrollment") > 0)

    # Standardize text columns
    .withColumn("school_name", upper(trim(col("school_name"))))
    .withColumn("region", upper(trim(col("region"))))

    # Standardize gender
    .withColumn(
        "gender",
        when(col("gender") == "M", "MALE")
        .when(col("gender") == "F", "FEMALE")
        .otherwise("UNKNOWN")
    )

    # Ensure grade between 1 and 12
    .filter((col("grade") >= 1) & (col("grade") <= 12))

    # Add derived academic_category
    .withColumn(
        "academic_category",
        when(col("performance_score") >= 85, "EXCELLENT")
        .when(col("performance_score") >= 70, "GOOD")
        .otherwise("NEEDS_IMPROVEMENT")
    )

    # Add processing timestamp
    .withColumn("silver_processed_timestamp", current_timestamp())
)

# ----------------------------
# WRITE SILVER TABLE
# ----------------------------

silver_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("year") \
    .saveAsTable("silver_education1")

print("✅ Silver table created successfully.")
