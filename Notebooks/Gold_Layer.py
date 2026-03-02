# ==========================================
# GOLD LAYER - YEAR OVER YEAR (YoY) GROWTH
# ==========================================

from pyspark.sql.functions import sum, col, round, when

# ------------------------------------------
# 1️⃣ Use Correct Catalog & Schema
# ------------------------------------------

spark.sql("USE CATALOG education_catalog")
spark.sql("USE SCHEMA education_schema")

# ------------------------------------------
# 2️⃣ Read Silver Table
# ------------------------------------------

silver_df = spark.table("silver_education1")

# ------------------------------------------
# 3️⃣ Aggregate Enrollment Per Year
# ------------------------------------------

yearly_df = (
    silver_df
    .groupBy("year")
    .agg(
        sum("enrollment").alias("total_enrollment")
    )
)

# ------------------------------------------
# 4️⃣ Create Previous Year DataFrame
#    Shift year by +1 so it can join
# ------------------------------------------

previous_year_df = (
    yearly_df
    .withColumnRenamed("total_enrollment", "previous_year_enrollment")
    .withColumn("year", col("year") + 1)
)

# ------------------------------------------
# 5️⃣ Join Current Year with Previous Year
# ------------------------------------------

yoy_df = (
    yearly_df
    .join(previous_year_df, on="year", how="left")
    .withColumn(
        "yoy_growth_percent",
        when(
            col("previous_year_enrollment").isNull(), None
        ).when(
            col("previous_year_enrollment") == 0, None
        ).otherwise(
            round(
                (
                    (col("total_enrollment") - col("previous_year_enrollment")) /
                    col("previous_year_enrollment")
                ) * 100,
                2
            )
        )
    )
    .orderBy("year")
)

# ------------------------------------------
# 6️⃣ Write Gold Table
# ------------------------------------------

yoy_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_yoy_enrollment")

print("✅ Gold YoY table created successfully (Self-Join Method).")


#-----------------------------------------------------------------------------------------#
# gold_gender_grade_distribution
#-----------------------------------------------------------------------------------

gender_grade_df = silver_df.groupBy(
    "year", "grade", "gender"
).agg(
    sum("enrollment").alias("total_enrollment")
)

gender_grade_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_gender_grade_distribution")
#-----------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------



# ==========================================
# GOLD LAYER - SCHOOL GROWTH PATTERNS
# ==========================================

from pyspark.sql.functions import sum, col, when

# ------------------------------------------
# 1️⃣ Use Correct Catalog & Schema
# ------------------------------------------

spark.sql("USE CATALOG education_catalog")
spark.sql("USE SCHEMA education_schema")

# ------------------------------------------
# 2️⃣ Read Silver Table
# ------------------------------------------

silver_df = spark.table("silver_education1")

# ------------------------------------------
# 3️⃣ Aggregate Enrollment Per School Per Year
# ------------------------------------------

school_year_df = (
    silver_df
    .groupBy("school_name", "year")
    .agg(
        sum("enrollment").alias("school_year_enrollment")
    )
)

# ------------------------------------------
# 4️⃣ Create Previous Year DataFrame
#    Shift year by +1 per school
# ------------------------------------------

previous_school_year_df = (
    school_year_df
    .withColumnRenamed("school_year_enrollment", "previous_year_enrollment")
    .withColumn("year", col("year") + 1)
)

# ------------------------------------------
# 5️⃣ Join Current Year with Previous Year
# ------------------------------------------

growth_df = (
    school_year_df
    .join(
        previous_school_year_df,
        on=["school_name", "year"],
        how="left"
    )
    .withColumn(
        "growth_status",
        when(col("previous_year_enrollment").isNull(), "NO_PREVIOUS_DATA")
        .when(col("school_year_enrollment") > col("previous_year_enrollment"), "GROWTH")
        .when(col("school_year_enrollment") < col("previous_year_enrollment"), "DECLINE")
        .otherwise("STABLE")
    )
    .orderBy("school_name", "year")
)

# ------------------------------------------
# 6️⃣ Write Gold Table
# ------------------------------------------

growth_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_school_growth_patterns")

print("✅ Gold School Growth Patterns table created successfully")


#-------------------------------------------------------------------------------------------------------
#gold_regional_comparison
#-------------------------------------------------------------------------------------------------------

regional_df = silver_df.groupBy(
    "region", "year"
).agg(
    sum("enrollment").alias("regional_enrollment")
)

regional_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_regional_comparison")

#-------------------------------------------------------------------------------------------------------
# gold_school_performance
#-------------------------------------------------------------------------------------------------------
from pyspark.sql.functions import avg

school_perf_df = silver_df.groupBy(
    "school_name", "region"
).agg(
    round(avg("performance_score"), 2).alias("avg_performance_score")
)

school_perf_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_school_performance")
