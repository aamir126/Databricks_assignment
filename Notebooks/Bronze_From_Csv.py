%python

raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/education_catalog/education_schema/raw_volume/education_csv")

raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year") \
    .saveAsTable("education_catalog.education_schema.bronze_education1")

print("✅ Bronze table created from CSV.")
