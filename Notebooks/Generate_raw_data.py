-- Create Catalog
CREATE CATALOG IF NOT EXISTS education_catalog
MANAGED LOCATION 'abfss://unity-catalog-storage@dbstorage55rtdmwah6w2s.dfs.core.windows.net/7405613239087432';

-- Create Schemas (Medallion Layers)
CREATE SCHEMA IF NOT EXISTS education_catalog.education_schema;

CREATE VOLUME IF NOT EXISTS education_catalog.education_schema.raw_volume;

%python
from pyspark.sql.functions import rand, round, expr, current_timestamp

spark.sql("USE CATALOG education_catalog")
spark.sql("USE SCHEMA education_schema")

# Create 10,000 rows
df = spark.range(10000)

df = df.withColumn("school_id", (rand()*50).cast("int")) \
       .withColumn("school_name", expr("concat('School_', school_id)")) \
       .withColumn("region", expr("array('North','South','East','West')[cast(rand()*4 as int)]")) \
       .withColumn("grade", (rand()*12 + 1).cast("int")) \
       .withColumn("gender", expr("array('M','F')[cast(rand()*2 as int)]")) \
       .withColumn("year", expr("array(2020,2021,2022,2023,2024)[cast(rand()*5 as int)]")) \
       .withColumn("enrollment", (rand()*180 + 20).cast("int")) \
       .withColumn("performance_score", round(rand()*40 + 60, 2)) \
       .withColumn("ingestion_timestamp", current_timestamp()) \
       .drop("id")

# Write as CSV (Raw Layer)
df.write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("/Volumes/education_catalog/education_schema/raw_volume/education_csv")

print("✅ CSV written to Unity Catalog Volume.")
