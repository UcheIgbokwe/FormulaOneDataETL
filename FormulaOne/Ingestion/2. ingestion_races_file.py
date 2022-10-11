# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", TimestampType(), True),
                                    StructField("time", TimestampType(), True),
                                    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formuladluche/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select the required columns

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
from pyspark.sql.functions import *

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"),col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add the columns as required

# COMMAND ----------

races_added_df = races_selected_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Rename the columns as required

# COMMAND ----------

races_renamed_df = races_added_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

races_final_df = races_renamed_df.select(col("race_id"), col("race_year"),col("round"), col("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/formuladluche/processed/races")

# COMMAND ----------

df = spark.read.parquet("/mnt/formuladluche/processed/races")
