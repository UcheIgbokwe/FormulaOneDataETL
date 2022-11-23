# Databricks notebook source
# MAGIC %md # Ingest pitstops.json file - Multi Line JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

pit_stops_schema = StructType([
                    StructField('raceId', IntegerType(), False),
                    StructField('driverId', StringType(), True),
                    StructField("stop", StringType(), True),
                    StructField("lap", IntegerType(), True),
                    StructField("time", StringType(), True),
                    StructField("duration", StringType(), True),
                    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
 .schema(pit_stops_schema) \
 .option("multiLine", True) \
 .json("/mnt/formuladluche/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add new columns
# MAGIC 1. raceId renamed to race_id
# MAGIC 2. driverId renamed to driver_id
# MAGIC 3. ingestion date added

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").parquet("/mnt/formuladluche/processed/pit_stops")

# COMMAND ----------

df = spark.read.parquet("/mnt/formuladluche/processed/pit_stops")

# COMMAND ----------

display(df)
