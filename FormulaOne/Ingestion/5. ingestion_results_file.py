# Databricks notebook source
# MAGIC %md # Ingest results.json file - Single Line JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType([
                    StructField('resultId', IntegerType(), False),
                    StructField('raceId', IntegerType(), False),
                    StructField('driverId', IntegerType(), False),
                    StructField('constructorId', IntegerType(), False),
                    StructField('number', IntegerType(), True),
                    StructField('grid', IntegerType(), False),
                    StructField('position', IntegerType(), True),
                    StructField('positionText', StringType(), False),
                    StructField('positionOrder', IntegerType(), False),
                    StructField('points', FloatType(), False),
                    StructField('laps', IntegerType(), False),
                    StructField('time', StringType(), True),
                    StructField('milliseconds', IntegerType(), True),
                    StructField('fastestLap', IntegerType(), True),
                    StructField('rank', IntegerType(), True),
                    StructField('fastestLapTime', IntegerType(), True),
                    StructField('fastestLapSpeed', StringType(), True),
                    StructField('statusId', IntegerType(), False)
])


# COMMAND ----------

results_df = spark.read \
 .schema(results_schema) \
 .json("/mnt/formuladluche/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted column
# MAGIC 1. statusId - Dropped

# COMMAND ----------

results_dropped_df = results_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns and add a new column
# MAGIC 1. ingestion date - added

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

results_final_df = results_dropped_df.withColumnRenamed("resultId", "result_id") \
                                            .withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("positionText", "position_text") \
                                            .withColumnRenamed("positionOrder", "position_order") \
                                            .withColumnRenamed("fastestLap", "fastest_lap") \
                                            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                            .withColumn("ingestion_date", current_timestamp())
                                    

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to processed container in parquet format and partition by Race Id

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/formuladluche/processed/results")

# COMMAND ----------

df = spark.read.parquet("/mnt/formuladluche/processed/results")

# COMMAND ----------

display(df)
