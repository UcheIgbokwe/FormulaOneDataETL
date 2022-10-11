# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True),
])

# COMMAND ----------

circuit_df = spark.read \
.option("header", True) \
.schema(circuit_schema) \
.csv("/mnt/formuladluche/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"), col("circuitRef"),col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date as column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuit_final_df = circuit_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet("/mnt/formuladluche/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formuladluche/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/formuladluche/processed/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------


