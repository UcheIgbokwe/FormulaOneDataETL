# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest Constructors.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formuladluche/raw/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Drop unwanted columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())  

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formuladluche/processed/constructors")

# COMMAND ----------

df = spark.read.parquet("/mnt/formuladluche/processed/constructors")
