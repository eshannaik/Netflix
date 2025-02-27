# Databricks notebook source
# MAGIC %md
# MAGIC ## Incremental Data Loading

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA netflix_catalog.net_schema

# COMMAND ----------

checkpoint_location = "abfss://silver@netflixprojecteshan.dfs.core.windows.net/checkpoint/"

# COMMAND ----------

df = spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation",checkpoint_location)\
    .load("abfss://raw@netflixprojecteshan.dfs.core.windows.net")

    #type of file -> checkpoint -> load(source)

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream\
    .option("checkpointLocation",checkpoint_location)\
    .trigger(processingTime = '10 seconds')\
    .start("abfss://bronze@netflixprojecteshan.dfs.core.windows.net/netflix_titles")

    #availableNow is bulk load, instead use processing time to run every few seconds

# COMMAND ----------

