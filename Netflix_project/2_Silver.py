# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook Lookup Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

dbutils.widgets.text("sourceFolder","netflix_directors")
dbutils.widgets.text("targetFolder","netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourceFolder")
var_trg_folder = dbutils.widgets.get("targetFolder")

# COMMAND ----------

df = spark.read.format("csv")\
  .option("headers",True)\
  .option("inferSchema", True)\
  .load(f"abfss://bronze@netflixprojecteshan.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path",f"abfss://silver@netflixprojecteshan.dfs.core.windows.net/{var_trg_folder}")\
    .save()