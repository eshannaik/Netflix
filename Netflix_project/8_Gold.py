# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Notebook Lookup Tables
# MAGIC

# COMMAND ----------

dbutils.widgets.text("tableName","netflix_directors")
dbutils.widgets.text("sourceFolder","netflix_directors")

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourceFolder")
var_trg_folder = dbutils.widgets.get("tableName")

# COMMAND ----------

df = spark.read.format("delta")\
  .option("headers",True)\
  .option("inferSchema", True)\
  .load(f"abfss://silver@netflixprojecteshan.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path",f"abfss://gold@netflixprojecteshan.dfs.core.windows.net/{var_trg_folder}")\
    .save()