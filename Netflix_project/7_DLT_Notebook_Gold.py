# Databricks notebook source
# MAGIC %md
# MAGIC ## DLT Notebook Gold Layer

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

looktables_rules = {
    "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

# dbutils.widgets.text("tableName","")
# dbutils.widgets.text("sourceFolder","")

# COMMAND ----------

# TableName = dbutils.widgets.get("tableName")
# SourceFolder = dbutils.widgets.get("sourceFolder")

# COMMAND ----------

@dlt.table(
  name = "gold_netflixdirectors"
)
@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
  df = spark.readStream.format("delta").load(f"abfss://silver@netflixprojecteshan.dfs.core.windows.net/netflix_directors")

  return df

# COMMAND ----------

@dlt.table(
  name = "gold_netflixcountries"
)
@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
  df = spark.readStream.format("delta").load(f"abfss://silver@netflixprojecteshan.dfs.core.windows.net/netflix_countries")

  return df

# COMMAND ----------

@dlt.table(
  name = "gold_netflixcast"
)
@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
  df = spark.readStream.format("delta").load(f"abfss://silver@netflixprojecteshan.dfs.core.windows.net/netflix_cast")

  return df

# COMMAND ----------

@dlt.table(
  name = {"gold_netflixcategory"}
)
@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
  df = spark.readStream.format("delta").load(f"abfss://silver@netflixprojecteshan.dfs.core.windows.net/netflix_category")

  return df

# COMMAND ----------

@dlt.table

def gold_stg_netflixtitles():
      df = spark.readStream.format("delta").load("abfss://silver@netflixprojecteshan.dfs.core.windows.net/net_titles")

# COMMAND ----------

@dlt.view

def gold_trns_netflixtitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")

    df = df.withColumn("new_flag",lit(1))

    return df

# COMMAND ----------

masterdata_rules = {
    "rule1" : "newflag is NOT NULL",
    "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles():
    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")

    return df