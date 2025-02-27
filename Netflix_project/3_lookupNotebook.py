# Databricks notebook source
# MAGIC %md
# MAGIC ## Array Parameter

# COMMAND ----------

files = [
  {
    "sourceFolder" : "netflix_directors",
    "targetFolder" : "netflix_directors"
  },
  {
    "sourceFolder" : "netflix_cast",
    "targetFolder" : "netflix_cast"
  },
  {
    "sourceFolder" : "netflix_countries",
    "targetFolder" : "netflix_countries"
  },
  {
    "sourceFolder" : "netflix_category",
    "targetFolder" : "netflix_category"
  }
]

# COMMAND ----------

dlt_files = [
  {
    "tableName" : "gold_netflixdirectors",
    "sourceFolder" : "netflix_directors"
  },
  {
    "tableName" : "gold_netflixcast",
    "sourceFolder" : "netflix_cast"
  },
  {
    "tableName" : "gold_netflixcountries",
    "sourceFolder" : "netflix_countries"
  },
  {
    "tableName" : "gold_netflixcategory",
    "sourceFolder" : "netflix_category"
  },
  {
    "tableName" : "gold_netflixtitles",
    "sourceFolder" : "netflix_titles"
  }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Utility to return the Array

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "my_arr", value = files)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="DLT_var",value=dlt_files)