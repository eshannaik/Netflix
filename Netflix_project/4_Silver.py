# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("delta")\
  .option("header",True)\
  .option("InferSchema",True)\
  .load("abfss://bronze@netflixprojecteshan.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df = df.fillna({"duration_minutes": 0,"duration_seasons": 1})
df.limit(5).display()

# COMMAND ----------

df = df.withColumn("duration_minutes", df["duration_minutes"].cast(IntegerType()))\
    .withColumn("duration_seasons", df["duration_seasons"].cast(IntegerType()))

df.printSchema()

# COMMAND ----------

df = df.withColumn("Shorttitle",split(df["title"], ":")[0])
df.limit(5).display()

# COMMAND ----------

df = df.withColumn("rating",split(df["rating"],"-")[0])
df.limit(5).display()

# COMMAND ----------

df.select(df["type"]).distinct().display()

# COMMAND ----------

df = df.withColumn("type_flag",\
    when(df["type"] == "Movie", 1)\
    .when(df["type"] == "TV Show", 2)\
    .otherwise(0))

df.limit(5).display()

# COMMAND ----------

window_spec = Window.orderBy(df["duration_minutes"].desc())

df = df.withColumn("duration_ranking",dense_rank().over(window_spec))
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# df.createOrReplaceTempView("Netflix")

# df = spark.sql('''
#           SELECT DISTINCT *, DENSE_RANK() OVER(ORDER BY duration_minutes DESC) as 'duration_ranking'
#           FROM Netflix
#           ''')

# df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------

df_vis = df.groupBy("type").agg(count("*").alias("total_count"))
df_vis.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@netflixprojecteshan.dfs.core.windows.net/netflix_titles")\
    .save()