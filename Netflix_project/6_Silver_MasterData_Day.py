# Databricks notebook source
# MAGIC %md
# MAGIC ### getting output from another notebook

# COMMAND ----------

var = dbutils.jobs.taskValues.get(taskKey="WeekdayLookup",key="weekoutput")
print(var)