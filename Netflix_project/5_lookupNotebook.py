# Databricks notebook source
dbutils.widgets.text("weekday","7")
var = int(dbutils.widgets.get("weekday"))
print(var)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="weekoutput",value=var)