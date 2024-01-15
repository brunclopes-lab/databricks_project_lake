# Databricks notebook source
# MAGIC %run ../functions/functions

# COMMAND ----------

for row in connector.get_tables():
    print(row)

# COMMAND ----------

for row in connector.get_tables():
    connector.read_and_load_table_bronze(row['TABLE_NAME'])
