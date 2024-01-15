# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

# DBTITLE 1,Leitura da tabela customer
df_customer = read_silver('dim_customer')

# COMMAND ----------

df_final = df_customer.withColumn("insert_date", date_format(current_timestamp() - expr("INTERVAL 3 HOURS"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

# COMMAND ----------

load_gold(df_final, 'dim_customer')
