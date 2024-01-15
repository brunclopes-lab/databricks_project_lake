# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

# DBTITLE 1,Leitura da tabela product
df_product = read_silver('dim_product')

# COMMAND ----------

df_final = df_product.withColumn("insert_date", date_format(current_timestamp() - expr("INTERVAL 3 HOURS"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

# COMMAND ----------

load_gold(df_final, 'dim_product')
