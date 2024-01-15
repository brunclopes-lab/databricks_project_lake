# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

# DBTITLE 1,Leitura da tabela sales
df_sales_order_header = read_silver('fact_orders')

# COMMAND ----------

df_final = df_sales_order_header.withColumn("insert_date", date_format(current_timestamp() - expr("INTERVAL 3 HOURS"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))\
                                # .withColumn('mes', month(col('order_date')))\
                                # .withColumn('ano', year(col('order_date')))\
                                # .orderBy('ano', 'mes', ascending=False)

# COMMAND ----------

load_gold_fact_full(df_final, 'fact_orders', 'order_date')
