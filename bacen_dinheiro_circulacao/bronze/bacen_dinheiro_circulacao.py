# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "transient"
path_source = "bacen_dinheiro_circulacao"
container_target = "bronze"
column_partition = ["ano", "mes"]

# COMMAND ----------

df = read_json(
    container_source, 
    f"{path_source}/*"
).select(
     col("Categoria").alias("categoria")
    ,col('Data').alias('data')
    ,col("Denominacao").alias("denominacao")
    ,col("Especie").alias("especie")
    ,col("Quantidade").alias("qtd")
    ,col("Valor").alias("vlr")
    ,year(col("data")).alias("ano")
    ,month(col("data")).alias("mes")
).dropDuplicates()

# COMMAND ----------

# Condição para o incremental
today = datetime.now() - timedelta(days=1)
current_month = today.month

last_month_date = today
last_month = last_month_date.month
while last_month == current_month:
    last_month_date = last_month_date - timedelta(days=1)
    last_month = last_month_date.month

where_clause = "(ano = '{}' AND mes = '{}') OR (ano = '{}' AND mes = '{}')".format(
  today.year, today.month, last_month_date.year, last_month_date.month
)

print(f'Where clause: {where_clause}')

# COMMAND ----------

filter_df = df.filter(where_clause)

# COMMAND ----------

write_table_incremental(
    df=filter_df, 
    schema=container_target, 
    table_name=path_source, 
    where_clause=where_clause)
