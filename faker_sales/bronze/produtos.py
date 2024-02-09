# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "transient"
path_source = "faker/produtos"
container_target = "bronze"
table_target = f'faker_{path_source.split("/")[1]}'

# COMMAND ----------

df = read_json(
    container_source, 
    path_source
).select(
     col('id_produto')
    ,col('nome_produto')
    ,col('categoria_produto')
    ,col('data_cadastro')
)

# COMMAND ----------

write_table(df, container_target, table_target)
