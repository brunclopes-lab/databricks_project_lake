# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "transient"
path_source = "faker/clientes"
container_target = "bronze"
table_target = f'faker_{path_source.split("/")[1]}'

# COMMAND ----------

df = read_json(
    container_source, 
    path_source
).select(
     col('id_cliente')
    ,col('primeiro_nome')
    ,col('ultimo_nome')
    ,col('email')
    ,col('endereco')
    ,col('bairro')
    ,col('cidade')
    ,col('estado')
    ,col('pais')
    ,col('cep')
    ,col('numero_telefone')
)

# COMMAND ----------

write_table(df, container_target, table_target)
