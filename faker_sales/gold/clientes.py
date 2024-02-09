# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "bronze"
container_target = "gold"
table_target = "faker_clientes"

# COMMAND ----------

df = read_table(
    container_source, 
    table_target
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
