# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "bronze"
container_target = "gold"
table_target = "faker_vendedores"

# COMMAND ----------

df = read_table(
    container_source, 
    table_target
).select(
     col('id_vendedor')
    ,col('primeiro_nome')
    ,col('ultimo_nome')
    ,col('email')
    ,col('endereco')
    ,col('bairro')
    ,col('cidade')
    ,col('estado')
    ,col('pais')
    ,col('numero_telefone')
    ,col('data_admissao')
)

# COMMAND ----------

write_table(df, container_target, table_target)
