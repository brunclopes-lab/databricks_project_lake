# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "bronze"
container_target = "gold"
table_target = "faker_produtos"

# COMMAND ----------

df = read_table(
    container_source, 
    table_target
).select(
     col('id_produto')
    ,col('nome_produto')
    ,col('categoria_produto')
    ,col('data_cadastro')
)

# COMMAND ----------

write_table(df, container_target, table_target)
