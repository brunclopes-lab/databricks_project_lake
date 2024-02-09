# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "bronze"
container_target = "gold"
table_target = "faker_vendas"

# COMMAND ----------

df = read_table(container_source, table_target
).select(
     col('id_pedido')
    ,col('id_cliente')
    ,col('id_produto')
    ,col('id_vendedor')
    ,col('data_venda')
    ,col('preco_unitario')
    ,col('quantidade')
    ,col("dt_ingestion")
)

# COMMAND ----------

write_table(df, container_target, table_target)
