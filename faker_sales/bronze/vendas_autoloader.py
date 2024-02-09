# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "transient"
path_source = "faker/vendas/"
container_target = "bronze"
table_target = f'faker_{path_source.split("/")[1]}'

# COMMAND ----------

df = read_stream(container_source, f"{path_source}/*", path_source
).select(
     col('id_pedido')
    ,col('id_cliente')
    ,col('id_produto')
    ,col('id_vendedor')
    ,col('data_venda')
    ,col('preco_unitario')
    ,col('quantidade')
    ,lit(current_timestamp()).alias("dt_ingestion")
)

# COMMAND ----------

write_stream(df, container_target, table_target)
