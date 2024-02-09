# Databricks notebook source
from pyspark.sql.functions import *

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

container_source = "bronze"
table_name = "bacen_dinheiro_circulacao"
container_target = "gold"
column_partition = ["ano", "mes"]

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

# COMMAND ----------

# Escolha o modo de carga

# Parametro de batch_append
# Valor default é is_batch_append = True

dbutils.widgets.removeAll()
dbutils.widgets.text(name="is_batch_append", defaultValue="true", label="Is Append")
is_batch_append = True if str(dbutils.widgets.get("is_batch_append")).lower() in ['true', 'yes'] else False

if is_batch_append:
  batch = 'batch_append'
  print(f'Selected batch: {batch}\nFilter process: {where_clause}')
else:
  batch = 'batch_full'
  print(f'Selected batch: {batch}')

# COMMAND ----------

df = read_table(
    container_source, 
    table_name
)

if is_batch_append:
  df = df.filter(where_clause)

# COMMAND ----------

# Escrita no datalake - modos append ou full
if batch == 'batch_append':
    write_table_incremental(
        df=df, 
        schema=container_target, 
        table_name=table_name,
        where_clause=where_clause)
elif batch == 'batch_full':
    write_table(
        df=df, 
        schema=container_target, 
        table_name=table_name,
        partition=column_partition)
