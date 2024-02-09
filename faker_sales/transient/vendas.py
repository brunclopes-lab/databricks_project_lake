# Databricks notebook source
from faker import Faker 
from datetime import date, timedelta
import random

fake = Faker()

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

path_folder = 'faker'
folder_name = "vendas"
mode = "append"
date_ingestion = date.today()


path = f"{path_folder}/{folder_name}/{folder_name}_{date_ingestion}"

# COMMAND ----------

vendas = []
# Gerando lista de vendas aleatórias
for i in range(1, 20001):
    venda = {
         'id_pedido': random.randint(10**9, 10**10 - 1)
        ,'id_cliente': random.randint(1, 150)
        ,'id_produto': random.randint(1, 100)
        ,'id_vendedor': random.randint(1, 30)
        ,'data_venda': fake.date_time_between(start_date=date.today(), end_date=date.today()).strftime('%Y-%m-%d')
        ,'preco_unitario': fake.pyfloat(left_digits=2, right_digits=2, positive=True, min_value=5.00, max_value=29.99)
        ,'quantidade': fake.pyint(min_value=1, max_value=30)

    }

    vendas.append(venda)

# COMMAND ----------

df_vendas = spark.createDataFrame(vendas)

# COMMAND ----------

write_json_transient(
    df=df_vendas, 
    folder_name=path,
    mode=mode
)
