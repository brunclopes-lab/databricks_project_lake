# Databricks notebook source
from faker import Faker 
from datetime import date, timedelta
import random

fake = Faker("pt_BR")

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

path_folder = 'faker'
folder_name = "clientes"
mode = "overwrite"

path = f"{path_folder}/{folder_name}"

# COMMAND ----------

clientes = []

for i in range(1, 71):
    cliente = {
         'id_cliente': i
        ,'primeiro_nome': fake.unique.first_name()
        ,'ultimo_nome': fake.unique.last_name()
        ,'email': fake.email()
        ,'endereco': fake.street_address()
        ,'bairro': fake.bairro()
        ,'cidade': fake.city()
        ,'estado': fake.state()
        ,'pais': fake.current_country()
        ,'cep': fake.postcode()
        ,'numero_telefone': fake.phone_number()
    }

    clientes.append(cliente)

# COMMAND ----------

df_clientes = spark.createDataFrame(clientes)

# COMMAND ----------

write_json_transient(
    df=df_clientes, 
    folder_name=path,
    mode=mode
)
