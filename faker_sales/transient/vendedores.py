# Databricks notebook source
from faker import Faker
from datetime import date

fake = Faker("pt_BR")

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

path_folder = 'faker'
folder_name = "vendedores"
mode = "overwrite"

path = f"{path_folder}/{folder_name}"

# COMMAND ----------

vendedores = []

for i in range(1, 41):
    vendedor = {
         'id_vendedor': i
        ,'primeiro_nome': fake.unique.first_name()
        ,'ultimo_nome': fake.unique.last_name()
        ,'email': fake.email()
        ,'endereco': fake.street_address()
        ,'bairro': fake.bairro()
        ,'cidade': fake.city()
        ,'estado': fake.state()
        ,'pais': fake.current_country()
        ,'numero_telefone': fake.phone_number()
        ,'data_admissao': fake.date_time_between(start_date=date(2020, 1, 1), end_date=date(2022, 1, 1)).strftime('%Y-%m-%d')
    }

    vendedores.append(vendedor)

# COMMAND ----------

df_vendedores = spark.createDataFrame(vendedores)

# COMMAND ----------

write_json_transient(
    df=df_vendedores, 
    folder_name=path,
    mode=mode
)
