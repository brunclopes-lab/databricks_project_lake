# Databricks notebook source
import requests 
import json
from datetime import date

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

mode = "append"
date_ingestion = date.today()

folder_name = "bacen_dinheiro_circulacao"
path = f"{folder_name}/{date_ingestion}"

# COMMAND ----------

def get_data():
    baseUrl = "https://olinda.bcb.gov.br"
    prox_indice = 0
    contador = 0 
    top = 10000
    list = []

    while True:
        url = f"{baseUrl}/olinda/servico/mecir_dinheiro_em_circulacao/versao/v1/odata/informacoes_diarias_com_categoria?$format=json&$orderby=Data%20asc&$skip={prox_indice}&$top={top}"
        result = requests.get(url)
        if result.status_code == 200:
            contador += 1
            print(f"Realizando o {contador}o request")
            infos = result.json()
            list.extend(infos['value'])
            count_list = len(list)
            print(f"A lista tem {count_list} registros.")
            if len(infos['value']) < 1:
                break
            prox_indice += top
        else: 
            print("Não foi possível realizar o request.")

    print("Request finalizado.")
    return list

# COMMAND ----------

df_bacen = spark.createDataFrame(get_data())

# COMMAND ----------

write_json_transient(
    df=df_bacen, 
    folder_name=path,
    mode=mode
)
