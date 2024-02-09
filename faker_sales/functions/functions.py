# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

def write_json_transient(df, folder_name, mode):
    container_target = "abfss://transient@dlsprojlakehousedev.dfs.core.windows.net"
    df.coalesce(1)\
      .write\
      .format("json")\
      .mode(mode)\
      .save(f"{container_target}/{folder_name}/")
    print(f"Carregando o json no path {folder_name}")

# COMMAND ----------

def read_json(schema, path):
    container_source = f"abfss://{schema}@dlsprojlakehousedev.dfs.core.windows.net"
    df = spark.read.format("json").load(f"{container_source}/{path}")
    return df

# COMMAND ----------

def read_table(schema, table_name):
    container_source = f"abfss://{schema}@dlsprojlakehousedev.dfs.core.windows.net"
    df = spark.read.format("delta").load(f"{container_source}/{table_name}")
    return df

# COMMAND ----------

def write_table(df, schema, table_name):
    container_target = f"abfss://{schema}@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    deltaFile = f"{container_target}/{table_name}"
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "True").saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")

# COMMAND ----------

def read_stream(schema, path, path_checkpoint):
    container_source = f"abfss://{schema}@dlsprojlakehousedev.dfs.core.windows.net"
    df = spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "json")\
    .option("cloudFiles.inferColumnTypes", "true")\
    .option("cloudFiles.schemaLocation", f"{container_source}/{path_checkpoint}/schemas")\
    .option("maxFilesPerTrigger", "1")\
    .load(f"{container_source}/{path}")
    print("Leitura utilizando o AutoLoader")
    return df

# COMMAND ----------

def write_stream(df, schema, table_name):
    container_target = f"abfss://{schema}@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    deltaFile = f"{container_target}/{table_name}"
    df.writeStream\
    .format("delta")\
    .option("checkpointLocation", f"{container_target}/{table_name}/_checkpoint")\
    .option("maxFilesPerTrigger", "1")\
    .trigger(once=True)\
    .toTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Escrita no catálogo {catalog}, no schema {schema} e na tabela {table_name} utilizando o AutoLoader")
