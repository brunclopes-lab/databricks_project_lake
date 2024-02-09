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
    print("dados salvos na camada transient")

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

def write_table(df, schema, table_name, partition):
    container_target = f"abfss://{schema}@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    deltaFile = f"{container_target}/{table_name}"
    df.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "True")\
    .partitionBy(partition)\
    .saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")

# COMMAND ----------

def write_table_incremental(df, schema, table_name, where_clause):
    container_target = f"abfss://{schema}@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    deltaFile = f"{container_target}/{table_name}"
    df.write.mode("overwrite")\
    .format("delta")\
    .option("mergeSchema", "True")\
    .option("replaceWhere", where_clause)\
    .saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Replace_where sendo feito com a condição {where_clause}")
    print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")
