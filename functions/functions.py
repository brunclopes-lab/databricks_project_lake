# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

sqldb_server = dbutils.secrets.get(scope='scope-project-lake', key='sqldb-server')
sqldb_database = dbutils.secrets.get(scope='scope-project-lake', key='sqldb-database')
sqldb_user = dbutils.secrets.get(scope='scope-project-lake', key='sqldb-user')
sqldb_pass = dbutils.secrets.get(scope='scope-project-lake', key='sqldb-pass')

# COMMAND ----------

# DBTITLE 1,Leitura das tabelas no banco e carga na bronze em formato delta
class SQLServerConnector: 
    def __init__(self, server, database, user, password):
        self.server = server
        self.database = database
        self.user = user
        self.password = password

    def get_tables(self): 
        query = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        where TABLE_SCHEMA = 'SalesLT'
        AND TABLE_TYPE = 'BASE TABLE'
        """

        # Configurações JDBC
        jdbc_url = f"jdbc:sqlserver://{self.server};database={self.database}"
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        # Leitura dos dados do SQL Server para um DataFrame
        df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS tabela_alias", properties=properties)
        return df.collect()

    def read_and_load_table_bronze(self, table_name):
        # Configurações JDBC
        jdbc_url = f"jdbc:sqlserver://{self.server};database={self.database}"
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        table_name = table_name.lower()
        table_name_db = f"SalesLT.{table_name}"
        container_target = "abfss://bronze@dlsprojlakehousedev.dfs.core.windows.net"
        catalog = "master"
        schema = "bronze"

        deltaFile = f"{container_target}/{table_name}"

        # Leitura dos dados do SQL Server para um DataFrame
        df = spark.read.jdbc(url=jdbc_url, table=table_name_db, properties=properties)
        df = df.withColumn("insert_date", date_format(current_timestamp() - expr("INTERVAL 3 HOURS"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

        df.write.mode("overwrite").format("delta").option("overwriteSchema", "True").saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
        print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")

# COMMAND ----------

connector = SQLServerConnector(sqldb_server, sqldb_database, sqldb_user, sqldb_pass)

# COMMAND ----------

# DBTITLE 1,Leitura e carga nas camadas silver e gold
def read_bronze(table_name):
    container_source = "abfss://bronze@dlsprojlakehousedev.dfs.core.windows.net"
    df = spark.read.format("delta").load(f"{container_source}/{table_name}")
    return df

def load_silver(df, table_name):
    container_target = "abfss://silver@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    schema = "silver"
    deltaFile = f"{container_target}/{table_name}"
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "True").saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")

def load_silver_fact_full(df, table_name, col_partition):
    container_target = "abfss://silver@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    schema = "silver"
    deltaFile = f"{container_target}/{table_name}"
    df.write.mode("overwrite").format("delta").partitionBy(col_partition).option("overwriteSchema", "True").saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")

def read_silver(table_name):
    container_source = "abfss://silver@dlsprojlakehousedev.dfs.core.windows.net"
    df = spark.read.format("delta").load(f"{container_source}/{table_name}")
    return df

def load_gold(df, table_name):
    container_target = "abfss://gold@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    schema = "gold"
    deltaFile = f"{container_target}/{table_name}"
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "True").saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")

def load_gold_fact_full(df, table_name, col_partition):
    container_target = "abfss://gold@dlsprojlakehousedev.dfs.core.windows.net"
    catalog = "master"
    schema = "gold"
    deltaFile = f"{container_target}/{table_name}"
    df.write.mode("overwrite").format("delta").partitionBy(col_partition).option("overwriteSchema", "True").saveAsTable(f"{catalog}.{schema}.{table_name}", path=deltaFile)
    print(f"Carga da tabela {table_name} no catálogo {catalog} e no schema {schema}.")
