# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

# DBTITLE 1,Leitura da tabela customer
df_customer = read_bronze('customer').select(
     col('CustomerID').alias("id_customer")
    ,col('Title').alias('title')
    ,concat_ws(' ', col('FirstName'), col('MiddleName'), col('LastName')).alias('name')
    ,col('CompanyName').alias('company_name')
    ,col('EmailAddress').alias('email')
    ,col('Phone').alias('phone')
    ,col('insert_date')
)

# COMMAND ----------

# DBTITLE 1, Leitura da tabela customeraddress para trazer o id_address
df_customer_address = read_bronze('customeraddress').select(
     col('CustomerID').alias('id_customer')
    ,col('AddressID').alias('id_address')
    ,col('AddressType').alias('type_address')
)

# Criando ranking a partir do customer id 
windowSpec = Window.orderBy("type_address").partitionBy("id_customer")
df_rank = df_customer_address.withColumn("rank", row_number().over(windowSpec)).filter(col('rank') == 1).drop('rank')

# Join para trazer o id_address
df_join_customer_address = df_customer.join(df_rank, on=[df_customer.id_customer == df_rank.id_customer], how='left').drop(df_rank.id_customer)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela address e join para trazer os dados de endereço
df_address = read_bronze('address').select(
     col('AddressID').alias('id_address')
    ,col('AddressLine1').alias('address_line_1')
    ,col('AddressLine2').alias('address_line_2')
    ,col('City').alias('city')
    ,col('StateProvince').alias('state_province')
    ,col('CountryRegion').alias('country')
    ,col('PostalCode').alias('postal_code')
)

df_join_address = df_join_customer_address.join(df_address, on=[df_join_customer_address.id_address == df_address.id_address], how='left').drop(df_address.id_address)

# COMMAND ----------

df_final = df_join_address.withColumn("insert_date", date_format(current_timestamp() - expr("INTERVAL 3 HOURS"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

# COMMAND ----------

load_silver(df_final, 'dim_customer')
