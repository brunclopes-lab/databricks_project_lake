# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

# DBTITLE 1,Leitura da tabela product
df_product = read_table('bronze', 'product').select(
     col('ProductID').alias("id_product")
    ,col('Name').alias('product_name')
    ,col('ProductNumber').alias('number_product')
    ,col('Color').alias('color')
    ,col('StandardCost').alias('standard_cost')
    ,col('ListPrice').alias('list_price')
    ,col('Size').alias('size')
    ,col('Weight').alias('weight')
    ,col('ProductCategoryID').alias('id_product_category')
    ,col('ProductModelID').alias('id_product_model')
    ,col('SellStartDate').alias('sell_start_date')
    ,col('SellEndDate').alias('sell_end_date')
    ,col('DiscontinuedDate').alias('discontinued_date')
)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela productcategory para trazer a categoria
df_product_category = read_table('bronze', 'productcategory').select(
     col('ProductCategoryID').alias('id_product_category')
    ,col('Name').alias('category_name')
)

df_join_product_category = df_product.join(df_product_category, on=[df_product.id_product_category == df_product_category.id_product_category]).drop(df_product_category.id_product_category)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela productmodel para trazer o modelo 
df_product_model = read_table('bronze', 'productmodel').select(
     col('ProductModelID').alias('id_product_model')
    ,col('Name').alias('model_name')
)

df_join_product_model = df_join_product_category.join(df_product_model, on=[df_join_product_category.id_product_model == df_product_model.id_product_model]).drop(df_product_model.id_product_model)

# COMMAND ----------

df_final = df_join_product_model.withColumn("insert_date", date_format(current_timestamp() - expr("INTERVAL 3 HOURS"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

# COMMAND ----------

load_table(df_final, 'silver', 'dim_product')
