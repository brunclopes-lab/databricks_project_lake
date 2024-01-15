# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../functions/functions

# COMMAND ----------

# DBTITLE 1,Leitura da tabela sales
df_sales_order_header = read_bronze('salesorderheader').select(
     col('SalesOrderID').alias("id_sales_order")
    ,col('RevisionNumber').alias('revision_number')
    ,col('OrderDate').alias('order_date')
    ,col('DueDate').alias('due_date')
    ,col('ShipDate').alias('ship_date')
    ,col('Status').alias('status')
    ,col('OnlineOrderFlag').alias('flag_online_order')
    ,col('SalesOrderNumber').alias('number_sales_order')
    ,col('PurchaseOrderNumber').alias('number_purchase_order')
    ,col('AccountNumber').alias('number_account')
    ,col('CustomerID').alias('id_customer')
    ,col('ShipToAddressID').alias('id_ship_to_address')
    ,col('BillToAddressID').alias('id_bill_to_address')
    ,col('ShipMethod').alias('ship_method')
    ,col('SubTotal').alias('sub_total')
    ,col('TaxAmt').alias('tax_amt')
    ,col('Freight').alias('freight')
    ,col('TotalDue').alias('total_due')
)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela sales detail e join 
df_sales_order_detail = read_bronze('salesorderdetail').select(
     col('SalesOrderID').alias('id_sales_order')
    ,col('SalesOrderDetailID').alias('id_sales_order_detail')
    ,col('OrderQty').alias('qty_order')
    ,col('ProductID').alias('id_product')
    ,col('UnitPrice').alias('unit_price')
    ,col('UnitPriceDiscount').alias('unit_price_discount')
    ,col('LineTotal').alias('total')
)

df_join_sales_detail = df_sales_order_header.join(df_sales_order_detail, on=[df_sales_order_header.id_sales_order == df_sales_order_detail.id_sales_order]).drop(df_sales_order_detail.id_sales_order)

# COMMAND ----------

df_final = df_join_sales_detail.withColumn("insert_date", date_format(current_timestamp() - expr("INTERVAL 3 HOURS"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))\
                                # .withColumn('mes', month(col('order_date')))\
                                # .withColumn('ano', year(col('order_date')))\
                                # .orderBy('ano', 'mes', ascending=False)

# COMMAND ----------

load_silver_fact_full(df_final, 'fact_orders', 'order_date')
