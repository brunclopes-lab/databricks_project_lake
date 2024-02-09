# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
import dlt 

# COMMAND ----------

path_vendas_json = "abfss://transient@dlsprojlakehousedev.dfs.core.windows.net/faker/vendas/*"
path_clientes_json = "abfss://transient@dlsprojlakehousedev.dfs.core.windows.net/faker/clientes"
path_vendedores_json = "abfss://transient@dlsprojlakehousedev.dfs.core.windows.net/faker/vendedores"
path_produtos_json = "abfss://transient@dlsprojlakehousedev.dfs.core.windows.net/faker/produtos"

# COMMAND ----------

# DBTITLE 1,criação de tabelas na bronze 
# vendas
@dlt.table(
    comment="json com dados de vendas da transient para a silver",
    table_properties={"quality": "bronze"}
)
def bronze_vendas():
    return (spark.read.format("json").load(path_vendas_json))

# vendedores
@dlt.table(
    comment="json com dados de vendedores da transient para a silver",
    table_properties={"quality": "bronze"}
)
def bronze_vendedores():
    return (spark.read.format("json").load(path_vendedores_json))

# produtos
@dlt.table(
    comment="json com dados de produtos da transient para a silver",
    table_properties={"quality": "bronze"}
)
def bronze_produtos():
    return (spark.read.format("json").load(path_produtos_json))

# clientes
@dlt.table(
    comment="json com dados de clientes da transient para a silver",
    table_properties={"quality": "bronze"}
)
def bronze_clientes():
    return (spark.read.format("json").load(path_clientes_json))

# COMMAND ----------

# DBTITLE 1,criação de view
@dlt.view(comment="selecionando colunas")
def vendas_transform():
    return (
        dlt.read("bronze_vendas").select(
             col('id_pedido')
            ,col('id_cliente')
            ,col('id_produto')
            ,col('id_vendedor')
            ,col('data_venda')
            ,col('preco_unitario')
            ,col('quantidade')
            ,lit(current_timestamp()).alias("dt_ingestion")
        )
    )

@dlt.view(comment="selecionando colunas")
def vendedores_transform():
    return (
        dlt.read("bronze_vendedores").select(
             col('id_vendedor').alias("vendedor_id")
            ,col('primeiro_nome').alias("primeiro_nome_vendedor")
            ,col('ultimo_nome').alias("ultimo_nome_vendedor")
            ,col('email').alias("email_vendedor")
            ,col('endereco').alias("endereco_vendedor")
            ,col('bairro').alias("bairro_vendedor")
            ,col('cidade').alias("cidade_vendedor")
            ,col('estado').alias("estado_vendedor")
            ,col('pais').alias("pais_vendedor")
            ,col('numero_telefone').alias("numero_telefone_vendedor")
            ,col('data_admissao').alias("data_admissao_vendedor")
        )
    )

@dlt.view(comment="selecionando colunas")
def produtos_transform():
    return (
        dlt.read("bronze_produtos").select(
             col('id_produto').alias("produto_id")
            ,col('nome_produto')
            ,col('categoria_produto')
            ,col('data_cadastro').alias("data_cadastro_produto")
        )
    )

@dlt.view(comment="selecionando colunas")
def clientes_transform():
    return (
        dlt.read("bronze_clientes").select(
             col('id_cliente').alias("cliente_id")
            ,col('primeiro_nome').alias("primeiro_nome_cliente")
            ,col('ultimo_nome').alias("ultimo_nome_cliente")
            ,col('email').alias("email_cliente")
            ,col('endereco').alias("endereco_cliente")
            ,col('bairro').alias("bairro_cliente")
            ,col('cidade').alias("cidade_cliente")
            ,col('estado').alias("estado_cliente")
            ,col('pais').alias("pais_cliente")
            ,col('cep').alias("cep_cliente")
            ,col('numero_telefone').alias("numero_telefone_cliente")
        )
    )

# COMMAND ----------

# DBTITLE 1,criação de tabela na silver 
@dlt.table(
    comment="join das tabelas na bronze para a silver",
    table_properties={"quality": "silver"}
)
def silver_vendas():

    table_vendas = dlt.read("vendas_transform")
    table_vendedores = dlt.read("vendedores_transform")
    table_produtos = dlt.read("produtos_transform")
    table_clientes = dlt.read("clientes_transform")

    join_vendas_vendedores = table_vendas.join(
        table_vendedores, 
        on=table_vendas.id_vendedor == table_vendedores.vendedor_id, 
        how="left")
    
    join_vendas_produtos = join_vendas_vendedores.join(
        table_produtos,
        on=join_vendas_vendedores.id_produto == table_produtos.produto_id,
        how="left"
    )
    
    join_vendas_clientes = join_vendas_produtos.join(
        table_clientes,
        on=join_vendas_produtos.id_cliente == table_clientes.cliente_id,
        how="left"
    )

    result_df = join_vendas_clientes.select(
            col('id_pedido')
            ,col('id_cliente')
            ,col('id_produto')
            ,col('id_vendedor')
            ,col('data_venda')
            ,col('preco_unitario')
            ,col('quantidade')
            ,col("dt_ingestion")
            ,col("primeiro_nome_vendedor")
            ,col("ultimo_nome_vendedor")
            ,col("email_vendedor")
            ,col("endereco_vendedor")
            ,col("bairro_vendedor")
            ,col("cidade_vendedor")
            ,col("estado_vendedor")
            ,col("pais_vendedor")
            ,col("numero_telefone_vendedor")
            ,col("data_admissao_vendedor")
            ,col('nome_produto')
            ,col('categoria_produto')
            ,col("data_cadastro_produto")
            ,col("primeiro_nome_cliente")
            ,col("ultimo_nome_cliente")
            ,col("email_cliente")
            ,col("endereco_cliente")
            ,col("bairro_cliente")
            ,col("cidade_cliente")
            ,col("estado_cliente")
            ,col("pais_cliente")
            ,col("cep_cliente")
            ,col("numero_telefone_cliente")
    )

    return result_df

# COMMAND ----------

@dlt.table(
    comment="dados na gold",
    table_properties={"quality": "gold"}
)
def vendas_gold():

    get_vendas = spark.table("LIVE.silver_vendas")
    return get_vendas
