# databricks_project_lake

O repositório contém alguns pequenos projetos realizados no workspace do Databricks, utilizando algumas features, como: 
- Auto Loader
- Delta Live Tables
- Unity Catalog

Dentre os projetos criados, temos dados obtidos de API, banco de dados relacional e também a lib Faker do Python, que simula dados fictícios. 
As ingestões foram orquestradas utilizando os Worflows do Databricks, e a ingestão foi feita em tabelas externas, no Data Lake do Azure. 
O que envolve leitura e escrita foi definido por funções, que realizam a leitura e ingestão de tabela ou arquivo, para facilitar os ajustes de código, seguindo as boas práticas de engenharia de software.

![image](https://github.com/brunclopes-lab/databricks_project_lake/assets/156497494/0b510b08-2f15-4af6-a5ce-578af2c08dcb)

Todos os recursos criados, metastore, catálogos, external locations e os clusters (do tipo All-purpose e SQL warehouse) foram criados utilizando o Terraform, ferramenta de 
infra as code.
