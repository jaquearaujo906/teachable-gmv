# Pipeline de Dados de GMV Diário – Case de Engenharia de Dados Teachable

Este projeto implementa um pipeline ETL de ponta a ponta para calcular o GMV Diário (Gross Merchandise Value) por subsidiária a partir de três tabelas de origem baseadas em eventos.

A solução foi projetada com boas práticas de produção em mente, utilizando a Arquitetura Medallion (Bronze / Silver / Gold), com suporte a processamento incremental, deduplicação, tratamento de dados com chegada tardia e saída analítica particionada.

O resultado final é uma tabela analítica fácil de consultar por analistas e usuários de negócio, sem a necessidade de joins complexos ou conhecimento da estrutura bruta de eventos.

---

## Estrutura do Projeto

```
teachable-gmv/
├── src/
│   └── etl_gmv.py
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│       └── daily_gmv/
├── sql/
│   ├── daily_gmv_ddl.sql
│   └── daily_gmv_query.sql
├── ARCHITECTURE.md
├── README.md
├── requirements.txt
└── .gitignore
```
---

---

## Entendimento dos Dados

O pipeline processa três tabelas de origem:

purchase  
Contém eventos do ciclo de vida da compra, incluindo purchase_id, transaction_datetime, order_date, release_date (confirmação de pagamento) e subsidiária.

product_item  
Contém informações de compra em nível de produto, como purchase_id, product_id, item_quantity e purchase_value.

purchase_extra_info  
Contém atributos adicionais relacionados à compra.

Todas as tabelas são unidas utilizando o campo:

purchase_id

---

## Lógica de Negócio do GMV

Apenas compras pagas são incluídas no cálculo do GMV.

Uma compra é considerada válida quando:

release_date IS NOT NULL

Fórmula do GMV:

GMV = purchase_value * item_quantity

---

## Tabela Analítica Final

Colunas:

transaction_date (DATE)  
subsidiary (STRING)  
gmv (DOUBLE)

Granularidade:

Uma linha por transaction_date e subsidiary.

Partição:

transaction_date

---

## Resumo da Lógica ETL

O pipeline executa os seguintes passos:

1. Leitura dos arquivos CSV brutos da camada Bronze  
2. Normalização e conversão dos tipos de dados  
3. Resolução de eventos CDC utilizando funções de janela  
4. Deduplicação por purchase_id mantendo o evento mais recente  
5. Escrita dos dados limpos na camada Silver  
6. Join das três tabelas Silver  
7. Filtro apenas de compras pagas  
8. Cálculo do GMV  
9. Agregação por dia e subsidiária  
10. Escrita da saída particionada na camada Gold  

---

## Reprodutibilidade e Processamento Incremental

O pipeline suporta execução utilizando uma janela de datas:

--start-date  
--end-date  

Exemplo:

python src/etl_gmv.py --start-date 2023-02-01 --end-date 2023-02-28

Apenas as partições afetadas são recalculadas, permitindo reprocessamento seguro sem duplicação de dados.

---

## Tratamento de Problemas de Dados

Duplicatas são tratadas utilizando:

row_number() over (partition by purchase_id order by transaction_datetime desc)

Apenas o evento mais recente é mantido.

Dados com chegada tardia são tratados por meio do reprocessamento apenas das partições de data impactadas, mantendo a tabela append-only no nível de partição.

---

## Entregáveis em SQL

O projeto inclui uma pasta sql contendo:

daily_gmv_ddl.sql – definição da tabela final para Data Warehouses e Tabelas Externas  
daily_gmv_query.sql – query para recuperar o GMV diário por subsidiária  

---

## Exemplo de Query SQL

SELECT  
    transaction_date,  
    subsidiary,  
    gmv  
FROM analytics.daily_gmv  
ORDER BY transaction_date, subsidiary;

---

## Exemplo de Saída

transaction_date | subsidiary | gmv  
2023-02-08 | internacional | 554.50  
2023-02-18 | nacional | 778.55  
2023-02-23 | internacional | 505.50  

---

## Orquestração em Produção
- Pipeline agendado diariamente via Airflow
- Alerta caso o GMV caia >20% em relação ao dia anterior
- Reprocessamento automático em caso de falha

---

## Arquitetura

Uma descrição detalhada da arquitetura está disponível em ARCHITECTURE.md e inclui:

Design das camadas de dados  
Estratégia de CDC  
Processamento incremental  
Tratamento de dados tardios  
Estratégia de monitoramento  
Abordagem de migração para cloud  

---

## Como Executar Localmente

1. Crie um ambiente virtual  
2. Instale as dependências:

pip install -r requirements.txt

3. Execute o pipeline:

python src/etl_gmv.py

Ou com janela incremental:

python src/etl_gmv.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD

---

## Tecnologias Utilizadas

Python  
Apache Spark (PySpark)  
SQL  
Sistema de arquivos local (simulação de Data Lake)  

---

## Cobertura dos Critérios de Avaliação

Esta solução contempla:

Lógica correta de GMV  
Joins e agregações adequadas  
Modelagem de dados analítica  
Tabela final particionada  
Código limpo e legível  
Pipeline reprodutível  
Tratamento de duplicatas  
Suporte a dados com chegada tardia  
DDL SQL e query analítica  
Documentação de arquitetura  

---

## Autora

Jaqueline Araujo Xavier  
Engenheira de Dados
