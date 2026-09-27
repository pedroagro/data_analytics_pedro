# Arquitetura AWS Medallion Data Pipeline

## Visão geral

Este projeto implementa um pipeline de Engenharia de Dados na AWS utilizando ingestão via API REST, armazenamento em Amazon S3, processamento distribuído com Apache Spark e Amazon EMR, catálogo com AWS Glue e consumo analítico via Amazon Athena.

## Fluxo

```text
API REST
    |
    v
AWS Lambda
    |
    v
S3 Landing
    |
    v
S3 Raw
    |
    v
S3 Bronze
    |
    v
Amazon EMR + Apache Spark
    |
    v
S3 Silver
    |
    v
S3 Gold
    |
    v
S3 Diamond
    |
    v
AWS Glue Data Catalog
    |
    v
Amazon Athena
    |
    v
BI / Analytics / Data Science