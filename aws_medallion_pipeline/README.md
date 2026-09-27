# AWS Medallion Data Pipeline

Projeto de Engenharia de Dados focado na construção de um pipeline completo em AWS utilizando arquitetura em camadas.

## Objetivo

Construir uma solução de dados escalável, rastreável e orientada a boas práticas de Engenharia de Dados.

O pipeline será responsável por:

1. Consumir dados de uma API REST
2. Persistir os dados no Amazon S3
3. Organizar os dados em camadas
4. Processar dados com Apache Spark
5. Aplicar regras de negócio
6. Criar visões analíticas
7. Otimizar dados para consumo
8. Catalogar os datasets
9. Permitir consultas analíticas
10. Implementar observabilidade e qualidade

## Arquitetura

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
EMR + Apache Spark
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