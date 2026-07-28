# StartupCRM Data Lakehouse

## 1. Observação sobre o ambiente gratuito

A antiga Databricks Community Edition foi substituída pela Databricks Free Edition. Na Free Edition, o compute é serverless e o runtime é gerenciado pela plataforma. Portanto, normalmente não existe uma tela para selecionar manualmente o Databricks Runtime 13.3 LTS.

Em um workspace clássico, crie um compute e selecione Databricks Runtime 13.3 LTS ou uma versão superior.

Todo o código deste projeto é compatível com Spark e Delta Lake presentes no Runtime 13.3 LTS ou superior e também com o compute serverless atual da Free Edition.

A Free Edition pode restringir o acesso de saída à internet. Caso o endpoint JSONPlaceholder seja bloqueado, conclua a verificação de identidade disponibilizada no workspace ou utilize um workspace de avaliação com acesso externo.

## 2. Como importar

1. Entre no workspace do Databricks.
2. Abra Workspace.
3. Crie ou abra sua pasta pessoal.
4. Clique no menu da pasta e escolha Import.
5. Selecione o arquivo `startupcrm_lakehouse_databricks.zip`.
6. Confirme a importação.
7. A estrutura de notebooks será recriada automaticamente.

Os arquivos `.py` deste pacote são notebooks no formato Databricks Source. Eles possuem `# Databricks notebook source` na primeira linha. Não devem ser criados como arquivos Python comuns.

## 3. Como configurar o compute

### Databricks Free Edition

1. Abra o notebook `09_pipeline_diaria`.
2. No seletor de compute no topo, escolha Serverless.
3. Aguarde o compute ficar disponível.
4. Execute Run all.

### Workspace clássico

1. Abra Compute.
2. Clique em Create compute.
3. Informe um nome, por exemplo `startupcrm_compute`.
4. Escolha Databricks Runtime 13.3 LTS ou superior.
5. Para o teste, use o menor tamanho de máquina disponível.
6. Aguarde o status Running.
7. Abra o notebook `09_pipeline_diaria`.
8. Anexe o notebook ao compute criado.
9. Execute Run all.

## 4. Ordem dos notebooks

1. `00_config_utils`
2. `01_setup`
3. `02_landing_ingestion`
4. `03_bronze`
5. `04_silver_dim_usuario`
6. `05_silver_fato_post`
7. `06_silver_fato_todo`
8. `07_gold_relatorios`
9. `08_validacoes`
10. `09_pipeline_diaria`
11. `10_demo_apresentacao`
12. `99_reset_ambiente`

Para uma execução completa, rode apenas `09_pipeline_diaria`.

Para apresentar ao time, execute depois o notebook `10_demo_apresentacao`.

## 5. Objetos criados

### Landing

`startupcrm_landing.users`

`startupcrm_landing.posts`

`startupcrm_landing.todos`

A Landing é histórica e recebe um novo snapshot a cada execução.

### Bronze

`startupcrm_bronze.users`

`startupcrm_bronze.posts`

`startupcrm_bronze.todos`

A Bronze representa o estado atual deduplicado de cada entidade.

### Silver

`startupcrm_silver.dim_usuario`

`startupcrm_silver.fato_post`

`startupcrm_silver.fato_todo`

A Silver aplica mnemônicos, nulos padronizados, membro desconhecido e surrogate keys SHA 256 determinísticas.

### Gold

`startupcrm_gold.vw_usuario_posts_tarefas`

`startupcrm_gold.vw_cidade_conclusao_tarefas`

`startupcrm_gold.ctl_validacao_pipeline`

## 6. Estratégia de atualização diária

A Landing usa append e preserva o histórico de extrações.

A Bronze seleciona a versão mais recente de cada chave e sobrescreve o snapshot atual.

A Silver é reconstruída de forma determinística a partir da Bronze.

A Gold é recriada com `CREATE OR REPLACE VIEW`.

As validações são acrescentadas à tabela de controle e a execução falha quando qualquer regra for reprovada.

## 7. Como agendar diariamente

1. Abra `09_pipeline_diaria`.
2. Clique em Schedule no canto superior direito.
3. Crie uma programação simples diária.
4. Escolha o compute disponível.
5. Salve a programação.

## 8. Decisões de arquitetura para explicar

1. A Landing preserva o payload e permite reprocessamento.
2. A Bronze resolve aninhamento e duplicidade sem aplicar nomes de negócio.
3. A Silver concentra regras de qualidade e modelagem dimensional.
4. As surrogate keys são hashes SHA 256 com prefixo da entidade e código de negócio.
5. A dimensão possui o membro desconhecido de código menos um.
6. Os fatos usam a mesma função de surrogate key da dimensão.
7. As views Gold usam surrogate keys nos relacionamentos.
8. O relatório de usuários usa left join para manter usuários com zero posts ou tarefas.
9. O relatório por cidade evita divisão por zero.
10. A pipeline falha automaticamente quando uma validação é reprovada.

## 9. Versionamento

O projeto é entregue como repositório Git na branch `main`, com histórico de commits separado por camada e tag de versão `v1.0.0`.

A API utilizada não exige autenticação e nenhuma credencial é armazenada no repositório.

Consulte `CHANGELOG.md` para as alterações da versão e `REPOSITORIO.md` para as instruções de publicação no GitHub.
