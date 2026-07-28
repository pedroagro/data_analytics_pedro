# Configuração do Databricks

## Cenário A: Free Edition atual

1. Acesse a página de cadastro da Databricks Free Edition.
2. Crie a conta e entre no workspace.
3. Abra Workspace.
4. Importe o ZIP do projeto na sua pasta.
5. Abra `09_pipeline_diaria`.
6. Selecione Serverless no topo do notebook.
7. Execute todas as células.
8. Abra `10_demo_apresentacao` e execute para conferir os resultados.

Na Free Edition atual, o runtime do serverless é gerenciado pela Databricks. Não é necessário e normalmente não é possível selecionar manualmente 13.3 LTS.

A Free Edition pode limitar o acesso de saída à internet. Se o teste da API falhar mesmo com a URL correta, verifique as opções de liberação de acesso externo da conta ou utilize um workspace de avaliação.

## Cenário B: Workspace clássico com seleção de runtime

1. Abra Compute.
2. Clique em Create compute.
3. Use o nome `startupcrm_compute`.
4. Em Databricks Runtime Version, escolha 13.3 LTS ou superior.
5. Mantenha Python e Scala padrões do runtime.
6. Use um nó pequeno para este teste.
7. Crie o compute.
8. Aguarde Running.
9. Importe o ZIP do projeto.
10. Abra `09_pipeline_diaria`.
11. Anexe o notebook ao compute.
12. Execute Run all.

## Verificação inicial

Execute `00_config_utils`.

A saída deve mostrar o catálogo atual, se Unity Catalog está ativo, a URL da API e os nomes completos dos objetos.

Depois execute `01_setup`.

A mensagem final deve ser `Schemas e tabelas criados com sucesso.`

## Execução completa

Execute `09_pipeline_diaria`.

A última mensagem deve ser `Pipeline diária StartupCRM finalizada com sucesso.`

## Conferência dos resultados

Execute `10_demo_apresentacao`.

Os volumes esperados do snapshot atual são:

Landing: 10 usuários, 100 posts e 200 tarefas.

Bronze: 10 usuários, 100 posts e 200 tarefas.

Silver: 11 usuários, contando o membro desconhecido, 100 posts e 200 tarefas.

Gold: 10 linhas no relatório por usuário.

## Solução de problemas

### O arquivo aparece como Python comum

Apague o objeto e importe o ZIP novamente. O arquivo precisa conter `# Databricks notebook source` na primeira linha.

### Notebook not found no comando `%run`

Todos os notebooks devem estar na mesma pasta. Use os nomes importados sem a extensão `.py`, por exemplo `%run ./00_config_utils`.

### A API não responde

Teste em uma célula:

```python
import requests
response = requests.get(
    "https://jsonplaceholder.typicode.com/users",
    timeout=30,
)
print(response.status_code)
print(len(response.json()))
```

O status esperado é 200 e a quantidade esperada é 10.

### Falha de permissão ao criar schemas

Use sua pasta e seu catálogo padrão. O utilitário detecta automaticamente o catálogo atual e não tenta criar um novo catálogo.

### Execução repetida

A execução pode ser repetida. A Landing acumula snapshots. Bronze, Silver e Gold permanecem idempotentes para o estado atual.
