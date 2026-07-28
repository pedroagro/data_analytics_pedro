# Arquitetura StartupCRM

## Fluxo

API REST JSONPlaceholder

Landing Delta histórica

Bronze Delta deduplicada

Silver dimensional

Gold views analíticas

Tabela de controle de qualidade

## Landing

Grava o payload completo de cada endpoint.

Adiciona `data_insercao` e `data_atualizacao`.

Executa paginação até a API retornar uma lista vazia.

Utiliza retry para falhas transitórias HTTP.

## Bronze

Seleciona a versão mais recente de cada ID.

Achata os structs de endereço, geolocalização e empresa.

Não utiliza mnemônicos de negócio.

Mantém as colunas de auditoria da Landing.

## Silver

A dimensão `dim_usuario` contém os atributos do usuário e da empresa.

A fato `fato_post` possui uma linha por post.

A fato `fato_todo` possui uma linha por tarefa.

Strings nulas ou vazias recebem `N/I`.

Inteiros nulos recebem menos um.

Booleanos nulos recebem falso.

A dimensão contém um membro desconhecido de código menos um.

As surrogate keys usam SHA 256, prefixo da entidade e código de negócio.

## Gold

A primeira view agrega posts e tarefas por usuário.

A segunda view agrega tarefas e conclusão por cidade.

Os relacionamentos usam `SRK_USR`.

Os usuários sem eventos permanecem no resultado por meio de left join.

## Qualidade

O pipeline valida:

Quantidade esperada por endpoint.

Colunas de auditoria.

Unicidade das chaves.

Ausência de nulos na Silver.

Integridade referencial.

Determinismo das surrogate keys.

Totais dos relatórios.

Faixa válida de percentual.

As validações são persistidas e qualquer reprovação interrompe a execução.
