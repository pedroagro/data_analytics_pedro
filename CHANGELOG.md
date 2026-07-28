# Changelog

Todas as mudanças relevantes deste projeto são registradas neste arquivo.

## [1.0.0] 2026-07-28

### Adicionado

1. Configuração centralizada para schemas, tabelas e funções compartilhadas.
2. Criação dos schemas Landing, Bronze, Silver e Gold.
3. Ingestão paginada da API JSONPlaceholder com tratamento de falhas HTTP.
4. Persistência histórica da camada Landing em Delta Lake.
5. Achatamento, seleção de colunas e deduplicação na camada Bronze.
6. Modelagem dimensional Silver com surrogate keys SHA 256 determinísticas.
7. Padronização de valores nulos em textos e números.
8. Dimensão de usuários e fatos de posts e tarefas.
9. Duas views analíticas Gold para os relatórios solicitados.
10. Validações de volume, duplicidade, nulidade, integridade referencial e totais.
11. Pipeline diária e notebook de demonstração para apresentação técnica.
12. Notebook opcional para limpeza completa do ambiente.
