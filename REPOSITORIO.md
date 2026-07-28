# Publicação do repositório

## Conteúdo versionado

O repositório utiliza a branch principal `main` e a versão inicial `v1.0.0`.

O histórico foi separado por responsabilidade arquitetural:

1. Documentação e estrutura inicial.
2. Configuração compartilhada e criação dos schemas.
3. Ingestão da camada Landing.
4. Transformação da camada Bronze.
5. Modelagem da camada Silver.
6. Relatórios da camada Gold.
7. Validações de qualidade.
8. Orquestração, demonstração e reset do ambiente.
9. Documentação da versão 1.0.0.

## Publicar no GitHub

Crie um repositório vazio no GitHub chamado `startupcrm-lakehouse`.

Dentro da pasta clonada, execute:

```bash
git remote add origin https://github.com/SEU_USUARIO/startupcrm-lakehouse.git
git push -u origin main
git push origin v1.0.0
```

Caso utilize SSH:

```bash
git remote add origin git@github.com:SEU_USUARIO/startupcrm-lakehouse.git
git push -u origin main
git push origin v1.0.0
```

## Clonar a partir do bundle entregue

```bash
git clone startupcrm_lakehouse_v1.0.0.bundle startupcrm_lakehouse
cd startupcrm_lakehouse
git log --oneline --decorate --graph
```

## Conferências antes da avaliação

```bash
git status
git log --oneline --decorate --graph --all
git tag
git show v1.0.0
```
