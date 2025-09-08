# Dagster Workflow Configurations

Este diretório contém quatro diferentes configurações de workflows Dagster, cada uma com um propósito específico.

---

## Nota: Erro durante a Live

Durante a live, ao criar um arquivo similar ao definitions.py, cometi alguns erros e pelo nervosismo de estar ao vivo, e para não quebrar a dinâmica da live realizando debugging, segui adiante. Mas aqui estão os erros que fizeram com que o código não rodasse:

Os 2 erros se referiam ao Job e eram:
1. A importação correta é "define_asset_job" e digitei "definition_asset_job"
2. Além disso, a definição do job estava errada, deveria passar "name" e "selection" adequadamente.

---

## Arquivos Disponíveis

### `definitions.completo.py`
Uma implementação completa e robusta de um workflow de geração de vídeo que inclui:
- Integração com OpenAI para geração de texto
- Integração com Google Cloud para Text-to-Speech
- Integração com AWS S3 para armazenamento
- Pipeline completo: tema → frase → áudio → vídeo → merge → metadata
- Schedule configurado para execução diária às 10h
- Tratamento de erros e retry logic
- (falta adicionar!) Monitoramento completo via metadata

### `definitions.hackernews.py`
Um workflow especializado para análise de dados do HackerNews:
- Coleta de histórias do HackerNews API
- Geração de word cloud baseada nas histórias
- Análise de dados e visualização -> possui Metadata
- Pipeline otimizado para processamento de dados em lote

### `definitions.py`
Uma implementação básica para demonstração e aprendizado:
- Estrutura simplificada do Dagster
- Usa dados simulados (strings fixas)
- Ideal para testes e prototipação
- Excelente para entender os conceitos básicos do Dagster

### `definitions.simples.py`
Uma versão minimalista do workflow com:
- Implementação básica dos conceitos do Dagster
- Configuração mínima necessária
- Útil para testes rápidos
- Fácil de entender e modificar

## Como Usar

Escolha o arquivo de configuração apropriado baseado no seu caso de uso:

1. Para produção com geração completa de vídeo:
```bash
dagster dev -f definitions.completo.py
```

2. Para análise de dados do HackerNews:
```bash
dagster dev -f definitions.hackernews.py
```

3. Para testes e desenvolvimento:
```bash
dagster dev -f definitions.py
```

4. Para uma demonstração simples:
```bash
dagster dev -f definitions.simples.py
```

## Pré-requisitos

Dependendo do workflow escolhido, você precisará:

- Python 3.8+
- Dagster
- OpenAI API Key (para definitions.completo.py)
- Google Cloud credentials (para definitions.completo.py)
- AWS S3 credentials (para definitions.completo.py)

## Estrutura dos Assets

Cada configuração segue a estrutura padrão do Dagster com assets bem definidos e dependências claras entre eles (não é necessário criar um DAG explícito como no Airflow, o próprio Dagster infere o DAG a partir das dependências de cada asset!). Os metadados são utilizados para monitoramento e observabilidade do pipeline.

## Monitoramento

Todos os workflows podem ser monitorados através da interface Dagit, que fornece:
- Visualização do DAG
- Status de execução
- Logs detalhados
- Metadados de performance
- Histórico de execuções

