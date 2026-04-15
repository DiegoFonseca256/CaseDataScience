# Hipótese Capital

Plataforma de análise fundamentalista de ações listadas na B3, com geração de relatórios assistidos por LLM e dashboard interativo.

## Visão Geral

O projeto automatiza o fluxo completo de pesquisa de ações — da coleta de dados à geração de análise qualitativa — em três etapas:

1. **Coleta de dados** — Indicadores fundamentalistas (P/L, ROE, Dividend Yield, Beta, etc.), dados cadastrais e notícias relevantes são obtidos via `yfinance` e `NewsAPI`.
2. **Armazenamento SQL** — Todos os dados coletados são persistidos em um banco de dados SQL local (`database.py`), permitindo consultas rápidas, histórico de análises e reutilização entre execuções sem necessidade de reprocessamento.
3. **Análise com LLM** — Um modelo rodado na Groq (Llama 3) recebe os dados estruturados e produz um relatório com avaliação do negócio, interpretação dos indicadores, classificação de sentimento das notícias e perguntas para investigação adicional.
4. **Dashboard interativo** — Interface Streamlit com métricas, gráficos de preço históricos, cards de notícias com imagens e análise da IA consolidada.

## Arquitetura

| Módulo | Responsabilidade |
|---|---|
| `main.py` | Pipeline principal: coleta dados de mercado e cadastro, busca notícias, trata o DataFrame, invoca a LLM e exporta o CSV consolidado |
| `LLM.py` | Comunicação com a Groq, construção de prompts, parsing de JSON e geração de relatórios por ticker |
| `dashboard.py` | Aplicação Streamlit que lê o CSV gerado e exibe indicadores, gráficos e análises |

## Pré-requisitos

- Python 3.10+
- Conta gratuita na [Groq](https://console.groq.com) para obtenção da API key
- Conta gratuita na [NewsAPI](https://newsapi.org) para busca de notícias (opcional)

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração

Crie um arquivo `.env` na raiz do projeto:

```
GROQ_API_KEY=sua_chave_aqui
NEWS_API_KEY=sua_chave_aqui        # opcional
LLM_MODEL=llama-3.3-70b-versatile  # modelo padrão
```

## Uso

### 1. Executar o pipeline de coleta e análise

```bash
python main.py
```

O script gera o arquivo `empresas_com_analise.csv` com os dados brutos e os relatórios da LLM.

### 2. Abrir o dashboard

```bash
streamlit run dashboard.py
```

## Indicadores coletados

- **Cotação:** preço atual, variação diária, market cap, beta
- **Fundamentalistas:** P/L, ROE, Dívida/Patrimônio, Dividend Yield, Free Cash Flow, Margem EBITDA, Margem Operacional, Liquidez Corrente
- **Range 52 semanas:** mínima e máxima com barra de posição relativa
- **Notícias:** até 5 artigos recentes com classificação de sentimento pela IA

## Stack

- **Dados:** `yfinance`, `requests`, `pandas`
- **Banco de dados:** SQL para armazenamento e persistência dos dados coletados (`database.py`)
- **IA:** `groq` (Llama 3), `tenacity` (retry com backoff exponencial)
- **Dashboard:** `streamlit`, `plotly`
- **Config:** `python-dotenv`
