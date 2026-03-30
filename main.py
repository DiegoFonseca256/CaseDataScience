"""
main.py
Coleta dados de mercado, notícias e gera análise com LLM.

Novos campos adicionados:
  - preco_atual      → currentPrice
  - variacao_dia     → regularMarketChangePercent
  - market_cap       → marketCap
"""
import io
import json
import os
import datetime

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

from LLM import analisar_lote
from database import get_conn, init_db

load_dotenv()

LISTA_TICKERS    = ["ASAI3", "RECV3", "MOVI3", "BRKM5", "HBSA3",
                    "ITUB4", "BBDC4", "OPCT3", "BRSR6", "PRIO3"]
ARQUIVO_CADASTRO = "empresa_info_cadastro.txt"
NEWS_API_KEY     = os.getenv("NEWS_API_KEY", "")


# ---------------------------------------------------------------------------
# Data Base
# ---------------------------------------------------------------------------

def salvar_empresas_no_db(lista_tickers):

    with get_conn() as conn:
        for ticker in lista_tickers:
            try:
                # Coleta via yfinance
                info = yf.Ticker(ticker + ".SA").info
                
                # Prepara os dados conforme o schema do seu database.py
                nome = info.get("longName")
                setor = info.get("sectorDisp")
                segmento = info.get("industryDisp")
                descricao = info.get("longBusinessSummary")

                # Executa o UPSERT
                conn.execute('''
                    INSERT INTO empresas (ticker, nome, setor, segAtuacao, descricao)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(ticker) DO UPDATE SET
                        nome = excluded.nome,
                        setor = excluded.setor,
                        segAtuacao = excluded.segAtuacao,
                        descricao = excluded.descricao
                ''', (ticker, nome, setor, segmento, descricao))
                
                print(f"✅ {ticker} (Estático) salvo/atualizado no DB.")
                
            except Exception as e:
                print(f"❌ Erro ao processar dados estáticos de {ticker}: {e}")


def cria_df_dados_cadastro():
    query = "SELECT * FROM empresas"
    
    try:
        with get_conn() as conn:
            # O pandas lê a query e já fecha a conexão ao terminar
            df = pd.read_sql(query, conn)
            
        if df.empty:
            print("⚠️ A tabela 'empresas' está vazia. Rode salvar_empresas_no_db primeiro.")
            
        return df
        
    except Exception as e:
        print(f"❌ Erro ao ler dados de cadastro do banco: {e}")
        return pd.DataFrame() # Retorna um DF vazio para não quebrar o pipeline
    
def salvar_snapshot_no_db(df):

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with get_conn() as conn:
        for _, row in df.iterrows():
            ticker = row['ticker']
            
            cursor = conn.execute('''
                INSERT INTO snapshots (
                    ticker, data_coleta, preco_atual, pl, roe, dy, 
                    market_cap, beta, resumo_llm, analise_llm, perguntas_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker, 
                agora, 
                row.get('preco_atual'), 
                row.get('P/L'), 
                row.get('ROE'), 
                row.get('dividendYield'),
                row.get('market_cap'),
                row.get('Beta'),
                row.get('resumo_negocio'),
                row.get('analise_fundamentos'),
                row.get('perguntas_json')
            ))
            
            try:
                noticias_brutas = json.loads(row.get('noticias_raw_json', '[]'))
                sentimentos_ia = json.loads(row.get('noticias_json', '[]'))
                
                mapa_sentimento = {item['titulo']: item['sentimento'] for item in sentimentos_ia if 'titulo' in item}

                for noticia in noticias_brutas:
                    titulo = noticia.get('title')
                    url = noticia.get('url')
                    data_noticia = noticia.get('publishedAt')
                    sentimento = mapa_sentimento.get(titulo, "neutra")

                    conn.execute('''
                        INSERT INTO noticias_historico (
                            ticker, data_noticia, titulo, sentimento, url
                        ) VALUES (?, ?, ?, ?, ?)
                    ''', (ticker, data_noticia, titulo, sentimento, url))
            except Exception as e:
                print(f"⚠️ Erro ao processar notícias para {ticker} no DB: {e}")

    print(f"✅ Snapshot e notícias de {len(df)} tickers salvos com sucesso.")


# ---------------------------------------------------------------------------
# Notícias
# ---------------------------------------------------------------------------

def busca_noticias(ticker_nome):
    if not NEWS_API_KEY:
        print(f"⚠️  NEWS_API_KEY ausente — sem notícias para {ticker_nome}")
        return []

    url = (
        f"https://newsapi.org/v2/everything"
        f"?q={ticker_nome}&language=pt&sortBy=publishedAt&apiKey={NEWS_API_KEY}"
    )
    try:
        response = requests.get(url, timeout=10).json()
        return response.get("articles", [])[:5]
    except Exception as e:
        print(f"⚠️  Erro ao buscar notícias de {ticker_nome}: {e}")
        return []


# ---------------------------------------------------------------------------
# Mercado — inclui preco_atual, variacao_dia e market_cap
# ---------------------------------------------------------------------------

def pegar_minima_52s(ticker):
    # 1. Tenta pelo campo pronto da API
    info = yf.Ticker(ticker + ".SA").info
    minima = info.get("fiftyTwoWeekLow")
    
    # 2. Se vier 0, None ou NaN, calculamos manualmente pelo histórico
    if not minima or minima == 0:
        # Pega o histórico do último 1 ano (1y)
        hist = yf.Ticker(ticker + ".SA").history(period="1y")
        if not hist.empty:
            minima = hist['Low'].min() # Pega o menor valor da coluna 'Low'
            
    return minima

def pega_dados_mercado(lista_tickers):
    novos_dados = []

    for ticker in lista_tickers:
        try:
            info = yf.Ticker(ticker + ".SA").info
            noticias = busca_noticias(ticker)

            dados_ticker = {
                "ticker":               ticker,
                # ── Cotação e mercado ────────────────────────────────────────
                "preco_atual":          info.get("currentPrice"),
                "variacao_dia":         info.get("regularMarketChangePercent"),
                "market_cap":           info.get("marketCap"),
                # ── Indicadores fundamentalistas ─────────────────────────────
                "P/L":                  info.get("trailingPE"),
                "ROE":                  info.get("returnOnEquity"),
                "debtToEquity":         info.get("debtToEquity"),
                "dividendYield":        info.get("dividendYield"),
                "freeCashflow":         info.get("freeCashflow"),
                "ebitdaMargins":        info.get("ebitdaMargins"),
                "Beta":                 info.get("beta"),
                "Liqui Corrente":       info.get("currentRatio"),
                "Vol Med Diário":       info.get("averageVolume"),
                "Margem Operacional":   info.get("operatingMargins"),
                "Máxima 52 Semanas":    info.get("fiftyTwoWeekHigh"),
                "Mínima 52 Semanas":    pegar_minima_52s(ticker),
                # ── Notícias (lista de dicts da NewsAPI) ─────────────────────
                "noticias":             noticias,
            }
            novos_dados.append(dados_ticker)
            print(f"✅ Dados de {ticker} coletados.")

        except Exception as e:
            print(f"❌ Erro ao coletar {ticker}: {e}")

    return novos_dados


# ---------------------------------------------------------------------------
# Tratamento
# ---------------------------------------------------------------------------

def tratamento_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Recebe df como parâmetro e retorna df tratado — sem variáveis globais."""
    cols_financeiras = [
        "P/L", "ROE", "dividendYield", "debtToEquity",
        "freeCashflow", "ebitdaMargins", "preco_atual",
        "variacao_dia", "market_cap",
    ]
    df = df.copy()
    # Garante tipo numérico
    for col in cols_financeiras:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# ---------------------------------------------------------------------------
# Criação do DataFrame
# ---------------------------------------------------------------------------

def cria_df_final(lista_tickers):

    # 1. Garante tabelas e atualiza dados estáticos das empresas
    init_db()
    salvar_empresas_no_db(lista_tickers)
    
    # 2. Coleta dados dinâmicos
    dados_mercado = pega_dados_mercado(lista_tickers)
    df_mercado = pd.DataFrame(dados_mercado)
    
    # 3. Busca dados estáticos do banco para o Merge
    df_cadastro = cria_df_dados_cadastro()

    # 4. Une as bases e aplica conversões numéricas
    df_base = pd.merge(df_cadastro, df_mercado, on="ticker", how="left")
    df_tratado = tratamento_dados(df_base)
    
    print(f"✔ {len(df_tratado)} ativos prontos para análise.")

    # Pausa de 1s para respeitar limites de taxa (Rate Limits) da API
    df_relatorios = analisar_lote(df_tratado, pausa=1.0)

    # 6. Merge final entre indicadores e as análises geradas
    df_completo = pd.merge(df_tratado, df_relatorios, on="ticker", how="left")
    
    # Validação rápida de integridade
    if 'analise_fundamentos' in df_completo.columns:
        sucessos = df_completo['analise_fundamentos'].notna().sum()
        print(f"✔ Pipeline concluído: {sucessos}/{len(lista_tickers)} análises geradas.")
    
    return df_completo

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    df_final= cria_df_final(LISTA_TICKERS)

    # Serializamos as notícias para não perder os metadados (URL, Imagem)
    if "noticias" in df_final.columns:
        df_final["noticias_raw_json"] = df_final["noticias"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else "[]"
        )

    # Persistência final
    print("\n=== Gravando Snapshot Histórico no DB ===")
    salvar_snapshot_no_db(df_final)
    print("\n🚀 Processo concluído 🚀")