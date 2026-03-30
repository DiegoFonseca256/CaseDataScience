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
import time

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
# Cadastro
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
# Pipeline principal
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    # 1. Cadastro
    salvar_empresas_no_db(LISTA_TICKERS)
    df_cadastro = cria_df_dados_cadastro()

    # 2. Mercado + notícias
    df_mercado = pd.DataFrame(pega_dados_mercado(LISTA_TICKERS))

    # 3. Merge
    df_final = pd.merge(df_cadastro, df_mercado, on="ticker", how="left")

    # 4. Tratamento
    df_final = tratamento_dados(df_final)
    print(df_final[["ticker", "preco_atual", "variacao_dia", "market_cap", "P/L"]].head())

    # 5. Análise LLM — usa analyst.py
    print("\n=== Gerando análises com LLM ===")
    df_relatorios = analisar_lote(df_final, pausa=1.0)

    # 6. Merge com relatórios
    df_completo = pd.merge(df_final, df_relatorios, on="ticker", how="left")

    # 7. Serializa notícias brutas como JSON string para o dashboard usar urlToImage e url
    if "noticias" in df_completo.columns:
        df_completo["noticias_raw_json"] = df_completo["noticias"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else "[]"
        )
        df_completo.drop(columns=["noticias"], inplace=True)

    # 8. Salva
    df_completo.to_csv("empresas_com_analise.csv", index=False, encoding="utf-8")
    print("\n✅ Concluído. Resultado em: empresas_com_analise.csv")