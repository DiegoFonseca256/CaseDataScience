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

from LLM import analisar_lote  # importa o módulo que acabamos de criar

load_dotenv()

LISTA_TICKERS    = ["ASAI3", "RECV3", "MOVI3", "BRKM5", "HBSA3",
                    "ITUB4", "BBDC4", "OPCT3", "BRSR6", "PRIO3"]
ARQUIVO_CADASTRO = "empresa_info_cadastro.txt"
NEWS_API_KEY     = os.getenv("NEWS_API_KEY", "")


# ---------------------------------------------------------------------------
# Cadastro
# ---------------------------------------------------------------------------

def escreve_dados_empresa(lista_tickers, nome_arquivo=ARQUIVO_CADASTRO):
    with open(nome_arquivo, "w", encoding="utf-8") as arq:
        for ticker in lista_tickers:
            try:
                info = yf.Ticker(ticker + ".SA").info
                dic_empresa = {
                    "nome":       info.get("longName"),
                    "ticker":     ticker,
                    "setor":      info.get("sectorDisp"),
                    "segAtuacao": info.get("industryDisp"),
                    "descricao":  info.get("longBusinessSummary"),
                }
                arq.write(json.dumps(dic_empresa, ensure_ascii=False) + "\n")
                print(f"✅ {ticker} salvo com sucesso.")
            except Exception as e:
                print(f"Erro ao processar {ticker}: {e}")


def salvar_dados_empresa():
    if os.path.exists(ARQUIVO_CADASTRO):
        print("Já existe esse arquivo")
    else:
        escreve_dados_empresa(LISTA_TICKERS)


def cria_df_dados_cadastro():
    with open(ARQUIVO_CADASTRO, "r", encoding="utf-8") as f:
        linhas = [line.strip() for line in f if line.strip()]

    dados_lista = []
    for line in linhas:
        try:
            dados_lista.append(pd.read_json(io.StringIO(line), typ="series"))
        except Exception as e:
            print(f"Erro ao processar linha: {e}")

    return pd.DataFrame(dados_lista)


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
    salvar_dados_empresa()
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