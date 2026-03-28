
import io
import json
import os
import time

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

LISTA_TICKERS    = ["ASAI3", "RECV3", "MOVI3", "BRKM5", "HBSA3",
                    "ITUB4", "BBDC4", "OPCT3", "BRSR6", "PRIO3"]
ARQUIVO_CADASTRO = "empresa_info_cadastro.txt"
NEWS_API_KEY     = os.getenv("NEWS_API_KEY", "")


# ---------------------------------------------------------------------------
# Cadastro
# ---------------------------------------------------------------------------

# Criamos um arquivo para acessar mais rapidamente os dados cadastrais e os buscamos
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

# Cria um dataframe
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

# Busca noticias com uma API de busca de noticias
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
# Mercado
# ---------------------------------------------------------------------------

# Busca dados do mercado com yfinance
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
                "Mínima 52 Semanas":    info.get("fiftyTwoWeekLow"),
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

#Trata os dados nulos
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
# Principal
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
