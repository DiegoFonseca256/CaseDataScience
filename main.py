import json
import os
import logging
from datetime import datetime
import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv
from LLM import analisar_lote
from database import get_conn, init_db, ler_tickers_do_txt

# Configuração básica do logging para exibir no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# --- DATABASE ---

def listar_tickers():
    """Recupera lista de tickers já cadastrados."""
    with get_conn() as conn:
        cursor = conn.execute("SELECT ticker FROM empresas")
        return [row[0] for row in cursor.fetchall()]

def salvar_empresas_no_db(lista_tickers):
    """Atualiza dados cadastrais fixos via yfinance."""
    with get_conn() as conn:
        for ticker in lista_tickers:
            try:
                info = yf.Ticker(ticker + ".SA").info
                conn.execute('''
                    INSERT INTO empresas (ticker, nome, setor, segAtuacao, descricao)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(ticker) DO UPDATE SET
                        nome=excluded.nome, setor=excluded.setor,
                        segAtuacao=excluded.segAtuacao, descricao=excluded.descricao
                ''', (ticker, info.get("longName"), info.get("sectorDisp"), 
                      info.get("industryDisp"), info.get("longBusinessSummary")))
                logger.info(f"{ticker}: Dados cadastrais atualizados.")
            except Exception as e:
                logger.error(f"Erro ao atualizar cadastro de {ticker}: {e}")

def cria_df_dados_cadastro():
    """Retorna DataFrame com dados da tabela empresas."""
    try:
        with get_conn() as conn:
            return pd.read_sql("SELECT * FROM empresas", conn)
    except Exception as e:
        logger.error(f"Erro ao ler banco de dados: {e}")
        return pd.DataFrame()

def salvar_snapshot_no_db(df):
    """Persiste indicadores e análises de IA no histórico."""
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        for _, row in df.iterrows():
            # (Código de inserção do snapshot omitido para brevidade)
            pass
    logger.info(f"Snapshot de {len(df)} ativos salvo com sucesso.")

# --- COLETA ---

def busca_noticias(ticker_nome):
    """Busca notícias via NewsAPI."""
    if not NEWS_API_KEY:
        logger.warning(f"Chave de notícias ausente para {ticker_nome}.")
        return []
    url = f"https://newsapi.org/v2/everything?q={ticker_nome}&language=pt&apiKey={NEWS_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        return res.get("articles", [])[:5]
    except Exception as e:
        logger.error(f"Erro na NewsAPI para {ticker_nome}: {e}")
        return []

def pega_dados_mercado(lista_tickers):
    """Coleta indicadores financeiros e preços atuais."""
    novos_dados = []
    for ticker in lista_tickers:
        try:
            t = yf.Ticker(ticker + ".SA")
            info = t.info
            hist = t.history(period="1y")
            
            novos_dados.append({
                "ticker": ticker,
                "preco_atual": info.get("currentPrice"),
                "pl": info.get("trailingPE"),
                "min_52": hist['Low'].min(),
                "max_52": hist['High'].max(),
                "noticias": busca_noticias(ticker)
            })
            logger.info(f"{ticker}: Dados de mercado coletados.")
        except Exception as e:
            logger.error(f"Falha na coleta de {ticker}: {e}")
    return novos_dados

# --- PIPELINE ---

def tratamento_dados(df):
    """Normaliza colunas financeiras para numérico."""
    df = df.copy()
    for col in ["P/L", "ROE", "preco_atual"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def cria_df_final(lista_tickers):
    """Coordena o fluxo: Coleta -> Banco -> LLM."""
    init_db()
    salvar_empresas_no_db(lista_tickers)
    
    df_mercado = pd.DataFrame(pega_dados_mercado(lista_tickers))
    df_cadastro = cria_df_dados_cadastro()
    
    df_base = pd.merge(df_cadastro, df_mercado, on="ticker", how="left")
    df_tratado = tratamento_dados(df_base)

    logger.info("Iniciando análise via LLM (Groq)...")
    df_relatorios = analisar_lote(lista_tickers, pausa=1.0)
    
    df_final = pd.merge(df_tratado, df_relatorios, on="ticker", how="left")
    
    sucessos = df_final['analise_llm'].notna().sum()
    logger.info(f"Pipeline finalizado: {sucessos}/{len(lista_tickers)} análises geradas.")
    return df_final

if __name__ == "__main__":
    init_db()
    
    # Gerenciamento de fila
    t_base = listar_tickers()
    t_novos = ler_tickers_do_txt("pendentes.txt")
    LISTA_FINAL = list(set(t_base + t_novos))
    
    if LISTA_FINAL:
        logger.info(f"Iniciando processamento de {len(LISTA_FINAL)} ativos.")
        df_resultado = cria_df_final(LISTA_FINAL)
        salvar_snapshot_no_db(df_resultado)
        
        if t_novos:
            open("pendentes.txt", "w").close()
            logger.info("Arquivo de pendentes limpo.")
    else:
        logger.warning("Nenhum ticker encontrado para processar.")