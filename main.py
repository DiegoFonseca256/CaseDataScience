import json
import os
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

from LLM import analisar_lote
from database import get_conn, init_db,ler_tickers_do_txt

load_dotenv()

NEWS_API_KEY     = os.getenv("NEWS_API_KEY", "")


# ---------------------------------------------------------------------------
# Data Base
# ---------------------------------------------------------------------------

def listar_tickers():
    with get_conn() as conn:
        cursor = conn.execute("SELECT ticker FROM empresas")
        return [row[0] for row in cursor.fetchall()]

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
            
            conn.execute('''
                    INSERT INTO snapshots (
                        ticker, data_coleta, preco_atual, variacao_dia, 
                        debtToEquity, freeCashflow, ebitdaMargins, 
                        LiquiCorrente, VolMedDiario, MargemOperacional, 
                        min_52, max_52, pl, roe, dy, 
                        market_cap, beta, resumo_llm, analise_llm, perguntas_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ticker, 
                    agora, 
                    row.get('preco_atual'),
                    row.get("variacao_dia"),
                    row.get("debt_to_equity") or row.get("debtToEquity"), # Tenta os dois formatos
                    row.get("free_cashflow") or row.get("freeCashflow"),
                    row.get("ebitda_margins") or row.get("ebitdaMargins"),
                    row.get("liquidez_corrente") or row.get("Liqui Corrente"), # Mapeia o nome com espaço para o sem espaço
                    row.get("vol_medio_diario") or row.get("Vol Med Diário"),
                    row.get("margem_operacional") or row.get("Margem Operacional"),
                    row.get('min_52'),
                    row.get('max_52'), 
                    row.get('pl') or row.get('P/L'), 
                    row.get('roe') or row.get('ROE'), 
                    row.get('dividend_yield') or row.get('dividendYield'),
                    row.get('market_cap'),
                    row.get('beta') or row.get('Beta'),
                    row.get('resumo_llm'), # Certifique-se que o merge gerou 'resumo_llm' e não 'resumo_negocio'
                    row.get('analise_llm'),
                    row.get('perguntas_json')
                ))
            
            try:
                noticias_brutas = json.loads(row.get('noticias_raw_json', '[]'))
                sentimentos_ia = json.loads(row.get('noticias_json', '[]'))
                
                mapa_sentimento = {item['titulo']: item['sentimento'] for item in sentimentos_ia if 'titulo' in item}

                for noticia in noticias_brutas:
                    titulo = noticia.get('title')
                    url = noticia.get('url')
                    descricao = noticia.get("description")
                    fonte= noticia.get("source").get("name", "?")
                    data_noticia = noticia.get('publishedAt')
                    sentimento = mapa_sentimento.get(titulo, "neutra")
                    imagem= noticia.get("urlToImage")

                    conn.execute('''
                        INSERT INTO noticias_historico (
                            ticker, data_noticia, titulo, fonte, sentimento, url, imagem, descricao
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (ticker, data_noticia, titulo,fonte, sentimento, url, imagem, descricao))
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


def pega_dados_mercado(lista_tickers):
    novos_dados = []

    for ticker in lista_tickers:
        try:
            info = yf.Ticker(ticker + ".SA").info
            noticias = busca_noticias(ticker)

            max_52=info.get("fiftyTwoWeekHigh")
            min_52=info.get("fiftyTwoWeekLow")
            hist_1y = yf.Ticker(ticker + ".SA").history(period="1y")
                
            min_52 = hist_1y['Low'].min()
            max_52 = hist_1y['High'].max()

            dados_ticker = {
                "ticker":               ticker,
                
                "preco_atual":          info.get("currentPrice"),
                "variacao_dia":         info.get("regularMarketChangePercent"),
                "market_cap":           info.get("marketCap"),
                
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
                "max_52":    max_52,
                'min_52':    min_52,
             
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
    print(df_mercado.head())
    
    # 3. Busca dados estáticos do banco para o Merge
    df_cadastro = cria_df_dados_cadastro()

    # 4. Une as bases e aplica conversões numéricas
    df_base = pd.merge(df_cadastro, df_mercado, on="ticker", how="left")
    df_tratado = tratamento_dados(df_base)
    
    print(df_mercado[['ticker', 'min_52', 'max_52']].head())
    print(f"✔ {len(df_tratado)} ativos prontos para análise.")

    # Pausa de 1s para respeitar limites de taxa (Rate Limits) da API
    df_relatorios = analisar_lote(lista_tickers, pausa=1.0)
    
    # 6. Merge final entre indicadores e as análises geradas
    df_completo = pd.merge(df_tratado, df_relatorios, on="ticker", how="left")
    print(df_completo[['ticker', 'min_52', 'max_52']].head())
    
    # Validação rápida de integridade
    if 'analise_fundamentos' in df_completo.columns:
        sucessos = df_completo['analise_fundamentos'].notna().sum()
        print(f"✔ Pipeline concluído: {sucessos}/{len(lista_tickers)} análises geradas.")
    
    return df_completo

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    
    # 1. Carrega o que já está no banco
    tickers_base = listar_tickers()
    
    # 2. Carrega o que você adicionou pelo Dashboard (o TXT)
    tickers_novos = ler_tickers_do_txt("pendentes.txt")
    
    # 3. Une tudo em uma lista única (sem duplicados)
    LISTA_FINAL = list(set(tickers_base + tickers_novos))
    
    if not LISTA_FINAL:
        print("Nenhum ticker para processar.")
    else:
        print(f"=== Processando: {LISTA_FINAL} ===")
        df_final = cria_df_final(LISTA_FINAL)
        salvar_snapshot_no_db(df_final)
        
        # Opcional: Limpar o TXT após processar com sucesso
        if tickers_novos:
            open("pendentes.txt", "w").close() 
            print("✅ Fila de pendentes limpa.")