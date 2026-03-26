import json
import os
import io

import yfinance as yf
import pandas as pd
import re
import requests
from dotenv import load_dotenv



lista_tickers=["ASAI3","RECV3","MOVI3","BRKM5","HBSA3","ITUB4","BBDC4","OPCT3","BRSR6","PRIO3"]

# Criamos um arquivo para acessar mais rapidamente os dados cadastrais
def escreve_dados_empresa(lista_tickers, nome_arquivo='empresa_info_cadastro.txt'):
    with open(nome_arquivo, 'w', encoding='utf-8') as arq:
        for ticker in lista_tickers:
            try:
                tickerSA = ticker + ".SA"
                empresa = yf.Ticker(tickerSA)
                info = empresa.info
                
                # Criando o dicionário com .get() para evitar erros de chaves ausentes
                dic_empresa = {
                    "nome": info.get("longName"),
                    "ticker": ticker, 
                    "setor": info.get("sectorDisp"),
                    "segAtuacao": info.get("industryDisp"), 
                    "descricao": info.get("longBusinessSummary")
                }

                # O modo 'a' cria o arquivo se não existir e adiciona ao final se existir
                linha = json.dumps(dic_empresa, ensure_ascii=False)
                arq.write(linha + "\n")
                
                print(f"✅ {ticker} salvo com sucesso.")
                
            except Exception as e:
                print(f"Erro ao processar {ticker}: {e}")
    arq.close()


def salvar_dados_empresa():
    if os.path.exists("empresa_info_cadastro.txt"):
        print("Já existe esse arquivo")
    else:
        escreve_dados_empresa(lista_tickers)

salvar_dados_empresa()

empresa=yf.Ticker(lista_tickers[0]+".SA")

print(empresa.info.get("trailingPE"))
print(empresa.info.get("returnOnEquity"))
print(empresa.info.get("debtToEquity"))
print(empresa.info.get("dividendYield"))
print(empresa.info.get("freeCashflow"))

with open('empresa_info_cadastro.txt', 'r', encoding='utf-8') as f:
    content = f.read()

    # Substitui padrões como por uma string vazia
    clean_content = re.sub(r"\ ", '', content)

    # 3. Carregando no Pandas
    # Usamos o io.StringIO para que o Pandas leia a string como se fosse um arquivo
    # O argumento lines=True é fundamental pois cada linha é um objeto JSON
    df_cadastro = pd.DataFrame([pd.read_json(io.StringIO(line), typ='series') 
                    for line in clean_content.strip().split('\n')])
f.close()
# Visualizando o resultado

def pega_dados_mercado(lista_tickers):
    novos_dados = []

    for ticker in lista_tickers:
        try:
            tickerSA = ticker + ".SA"
            empresa = yf.Ticker(tickerSA)
            info = empresa.info
            
            # Criamos um dicionário com os dados coletados
            dados_ticker = {
                "ticker": ticker,
                "P/L": info.get("trailingPE"),
                "ROE": info.get("returnOnEquity"),
                "preco_atual": info.get("currentPrice"),
                "debtToEquity": info.get("debtToEquity"),
                "dividendYield": info.get("dividendYield"),
                "freeCashflow": info.get("freeCashflow"),
                "ebitdaMargins": info.get("ebitdaMargins")
            }
            novos_dados.append(dados_ticker)
            print(f"✅ Dados de {ticker} coletados.")
            
        except Exception as e:
            print(f"❌ Erro ao coletar {ticker}: {e}")
    return novos_dados

# df_mercado=pd.DataFrame(pega_dados_mercado(lista_tickers))
# df_final = pd.merge(df_cadastro, df_mercado, on="ticker", how="left")
# print(df_final.head())

# def gnews_scraper(ticker):
#     # Busca notícias da empresa no Google News Brasil
#     url = f"https://news.google.com/rss/search?q={ticker}+stock+B3&hl=pt-BR&gl=BR&ceid=BR:pt-419"
#     feed = feedparser.parse(url)
    
#     for entry in feed.entries[:5]:
#         print(f"Notícia: {entry.title}")
#         print(f"Link: {entry.link}")

# gnews_scraper(lista_tickers[0])]

def busca_noticias_pro(ticker_nome):
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    if not API_KEY:
        print("Erro: API_KEY não encontrada no ambiente.")
        return []
    
    url = f"https://newsapi.org/v2/everything?q={ticker_nome}&language=pt&sortBy=publishedAt&apiKey={API_KEY}"
    
    response = requests.get(url).json()
    artigos = response.get('articles', [])[:5]
    
    return artigos # Retorna título, descrição (snippet) e conteúdo parcial

print(busca_noticias_pro(lista_tickers[0]))