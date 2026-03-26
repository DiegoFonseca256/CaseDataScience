import json
import os
import io

import yfinance as yf
import pandas as pd
import re
import requests



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

print(empresa.info.get("trailingPE",None))
print(empresa.info.get("returnOnEquity",None))
print(empresa.info.get("debtToEquity"))
print(empresa.info.get("dividendYield"))
print(empresa.info.get("freeCashflow"))

with open('empresa_info_cadastro.txt', 'r', encoding='utf-8') as f:
    # Lemos linha por linha e ignoramos linhas vazias
    linhas = [line.strip() for line in f if line.strip()]

# Carregando no Pandas de forma segura
dados_lista = []
for line in linhas:
    try:
        # Lemos cada linha JSON e convertemos em série para o DataFrame
        dados_lista.append(pd.read_json(io.StringIO(line), typ='series'))
    except Exception as e:
        print(f"Erro ao processar linha: {e}")

df_cadastro = pd.DataFrame(dados_lista)
f.close()
# Visualizando o resultado

def busca_noticias_pro(ticker_nome):
    api_key = "b2dea0b5ebce41bcb77adf0348289540"
    url = f"https://newsapi.org/v2/everything?q={ticker_nome}&language=pt&sortBy=publishedAt&apiKey={api_key}"
    
    response = requests.get(url).json()
    artigos = response.get('articles', [])[:5]
    
    return artigos # Retorna título, descrição (snippet) e conteúdo parcial


def pega_dados_mercado(lista_tickers):
    novos_dados = []

    for ticker in lista_tickers:
        try:
            tickerSA = ticker + ".SA"
            empresa = yf.Ticker(tickerSA)
            info = empresa.info
            noticias=busca_noticias_pro(ticker)
            # Criamos um dicionário com os dados coletados
            dados_ticker = {
                "ticker": ticker,
                "P/L": info.get("trailingPE"),
                "ROE": info.get("returnOnEquity"),
                "preco_atual": info.get("currentPrice"),
                "debtToEquity": info.get("debtToEquity"),
                "dividendYield": info.get("dividendYield"),
                "freeCashflow": info.get("freeCashflow"),
                "ebitdaMargins": info.get("ebitdaMargins"),
                "noticias": noticias
            }
            novos_dados.append(dados_ticker)
            print(f"✅ Dados de {ticker} coletados.")
            
        except Exception as e:
            print(f"❌ Erro ao coletar {ticker}: {e}")
    return novos_dados

df_mercado=pd.DataFrame(pega_dados_mercado(lista_tickers))
df_final = pd.merge(df_cadastro, df_mercado, on="ticker", how="left")


def tratamento_dados(df):
    # Lista das colunas que podem ter valores nulos
    cols_financeiras = ["P/L", "ROE", "dividendYield", "debtToEquity", "freeCashflow", "ebitdaMargins"]

    # 1. Garantimos que são números primeiro (converte erros em NaN)
    df[cols_financeiras] = df[cols_financeiras].apply(pd.to_numeric, errors='coerce')

    # 2. Convertemos as colunas para o tipo 'object' (que aceita texto)
    df_final[cols_financeiras] = df_final[cols_financeiras].astype(object)

    # 3. Agora o preenchimento com "N/A" não dará erro
    df_final.fillna("N/A", inplace=True)
    return df

tratamento_dados(df_final)
print(df_final.head())

df_final.to_csv("empresas.csv", index=False,encoding="utf-8")