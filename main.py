import json
import os
import yfinance as yf


lista_tickers=["ASAI3","RECV3","MOVI3","BRKM5","HBSA3","ITUB4","BBDC4","OPCT3","BRSR6","PRIO3"]

# Criamos um arquivo para acessar mais rapidamente os dados cadastrais
def escreve_dados_empresa(lista_tickers, nome_arquivo='empresa_info_cadastro.txt'):
    with open(nome_arquivo, 'w', encoding='utf-8') as arq:
        for ticker in lista_tickers:
            try:
                req = ticker + ".SA"
                empresa = yf.Ticker(req)
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