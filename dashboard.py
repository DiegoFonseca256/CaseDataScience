"""
dashboard.py
Interface Streamlit — Hipótese Capital

Execução:
    1. python main.py
    2. streamlit run dashboard.py
"""
import json
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from LLM import _fmt, _fmt_grande, _variacao_fmt

load_dotenv()

st.set_page_config(page_title="Hipótese Capital", page_icon="📊", layout="wide")

CSV_PATH = "empresas_com_analise.csv"

# ---------------------------------------------------------------------------
# Leitura do CSV
# ---------------------------------------------------------------------------
if not os.path.exists(CSV_PATH):
    st.error("Arquivo `empresas_com_analise.csv` não encontrado. Execute `python main.py` primeiro.")
    st.stop()

df = pd.read_csv(CSV_PATH, encoding="utf-8")

# ---------------------------------------------------------------------------
# Sidebar — seleção de ticker
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("📊 Hipótese Capital")
    ticker = st.selectbox("Ticker", df["ticker"].unique())

linha = df[df["ticker"] == ticker].iloc[0]

# ---------------------------------------------------------------------------
# Cabeçalho
# ---------------------------------------------------------------------------
st.title(f"{linha.get('nome', ticker)}")
st.caption(f"**{ticker}** · {linha.get('setor', '')} · {linha.get('segAtuacao', '')}")
st.divider()

# ---------------------------------------------------------------------------
# Indicadores
# ---------------------------------------------------------------------------
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Preço",         _fmt(linha.get("preco_atual"), prefixo="R$ "),
                           _variacao_fmt(linha.get("variacao_dia")))
c2.metric("P/L",           _fmt(linha.get("P/L"), sufixo="x"))
c3.metric("ROE",           _fmt(linha.get("ROE"), sufixo="%"))
c4.metric("Div. Yield",    _fmt(linha.get("dividendYield"), sufixo="%"))
c5.metric("Market Cap",    _fmt_grande(linha.get("market_cap")))

st.divider()

# ---------------------------------------------------------------------------
# Análise LLM
# ---------------------------------------------------------------------------
resumo  = linha.get("resumo_negocio", "")
analise = linha.get("analise_fundamentos", "")

if not resumo or pd.isna(resumo):
    st.warning("Análise não disponível. Execute `python main.py` com `GROQ_API_KEY` configurado.")
else:
    col_esq, col_dir = st.columns([3, 2])

    with col_esq:
        st.subheader("Resumo do negócio")
        st.info(resumo)
        st.subheader("Análise dos indicadores")
        st.write(analise)

    with col_dir:
        st.subheader("❓ Perguntas para investigar")
        try:
            for i, q in enumerate(json.loads(linha.get("perguntas_json", "[]")), 1):
                st.warning(f"**{i}.** {q}")
        except (json.JSONDecodeError, TypeError):
            st.caption("Perguntas não disponíveis.")

st.divider()

# ---------------------------------------------------------------------------
# Notícias
# ---------------------------------------------------------------------------
st.subheader("📰 Notícias")

try:
    noticias_raw  = json.loads(linha.get("noticias_raw_json", "[]") or "[]")
    noticias_llm  = json.loads(linha.get("noticias_json", "{}") or "{}")
except (json.JSONDecodeError, TypeError):
    noticias_raw  = []
    noticias_llm  = {}

# Monta sets por sentimento para lookup rápido
positivas = {t.strip().lower() for t in noticias_llm.get("positivas", [])}
negativas = {t.strip().lower() for t in noticias_llm.get("negativas", [])}

def sentimento(titulo: str) -> str:
    t = titulo.strip().lower()
    if any(t in s or s in t for s in positivas): return "positiva"
    if any(t in s or s in t for s in negativas): return "negativa"
    return "neutra"

icones = {"positiva": "🟢", "negativa": "🔴", "neutra": "⚪"}

if not noticias_raw:
    st.caption("Sem notícias. Configure `NEWS_API_KEY` no `.env` e execute `python main.py`.")
else:
    for artigo in noticias_raw:
        titulo = artigo.get("title", "Sem título")
        url    = artigo.get("url", "#")
        fonte  = (artigo.get("source") or {}).get("name", "?")
        data   = (artigo.get("publishedAt") or "")[:10]
        imagem = artigo.get("urlToImage")
        sent   = sentimento(titulo)
        icon   = icones[sent]

        with st.container(border=True):
            col_img, col_texto = st.columns([1, 3])
            with col_img:
                if imagem:
                    st.image(imagem, use_container_width=True)
                else:
                    st.caption("Sem imagem")
            with col_texto:
                st.markdown(f"{icon} **[{titulo}]({url})**")
                st.caption(f"{fonte} · {data}")