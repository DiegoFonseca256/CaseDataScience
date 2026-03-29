"""
dashboard.py
Interface Streamlit — Hipótese Capital (versão completa)

Execução:
    1. python main.py
    2. streamlit run dashboard.py

Requisitos:
    pip install streamlit plotly pandas yfinance python-dotenv
"""
import json
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf
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
# Funções auxiliares
# ---------------------------------------------------------------------------

@st.cache_data(ttl=86400, show_spinner=False)
def coletar_historico(ticker: str, periodo: str = "6mo") -> pd.DataFrame:
    """Histórico de preços para o gráfico — único dado buscado ao vivo."""
    try:
        hist = yf.Ticker(ticker + ".SA").history(period=periodo)
        return hist[["Close", "Volume"]].reset_index()
    except Exception:
        return pd.DataFrame()


def sentimento(titulo: str, positivas: set, negativas: set) -> str:
    """Identifica o sentimento da notícia consultando as listas do noticias_json."""
    t = titulo.strip().lower()
    if any(t in s or s in t for s in positivas):
        return "positiva"
    if any(t in s or s in t for s in negativas):
        return "negativa"
    return "neutra"


def renderizar_cards_noticias(noticias_raw: list, noticias_llm: dict) -> None:
    """Renderiza notícias como cards clicáveis com foto via HTML."""
    if not noticias_raw:
        st.caption("Sem notícias. Configure `NEWS_API_KEY` no `.env` e execute `python main.py`.")
        return

    positivas = {t.strip().lower() for t in noticias_llm.get("positivas", [])}
    negativas = {t.strip().lower() for t in noticias_llm.get("negativas", [])}

    cores  = {"positiva": "#22c55e", "negativa": "#ef4444", "neutra": "#94a3b8"}
    labels = {"positiva": "Positiva", "negativa": "Negativa", "neutra": "Neutra"}
    placeholder = "https://placehold.co/400x200/1e293b/94a3b8?text=Sem+imagem"

    cards_html = ""
    for artigo in noticias_raw:
        titulo = artigo.get("title", "Sem título") or "Sem título"
        url    = artigo.get("url", "#") or "#"
        fonte  = (artigo.get("source") or {}).get("name", "?")
        data   = (artigo.get("publishedAt") or "")[:10]
        imagem = artigo.get("urlToImage") or placeholder
        descr  = (artigo.get("description") or "")[:120]
        sent   = sentimento(titulo, positivas, negativas)
        cor    = cores[sent]
        label  = labels[sent]

        titulo_safe = titulo.replace('"', "&quot;").replace("'", "&#39;")
        descr_safe  = descr.replace('"', "&quot;").replace("'", "&#39;")

        cards_html += f"""
        <a href="{url}" target="_blank" style="text-decoration:none;">
          <div class="card">
            <div class="card-img-wrap">
              <img src="{imagem}" onerror="this.src='{placeholder}'" alt="{titulo_safe}"/>
              <span class="badge" style="background:{cor}">{label}</span>
            </div>
            <div class="card-body">
              <p class="card-title">{titulo_safe}</p>
              <p class="card-descr">{descr_safe}</p>
              <p class="card-meta">{fonte} · {data}</p>
            </div>
          </div>
        </a>"""

    html = f"""
    <style>
      * {{ box-sizing: border-box; margin: 0; padding: 0; }}
      body {{ background: transparent; font-family: sans-serif; }}
      .grid {{
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
        gap: 16px;
        padding: 4px 2px 16px;
      }}
      .card {{
        background: #1e293b;
        border-radius: 12px;
        overflow: hidden;
        transition: transform .18s ease, box-shadow .18s ease;
        cursor: pointer;
        border: 1px solid #334155;
      }}
      .card:hover {{ transform: translateY(-4px); box-shadow: 0 8px 24px rgba(0,0,0,.35); }}
      .card-img-wrap {{ position: relative; width: 100%; height: 160px; overflow: hidden; background: #0f172a; }}
      .card-img-wrap img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
      .badge {{
        position: absolute; top: 10px; right: 10px;
        padding: 3px 10px; border-radius: 999px;
        font-size: 11px; font-weight: 600; color: #fff; letter-spacing: .4px;
      }}
      .card-body {{ padding: 14px 16px 16px; }}
      .card-title {{
        font-size: 14px; font-weight: 600; color: #f1f5f9;
        line-height: 1.4; margin-bottom: 6px;
        display: -webkit-box; -webkit-line-clamp: 3;
        -webkit-box-orient: vertical; overflow: hidden;
      }}
      .card-descr {{
        font-size: 12px; color: #94a3b8; line-height: 1.5; margin-bottom: 10px;
        display: -webkit-box; -webkit-line-clamp: 2;
        -webkit-box-orient: vertical; overflow: hidden;
      }}
      .card-meta {{ font-size: 11px; color: #64748b; font-weight: 500; }}
    </style>
    <div class="grid">{cards_html}</div>
    """

    altura = ((len(noticias_raw) // 3) + 1) * 320
    components.html(html, height=altura, scrolling=False)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("📊 Hipótese Capital")
    st.caption("Painel de Análise Fundamentalista")
    st.divider()

    ticker = st.selectbox("Ticker", df["ticker"].unique())

    st.divider()
    if st.button("🔄 Recarregar dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    groq_ok = bool(os.getenv("GROQ_API_KEY"))
    news_ok = bool(os.getenv("NEWS_API_KEY"))
    st.caption(f"LLM : {'✅ Groq ok' if groq_ok else '❌ GROQ_API_KEY ausente'}")
    st.caption(f"News: {'✅ NewsAPI ok' if news_ok else '⚠️  Sem NewsAPI'}")


# ---------------------------------------------------------------------------
# Filtra linha do ticker selecionado
# ---------------------------------------------------------------------------
linha = df[df["ticker"] == ticker].iloc[0]


# ---------------------------------------------------------------------------
# Cabeçalho
# ---------------------------------------------------------------------------
col_titulo, col_preco = st.columns([3, 1])

with col_titulo:
    st.title(f"{linha.get('nome', ticker)}")
    st.caption(f"**{ticker}** · {linha.get('setor', '')} · {linha.get('segAtuacao', '')}")

with col_preco:
    st.metric(
        label="Preço atual",
        value=_fmt(linha.get("preco_atual"), prefixo="R$ "),
        delta=_variacao_fmt(linha.get("variacao_dia")),
    )

st.divider()

# ---------------------------------------------------------------------------
# Indicadores principais
# ---------------------------------------------------------------------------
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Market Cap",    _fmt_grande(linha.get("market_cap")))
c2.metric("P/L",           _fmt(linha.get("P/L"), sufixo="x"))
c3.metric("ROE",           _fmt(linha.get("ROE"), sufixo="%"))
c4.metric("Div. Yield",    _fmt(linha.get("dividendYield"), sufixo="%"))
c5.metric("Dívida/Equity", _fmt(linha.get("debtToEquity"), sufixo="x"))
c6.metric("Beta",          _fmt(linha.get("Beta")))

with st.expander("📐 Todos os indicadores"):
    ca, cb = st.columns(2)
    with ca:
        st.metric("Margem EBITDA",      _fmt(linha.get("ebitdaMargins"), sufixo="%"))
        st.metric("Margem Operacional", _fmt(linha.get("Margem Operacional"), sufixo="%"))
        st.metric("Liquidez Corrente",  _fmt(linha.get("Liqui Corrente"), sufixo="x"))
    with cb:
        st.metric("Free Cash Flow",     _fmt_grande(linha.get("freeCashflow")))
        st.metric("Vol. Médio Diário",  _fmt(linha.get("Vol Med Diário"), dec=0))
        st.metric("Máx. 52 semanas",    _fmt(linha.get("Máxima 52 Semanas"), prefixo="R$ "))
        st.metric("Mín. 52 semanas",    _fmt(linha.get("Mínima 52 Semanas"), prefixo="R$ "))

# Barra de progresso do range de 52 semanas
try:
    preco  = float(linha.get("preco_atual"))
    min_52 = float(linha.get("Mínima 52 Semanas"))
    max_52 = float(linha.get("Máxima 52 Semanas"))
    rng = max_52 - min_52
    if rng > 0:
        pct = (preco - min_52) / rng
        st.markdown(f"**Range 52 semanas** — preço atual em **{pct*100:.0f}%** do range (mín → máx)")
        st.progress(min(max(pct, 0.0), 1.0))
except (TypeError, ValueError):
    pass

st.divider()

# ---------------------------------------------------------------------------
# Gráfico histórico de preços
# ---------------------------------------------------------------------------
with st.expander("📈 Histórico de preços", expanded=True):
    periodo = st.radio(
        "Período", ["1mo", "3mo", "6mo", "1y", "2y"],
        index=2, horizontal=True, label_visibility="collapsed",
    )
    with st.spinner("Carregando histórico..."):
        hist = coletar_historico(ticker, periodo)

    if not hist.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=hist["Date"], y=hist["Close"],
            mode="lines", name="Preço",
            line=dict(color="#1f77b4", width=2),
            fill="tozeroy", fillcolor="rgba(31,119,180,0.08)",
        ))
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0), height=260,
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.15)"),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("Histórico não disponível.")

st.divider()

# ---------------------------------------------------------------------------
# Análise LLM
# ---------------------------------------------------------------------------
st.markdown("### 🤖 Análise da IA")

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

    # Notícias classificadas em abas
    st.divider()
    st.subheader("📰 Notícias classificadas pela IA")
    try:
        noticias_llm = json.loads(linha.get("noticias_json", "{}") or "{}")
    except (json.JSONDecodeError, TypeError):
        noticias_llm = {}

    tab_pos, tab_neg, tab_neu = st.tabs([
        f"🟢 Positivas ({len(noticias_llm.get('positivas', []))})",
        f"🔴 Negativas ({len(noticias_llm.get('negativas', []))})",
        f"⚪ Neutras ({len(noticias_llm.get('neutras', []))})",
    ])
    with tab_pos:
        for n in noticias_llm.get("positivas", []) or ["Nenhuma identificada."]:
            st.success(n)
    with tab_neg:
        for n in noticias_llm.get("negativas", []) or ["Nenhuma identificada."]:
            st.error(n)
    with tab_neu:
        for n in noticias_llm.get("neutras", []) or ["Nenhuma identificada."]:
            st.info(n)

st.divider()

# ---------------------------------------------------------------------------
# Cards de notícias com foto e link
# ---------------------------------------------------------------------------
st.subheader("🗞️ Notícias com foto")

try:
    noticias_raw = json.loads(linha.get("noticias_raw_json", "[]") or "[]")
    noticias_llm = json.loads(linha.get("noticias_json", "{}") or "{}")
except (json.JSONDecodeError, TypeError):
    noticias_raw = []
    noticias_llm = {}

renderizar_cards_noticias(noticias_raw, noticias_llm)