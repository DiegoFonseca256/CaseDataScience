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
from database import (
    get_conn, init_db,
    listar_portfolio, adicionar_ticker, remover_ticker, ticker_tem_dados,
)

load_dotenv()

st.set_page_config(page_title="Hipótese Capital", page_icon="📊", layout="wide")

# ---------------------------------------------------------------------------
# CARREGAMENTO DE DADOS VIA SQLITE
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner="Buscando dados no banco...")
def carregar_dados():

    query = """
    SELECT e.nome, e.setor, e.segAtuacao, e.descricao, s.*
    FROM empresas e
    JOIN snapshots s ON e.ticker = s.ticker
    WHERE s.id IN (SELECT MAX(id) FROM snapshots GROUP BY ticker)
    """
    try:
        with get_conn() as conn:
            df_db = pd.read_sql(query, conn)
        return df_db
    except Exception as e:
        st.error(f"Erro ao acessar o banco de dados: {e}")
        return pd.DataFrame()

# Inicializa o DB caso não exista e carrega
init_db()
df = carregar_dados()

if df.empty:
    st.warning("⚠️ Banco de dados vazio ou não encontrado. Execute `python main.py` primeiro.")
    st.stop()


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


def renderizar_cards_noticias(ticker: str):

    with get_conn() as conn:
        query = "SELECT * FROM noticias_historico WHERE ticker = ? ORDER BY id DESC LIMIT 6"
        noticias = conn.execute(query, (ticker,)).fetchall()

    if not noticias:
        st.caption("Sem notícias registradas para este ticker.")
        return

    cores = {"positiva": "#22c55e", "negativa": "#ef4444", "neutra": "#94a3b8"}
    labels = {"positiva": "Positiva", "negativa": "Negativa", "neutra": "Neutra"}
    placeholder = "https://placehold.co/400x200/1e293b/94a3b8?text=Sem+imagem"

    cards_html = ""
    for n in noticias:
        
        cor = cores.get(n['sentimento'], "#94a3b8")
        label  = labels[n['sentimento']]
        titulo_safe = n['titulo'].replace('"', "&quot;").replace("'", "&#39;")
        descr_safe  = n['descricao'].replace('"', "&quot;").replace("'", "&#39;")

        cards_html += f"""
        <a href="{n["url"]}" target="_blank" style="text-decoration:none;">
          <div class="card">
            <div class="card-img-wrap">
              <img src="{n["imagem"]}" onerror="this.src='{n["placeholder"]}'" alt="{n["titulo_safe"]}"/>
              <span class="badge" style="background:{cor}">{label}</span>
            </div>
            <div class="card-body">
              <p class="card-title">{titulo_safe}</p>
              <p class="card-descr">{descr_safe}</p>
              <p class="card-meta">{n['fonte']} · {n['data']}</p>
            </div>
          </div>
        </a>"""

    # Estilos CSS simplificados para o modo DB
    html = f"""
    <style>
      .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 12px; }}
      .card {{ background: #1e293b; border-radius: 8px; padding: 15px; font-family: sans-serif; }}
      .card-title {{ font-size: 14px; color: #f1f5f9; font-weight: 600; margin: 8px 0; }}
      .card-meta {{ font-size: 10px; font-weight: bold; }}
    </style>
    <div class="grid">{cards_html}</div>"""
    
    components.html(html, height=350)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("📊 Hipótese Capital")
    st.caption("Painel de Análise Fundamentalista")
    st.divider()

    # --- Seleção de ticker para visualização ---
    ticker = st.selectbox("Ticker", df["ticker"].unique())

    st.divider()

    # --- Gerenciamento do portfólio ---
    st.markdown("**⚙️ Portfólio**")

    # Adicionar ticker
    novo_ticker = st.text_input(
        "Adicionar ticker",
        placeholder="Ex: VALE3",
        max_chars=6,
        label_visibility="collapsed",
    ).upper().strip()

    if st.button("➕ Adicionar", use_container_width=True, disabled=not novo_ticker):
        sucesso, msg = adicionar_ticker(novo_ticker)
        if sucesso:
            st.success(msg)
            st.cache_data.clear()
            st.rerun()
        else:
            st.error(msg)

    # Remover ticker
    portfolio_atual = listar_portfolio()
    ticker_remover = st.selectbox(
        "Remover ticker",
        options=["— selecione —"] + portfolio_atual,
        label_visibility="collapsed",
    )

    if st.button(
        "🗑️ Remover do portfólio",
        use_container_width=True,
        disabled=(ticker_remover == "— selecione —"),
        type="secondary",
    ):
        sucesso, msg = remover_ticker(ticker_remover)
        if sucesso:
            st.warning(msg)
            st.cache_data.clear()
            st.rerun()
        else:
            st.error(msg)

    # Lista atual do portfólio
    with st.expander(f"📋 Portfólio ({len(portfolio_atual)} tickers)"):
        if portfolio_atual:
            for t in portfolio_atual:
                tem_dados = ticker_tem_dados(t)
                st.caption(f"{'✅' if tem_dados else '⏳'} {t}")
        else:
            st.caption("Portfólio vazio.")

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
c2.metric("P/L",           _fmt(linha.get("pl"), sufixo="x"))
c3.metric("ROE",           _fmt(linha.get("roe"), sufixo="%"))
c4.metric("Div. Yield",    _fmt(linha.get("dy"), sufixo="%"))
c5.metric("Dívida/Equity", _fmt(linha.get("debtToEquity"), sufixo="x"))
c6.metric("Beta",          _fmt(linha.get("beta")))

with st.expander("📐 Todos os indicadores"):
    ca, cb = st.columns(2)
    with ca:
        st.metric("Margem EBITDA",      _fmt(linha.get("ebitdaMargins"), sufixo="%"))
        st.metric("Margem Operacional", _fmt(linha.get("MargemOperacional"), sufixo="%"))
        st.metric("Liquidez Corrente",  _fmt(linha.get("LiquiCorrente"), sufixo="x"))
    with cb:
        st.metric("Free Cash Flow",     _fmt_grande(linha.get("freeCashflow")))
        st.metric("Vol. Médio Diário",  _fmt(linha.get("VolMedDiario"), dec=0))
        st.metric("Máx. 52 semanas",    _fmt(linha.get("max_52"), prefixo="R$ "))
        st.metric("Mín. 52 semanas",    _fmt(linha.get("min_52"), prefixo="R$ "))

# Barra de progresso do range de 52 semanas
try:
    preco  = float(linha.get("preco_atual"))
    min_52 = float(linha.get("min_52"))
    max_52 = float(linha.get("max_52"))
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
        noticias_llm = json.loads(linha.get("noticias_json", "[]") or "[]")
    except (json.JSONDecodeError, TypeError):
        noticias_llm = []

st.divider()

# ---------------------------------------------------------------------------
# Cards de notícias com foto e link
# ---------------------------------------------------------------------------
st.subheader("🗞️ Notícias com foto")

try:
    noticias_raw = json.loads(linha.get("noticias_raw_json", "[]") or "[]")
    noticias_llm = json.loads(linha.get("noticias_json", "[]]") or "[]")
except (json.JSONDecodeError, TypeError):
    noticias_raw = []
    noticias_llm = []

renderizar_cards_noticias(ticker)