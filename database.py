"""
database.py
Schema SQLite e funções de acesso ao banco.
 
Modelagem:
  empresas   → dados ESTÁTICOS  (nome, setor, descrição)  — 1 registro por ticker
  snapshots  → dados DINÂMICOS  (preço, indicadores)       — 1 por execução do pipeline
  noticias   → dados DINÂMICOS  (artigos coletados)        — N por snapshot
  relatorios → dados DINÂMICOS  (análise LLM)              — 1 por snapshot
 
Garantias:
  - Rodadas subsequentes NUNCA sobrescrevem dados anteriores
  - Todo snapshot tem timestamp de coleta
  - Dashboard pode consultar histórico por ticker e período
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

DB_PATH = Path("hipotese_capital.db")

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL") 
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    """Inicializa o esquema do banco de dados."""
    with get_conn() as conn:
        # Dados Estáticos
        conn.execute('''
            CREATE TABLE IF NOT EXISTS empresas (
                ticker TEXT PRIMARY KEY,
                nome TEXT,
                setor TEXT,
                segAtuacao TEXT,
                descricao TEXT
            )
        ''')
        # Dados Temporais
        conn.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                data_coleta TEXT,
                preco_atual REAL,
                variacao_dia REAL,
                debtToEquity REAL,
                freeCashflow REAL,
                ebitdaMargins REAL,
                LiquiCorrente REAL,
                VolMedDiario REAL,
                MargemOperacional REAL,
                min_52 REAL,
                max_52 REAL,
                pl REAL,
                roe REAL,
                dy REAL,
                market_cap REAL,
                beta REAL,
                resumo_llm TEXT,
                analise_llm TEXT,
                perguntas_json TEXT
            )
        ''')
        # Notícias
        conn.execute('''
            CREATE TABLE IF NOT EXISTS noticias_historico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                data_noticia TEXT,
                titulo TEXT,
                fonte TEXT,
                sentimento TEXT,
                url TEXT,
                imagem TEXT,
                descricao TEXT,
                FOREIGN KEY(ticker) REFERENCES empresas(ticker)
            )
        ''')
        # Portfolio — lista de tickers ativos para o pipeline
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio (
                ticker        TEXT PRIMARY KEY,
                adicionado_em TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
                ativo         INTEGER NOT NULL DEFAULT 1
            )
        """)

# ---------------------------------------------------------------------------
# Funções de gerenciamento do portfólio
# ---------------------------------------------------------------------------

def listar_portfolio() -> list:
    """Retorna a lista de tickers ativos no portfólio."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ticker FROM portfolio WHERE ativo = 1 ORDER BY ticker"
        ).fetchall()
    return [r["ticker"] for r in rows]


def adicionar_ticker(ticker: str) -> tuple:
    """
    Adiciona um ticker ao portfólio.
    Retorna (sucesso: bool, mensagem: str).
    """
    ticker = ticker.upper().strip()

    if not ticker or len(ticker) < 4 or len(ticker) > 6:
        return False, f"Ticker '{ticker}' inválido — deve ter entre 4 e 6 caracteres."

    with get_conn() as conn:
        row = conn.execute(
            "SELECT ativo FROM portfolio WHERE ticker = ?", (ticker,)
        ).fetchone()

        if row:
            if row["ativo"] == 1:
                return False, f"**{ticker}** já está no portfólio."
            else:
                conn.execute(
                    "UPDATE portfolio SET ativo = 1, adicionado_em = datetime('now') WHERE ticker = ?",
                    (ticker,)
                )
                return True, f"**{ticker}** reativado no portfólio."
        else:
            conn.execute("INSERT INTO portfolio (ticker) VALUES (?)", (ticker,))
            return True, f"**{ticker}** adicionado ao portfólio."


def remover_ticker(ticker: str) -> tuple:
    """
    Remove (desativa) um ticker do portfólio.
    Não apaga dados históricos — apenas marca como inativo.
    Retorna (sucesso: bool, mensagem: str).
    """
    ticker = ticker.upper().strip()

    with get_conn() as conn:
        row = conn.execute(
            "SELECT ativo FROM portfolio WHERE ticker = ?", (ticker,)
        ).fetchone()

        if not row or row["ativo"] == 0:
            return False, f"**{ticker}** não está no portfólio."

        conn.execute("UPDATE portfolio SET ativo = 0 WHERE ticker = ?", (ticker,))

    return True, f"**{ticker}** removido. Dados históricos preservados."


def ticker_tem_dados(ticker: str) -> bool:
    """Verifica se o ticker já tem snapshots no banco."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as n FROM snapshots WHERE ticker = ?", (ticker,)
        ).fetchone()
    return row["n"] > 0