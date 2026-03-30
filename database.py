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
                pl REAL,
                roe REAL,
                dy REAL,
                market_cap REAL,
                beta REAL,
                resumo_llm TEXT,
                analise_llm TEXT,
                perguntas_json TEXT,
                FOREIGN KEY(ticker) REFERENCES empresas(ticker)
            )
        ''')
        # Notícias
        conn.execute('''
            CREATE TABLE IF NOT EXISTS noticias_historico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                data_noticia TEXT,
                titulo TEXT,
                sentimento TEXT,
                url TEXT,
                FOREIGN KEY(ticker) REFERENCES empresas(ticker)
            )
        ''')