from __future__ import annotations

import json
import os
import re
import time

import pandas as pd
from groq import Groq
from tenacity import retry, stop_after_attempt, wait_exponential

from database import get_conn, init_db

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
LLM_MODEL    = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")  # melhor qualidade
LLM_RETRIES  = int(os.getenv("LLM_MAX_RETRIES", "3"))

# Modelos disponíveis no Groq (free tier):
#   llama-3.3-70b-versatile  → melhor qualidade, ~6k tokens/min  ← recomendado
#   llama-3.1-8b-instant     → mais rápido, limite maior
#   mixtral-8x7b-32768       → bom para textos longos

client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# ---------------------------------------------------------------------------
# CAMADA 1 — System prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """\
Você é o analista-chefe da Hipótese Capital, gestora de ações concentrada com \
R$ 1,2 bilhão sob gestão e portfólio de 8 a 12 posições. Sua função é produzir \
análises que orientem decisões reais de alocação com horizonte de 3 a 5 anos.
 
FRAMEWORK DE ANÁLISE — siga esta ordem de raciocínio:
 
1. QUALIDADE DO NEGÓCIO (avalie antes dos números):
   - A empresa tem vantagem competitiva durável? (pricing power, switching cost, escala)
   - O modelo gera caixa de forma previsível ou é dependente de ciclo econômico?
   - A gestão aloca capital com disciplina? (ROIC consistente, histórico de M&A, política de dividendos)
 
2. SAÚDE FINANCEIRA (use os indicadores para confirmar ou refutar a qualidade):
   - ROE > 15% sustentado indica negócio de qualidade; abaixo de 10% exige justificativa clara
   - Dívida/Equity > 2x em setor cíclico é risco estrutural — sinalize obrigatoriamente
   - FCF positivo e crescente é condição necessária para tese de longo prazo
   - Liquidez Corrente < 1.0 sinaliza pressão de curto prazo — cite sempre que ocorrer
   - Compare Margem Operacional vs Margem EBITDA: diferença grande indica despesa financeira pesada
 
3. VALUATION E MOMENTO (contextualize o preço):
   - P/L isolado não diz nada; relacione com ROE e perspectiva de crescimento
   - Preço acima de 80% do range de 52 semanas exige margem de segurança maior para entrada
   - Beta > 1.5 amplifica risco de drawdown em cenário de juros altos — sinalize
 
4. CATALISADORES E RISCOS (o que pode mudar a tese):
   - Separe notícias que alteram fundamentos de longo prazo do ruído de curto prazo
   - Identifique riscos que os números ainda não mostram (regulatório, competitivo, execução)
 
RESTRIÇÕES DE QUALIDADE — violações invalidam a análise:
- NUNCA repita os números literalmente — interprete o que eles significam
- NUNCA produza análise genérica que se aplicaria a qualquer empresa do setor
- NUNCA suavize um problema — se o número é ruim, diga que é ruim e qual o risco
- Se um dado estiver ausente (N/D), ignore-o em vez de especular
- As perguntas para o analista devem ser ESPECÍFICAS para esta empresa
 
FORMATO: responda SOMENTE em JSON válido, sem markdown, sem texto fora do JSON.\
"""


# ---------------------------------------------------------------------------
# CAMADA 2 — Formatação
# ---------------------------------------------------------------------------

def _fmt(val, sufixo: str = "",prefixo="", dec: int = 2) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/D"
    try:
        return f"{prefixo}{float(val):.{dec}f}{sufixo}"
    except (TypeError, ValueError):
        return "N/D"


def _fmt_grande(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/D"
    try:
        v = float(val)
        if abs(v) >= 1e9:
            return f"R$ {v/1e9:.2f}B"
        if abs(v) >= 1e6:
            return f"R$ {v/1e6:.1f}M"
        return f"R$ {v:.0f}"
    except (TypeError, ValueError):
        return "N/D"


def _variacao_fmt(val) -> str:

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/D"
    
    try:
        v = float(val)
        return f"{'+'if v >= 0 else ''}{v:.2f}%"
    except (TypeError, ValueError):
        return "N/D"


def _formatar_noticias(noticias_raw) -> str:

    if not noticias_raw:
        return "Nenhuma notícia disponível."
    
    if isinstance(noticias_raw, str):
        return noticias_raw if noticias_raw.strip() else "Nenhuma notícia disponível."
    
    if isinstance(noticias_raw, list):
        linhas = []
        for i, a in enumerate(noticias_raw[:5], 1):
            titulo = a.get("title", "Sem título")
            fonte  = a.get("source", {}).get("name", "?")
            data   = a.get("publishedAt", "")[:10]
            linhas.append(f"{i}. [{data} | {fonte}] {titulo}")
        return "\n".join(linhas) if linhas else "Nenhuma notícia disponível."
    
    return "Nenhuma notícia disponível."


def _extrair_json(texto: str) -> str:
    """Remove markdown ao redor do JSON se o modelo inserir cercas de código."""

    texto = texto.strip()
    if texto.startswith("```"):
        linhas = texto.splitlines()
        linhas = linhas[1:] if linhas[0].startswith("```") else linhas
        linhas = linhas[:-1] if linhas and linhas[-1].strip() == "```" else linhas
        texto = "\n".join(linhas).strip()
    return texto


# ---------------------------------------------------------------------------
# CAMADA 2 — User prompt compacto
# ---------------------------------------------------------------------------

def construir_prompt(ticker: str) -> str:
    with get_conn() as conn:
        # 1. Busca dados estáticos e o snapshot mais recente
        query = '''
            SELECT e.*, s.*
            FROM empresas e
            JOIN snapshots s ON e.ticker = s.ticker
            WHERE e.ticker = ?
            ORDER BY s.data_coleta DESC
            LIMIT 1
        '''
        dados = conn.execute(query, (ticker,)).fetchone()
        
        if not dados:
            return f"Erro: Dados não encontrados para o ticker {ticker}."

        preco=dados["preco_atual"]
        min_52=dados["min_52"]
        max_52=dados["max_52"]
        pos_52s = "N/D"

        if all(v is not None and not pd.isna(v) for v in [preco, min_52, max_52]):
            try:
                rng = float(max_52) - float(min_52)
                if rng > 0:
                    percentual = (float(preco) - float(min_52)) / rng * 100
                    pos_52s = f"{percentual:.0f}% do range"
            except (TypeError, ValueError):
                pass

        # 2. Busca as notícias vinculadas a esse ticker
        query_noticias = '''
            SELECT titulo, sentimento 
            FROM noticias_historico 
            WHERE ticker = ? 
            ORDER BY id DESC LIMIT 5
        '''
        noticias = conn.execute(query_noticias, (ticker,)).fetchall()
        noticias_str = "\n".join([f"- {n['titulo']} (Sentimento: {n['sentimento']})" for n in noticias])

    ind = {
        "P/L":         _fmt(dados['pl'], "x"),
        "ROE":        _fmt(dados['roe'], "%"),
        "DY":         _fmt(dados['dy'], "%"),
        "M.Cap":      _fmt_grande(dados['market_cap']),
        "Beta":       _fmt(dados['beta'])
    }
    ind_str = " | ".join(f"{k}:{v}" for k, v in ind.items() if v != "N/D")

    return (
        f"CONTEXTO: Você é o analista-chefe da Hipótese Capital.\n"
        f"ATIVO: {dados['ticker']} | {dados['nome']} | {dados['setor']}\n"
        f"MODELO DE NEGÓCIO: {str(dados['descricao'])[:400]}\n"
        f"MERCADO: Preço R${_fmt(dados['preco_atual'])} | MCap: {ind['M.Cap']} | Beta: {ind['Beta']}Range52s: {pos_52s}\n"
        f"FUNDAMENTOS: {ind_str}\n"
        f"NOTÍCIAS RECENTES:\n{noticias_str}\n\n"
        f"TAREFA: Retorne APENAS um JSON com este formato estrito:\n"
        f"{{\n"
        f'  "resumo_negocio": "2-3 frases sobre o modelo e geração de valor",\n'
        f'  "analise_fundamentos": "análise qualitativa dos indicadores e momento",\n'
        f'  "noticias": [\n'
        f'    {{"titulo": "título da notícia", "sentimento": "positiva/negativa/neutra"}}\n'
        f'  ],\n'
        f'  "perguntas_analista": ["pergunta1", "pergunta2", "pergunta3"]\n'
        f"}}"
    )


# ---------------------------------------------------------------------------
# CAMADA 3 — Chamada ao Groq
# ---------------------------------------------------------------------------

def _extrair_retry_delay(exc: Exception) -> float:
    """Lê o tempo de espera sugerido no erro 429."""
    try:
        match = re.search(r"retry[_ ]in[^0-9]*([0-9]+(?:\.[0-9]+)?)", str(exc), re.IGNORECASE)
        if match:
            return float(match.group(1)) + 2
    except Exception:
        pass
    return 30.0


@retry(
    stop=stop_after_attempt(LLM_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    reraise=True,
)
# Chama o Groq via SDK oficial
def _chamar_groq(prompt: str) -> str:

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        temperature=0.2,
        max_tokens=2048,
    )
    return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Funções públicas
# ---------------------------------------------------------------------------

#Gera relatorio para uma linha do df_final
def analisar_empresa(linha: pd.Series) -> dict:

    ticker = linha.get("ticker", "?")

    if not GROQ_API_KEY:
        print(f"⚠️  [{ticker}] GROQ_API_KEY não configurada.")
        print("     Obtenha gratuitamente em: https://console.groq.com")
        return {"erro": "GROQ_API_KEY ausente", "ticker": ticker}

    prompt = construir_prompt(linha)
    print(f"🤖 [{ticker}] Enviando para Groq ({LLM_MODEL})...")

    try:
        relatorio = _chamar_groq(prompt)
    except Exception as exc:
        msg = str(exc)
        if "429" in msg or "rate_limit" in msg.lower():
            espera = _extrair_retry_delay(exc)
            print(f"⏳ [{ticker}] Rate limit. Aguardando {espera:.0f}s...")
            time.sleep(espera)
            try:
                relatorio = _chamar_groq(prompt)
            except Exception as exc2:
                print(f"❌ [{ticker}] Falhou após aguardar: {exc2}")
                return {"erro": str(exc2), "ticker": ticker}
        else:
            print(f"❌ [{ticker}] Groq falhou: {exc}")
            return {"erro": str(exc), "ticker": ticker}

    relatorio_limpo = _extrair_json(relatorio)

    if not relatorio_limpo.rstrip().endswith("}"):
        print(f"❌ [{ticker}] Resposta truncada: ...{relatorio_limpo[-80:]}")
        return {"erro": "resposta truncada", "ticker": ticker}

    try:
        data = json.loads(relatorio_limpo)
    except json.JSONDecodeError as exc:
        print(f"❌ [{ticker}] JSON inválido: {exc} | trecho: {relatorio_limpo[:200]}")
        return {"erro": "JSON inválido", "ticker": ticker}

    data["ticker"] = ticker
    print(f"✅ [{ticker}] Relatório gerado.")
    return data

# Processa o df_final inteiro e retorna DataFrame com relatórios.
def analisar_lote(df: pd.DataFrame, pausa: float = 2.0) -> pd.DataFrame:

    relatorios = []
    for _, linha in df.iterrows():
        rel = analisar_empresa(linha)
        relatorios.append({
            "ticker":              rel.get("ticker", ""),
            "resumo_negocio":      rel.get("resumo_negocio", ""),
            "analise_fundamentos": rel.get("analise_fundamentos", ""),
            "noticias_json":       json.dumps(rel.get("noticias", {}),ensure_ascii=False),
            "sentimentos_json":    json.dumps(rel.get("sentimentos", {}),ensure_ascii=False),
            "perguntas_json":      json.dumps(rel.get("perguntas_analista", []),ensure_ascii=False),
            "erro":                rel.get("erro", ""),
        })
        time.sleep(pausa)
    return pd.DataFrame(relatorios)