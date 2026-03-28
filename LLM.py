from __future__ import annotations

import json
import os
import re
import time

import pandas as pd
from groq import Groq

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
Você é um analista sênior de renda variável com 15 anos de experiência, \
especializado em value investing à brasileira. Trabalha para a Hipótese Capital, \
gestora de ações concentrada com R$ 1,2 bilhão sob gestão.

FILOSOFIA DA CASA:
- Poucas posições, convicção alta, horizonte de 3 a 5 anos
- Qualidade do negócio antes de valuation barato
- Proteção de downside é prioridade
- Ceticismo com narrativas de curto prazo
- Preferência por empresas com moat, gestão alocadora e geração de caixa consistente

SEU ESTILO:
- Direto e objetivo — quem lê tem 15 anos de mercado
- Interprete os indicadores, não apenas os repita
- Se os números são ruins, diga que são ruins
- Relacione indicadores entre si (ROE alto com dívida alta é diferente sem dívida)
- Use dados de 52 semanas e beta para contextualizar momento e volatilidade
- Responda SEMPRE em JSON válido, sem markdown, sem texto fora do JSON\
"""


def construir_prompt(linha: pd.Series) -> str:
    preco  = linha.get("preco_atual")
    min_52 = linha.get("Mínima 52 Semanas")
    max_52 = linha.get("Máxima 52 Semanas")
    pos_52s = "N/D"
    if all(v is not None and not pd.isna(v) for v in [preco, min_52, max_52]):
        try:
            rng = float(max_52) - float(min_52)
            if rng > 0:
                pos_52s = f"{(float(preco) - float(min_52)) / rng * 100:.0f}% do range"
        except (TypeError, ValueError):
            pass

    # Só envia indicadores com valor real — evita tokens desperdiçados com N/D
    ind = {
        "P/L":        linha.get("P/L"),
        "ROE":        linha.get("ROE"),
        "Div/Eq":     linha.get("debtToEquity"),
        "DY":         linha.get("dividendYield"),
        "FCF":        linha.get("freeCashflow"),
        "Mrg.EBITDA": linha.get("ebitdaMargins"),
        "Mrg.Oper":   linha.get("Margem Operacional"),
        "Liq.Corr":   linha.get("Liqui Corrente"),
    }
    ind_str = " | ".join(f"{k}:{v}" for k, v in ind.items() if v != "N/D")

    return (
        f"Empresa: {linha.get('ticker')} | {linha.get('nome')} | {linha.get('setor')}\n"
        f"Negócio: {str(linha.get('descricao', ''))[:300]}\n"
        f"Cotação: R${preco} {linha.get('variacao_dia')} | "
        f"MCap:{linha.get('market_cap')} | Beta:{linha.get('Beta')} | Range52s:{pos_52s}\n"
        f"Fundamentos: {ind_str}\n"
        f"Notícias:\n{linha.get('noticias')}\n\n"
        f"Retorne APENAS este JSON preenchido:\n"
        f'{{"resumo_negocio":"2-3 frases sobre modelo de negócio e geração de valor",'
        f'"analise_fundamentos":"interpretação dos indicadores em conjunto — qualidade, risco e momento",'
        f'"noticias":{{"positivas":[],"negativas":[],"neutras":[]}},'
        f'"sentimentos":{{}},'
        f'"perguntas_analista":["pergunta1","pergunta2","pergunta3"]}}'
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

    relatorio

    if not relatorio.rstrip().endswith("}"):
        print(f"❌ [{ticker}] Resposta truncada: ...{relatorio[-80:]}")
        return {"erro": "resposta truncada", "ticker": ticker}

    try:
        data = json.loads(relatorio)
    except json.JSONDecodeError as exc:
        print(f"❌ [{ticker}] JSON inválido: {exc} | trecho: {relatorio[:200]}")
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
            "noticias_json":       json.dumps(rel.get("noticias", {}),            ensure_ascii=False),
            "sentimentos_json":    json.dumps(rel.get("sentimentos", {}),          ensure_ascii=False),
            "perguntas_json":      json.dumps(rel.get("perguntas_analista", []),  ensure_ascii=False),
            "erro":                rel.get("erro", ""),
        })
        time.sleep(pausa)
    return pd.DataFrame(relatorios)