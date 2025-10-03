# gpt.py
# App Streamlit — Painel Alpargatas sem "Dados_alpa.xlsx"
# Requer: pandas, plotly, streamlit, odfpy (p/ .ods)

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# =========================
# 0) Caminhos robustos
# =========================
APP_DIR  = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "dados"

ARQ_INICIAIS = DATA_DIR / "anos_iniciais.xlsx"
ARQ_FINAIS   = DATA_DIR / "anos_finais.xlsx"
ARQ_MEDIO    = DATA_DIR / "ensino_medio.xlsx"
ARQ_EVASAO   = DATA_DIR / "evasao.ods"         # .ods -> engine="odf"

ALVO_CIDADES = [
    "ALAGOA NOVA",
    "BANANEIRAS",
    "CABACEIRAS",
    "CAMPINA GRANDE",
    # "CAMPINA GRANDE - MIXING CENTER",  # EXCLUÍDO
    "CARPINA",
    "CATURITÉ",
    "GUARABIRA",
    "INGÁ",
    "ITATUBA",
    "JOÃO PESSOA",
    "LAGOA SECA",
    "MOGEIRO",
    "MONTES CLAROS",
    "QUEIMADAS",
    "SANTA RITA",
    "SÃO PAULO",
    "SERRA REDONDA",
]


# =========================
# Utilitários
# =========================
def nrm(x) -> str:
    """Remove acento, vira UPPER e tira espaços; NaN -> ''."""
    if pd.isna(x):
        return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()


def chave_municipio(nome: str) -> str:
    """Chave robusta para casar nomes entre fontes."""
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n:
        n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    return n


def to7(s: pd.Series) -> pd.Series:
    return (
        s.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )


def _check_files_exist():
    req = [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO]
    missing = [p for p in req if not p.exists()]
    if missing:
        st.error("Arquivos de dados não encontrados:")
        for p in missing:
            st.write(f"• {p}")
        if not DATA_DIR.exists():
            st.write("A pasta 'dados/' não existe. Caminho esperado:", str(DATA_DIR))
        else:
            st.write("Conteúdo de 'dados/':", [p.name for p in DATA_DIR.iterdir()])
        st.stop()


def _anos_disponiveis(df: pd.DataFrame, a0=2005, a1=2023) -> list[int]:
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m:
            ano = int(m.group(1))
            if a0 <= ano <= a1:
                anos.append(ano)
    return sorted(set(anos))


def _long_por_municipio_ano(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    """Converte planilha INEP em formato longo por município-ano."""
    t = df.copy()
    if "CO_MUNICIPIO" not in t.columns:
        raise KeyError("Planilha não possui coluna CO_MUNICIPIO.")
    t["CO_MUNICIPIO"] = to7(t["CO_MUNICIPIO"])
    anos = _anos_disponiveis(t, 2005, 2023)
    if not anos:
        return pd.DataFrame(columns=["CO_MUNICIPIO", "ANO", rotulo])
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    for c in cols:
        t[c] = pd.to_numeric(t[c], errors="coerce")
    long_ = t[["CO_MUNICIPIO"] + cols].melt("CO_MUNICIPIO", value_name=rotulo)
    long_["ANO"] = long_["variable"].str.extract(r"(\d{4})").astype(int)
    long_.drop(columns="variable", inplace=True)
    return long_.groupby(["CO_MUNICIPIO", "ANO"], as_index=False)[rotulo].mean()


def media_por_municipio(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    """Média do VL_INDICADOR_REND_2023 por município."""
    out = pd.DataFrame({
        "CO_MUNICIPIO": to7(df["CO_MUNICIPIO"]),
        rotulo: pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce"),
    })
    return out.groupby("CO_MUNICIPIO", as_index=False)[rotulo].mean()


# =========================
# 1) Montagem dos dados
# =========================
@st.cache_data(show_spinner=True)
def build_data():
    _check_files_exist()

    # --- Aprovação (INEP)
    df_ini = pd.read_excel(ARQ_INICIAIS, header=9)
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9)
    df_med = pd.read_excel(ARQ_MEDIO,    header=9)

    # --- Evasão (tem NO_UF / NO_MUNICIPIO)
    df_eva = pd.read_excel(ARQ_EVASAO, header=8, engine="odf")

    # Lookup (código -> UF / Município)
    lookup_cols = ["NO_UF", "CO_MUNICIPIO", "NO_MUNICIPIO"]
    lookup_cols = [c for c in lookup_cols if c in df_eva.columns]
    lookup = df_eva[lookup_cols].copy()
    if "CO_MUNICIPIO" not in lookup.columns:
        st.error("Arquivo de evasão não possui CO_MUNICIPIO para lookup.")
        st.stop()
    lookup["CO_MUNICIPIO"] = to7(lookup["CO_MUNICIPIO"])
    if "NO_MUNICIPIO" in lookup.columns:
        lookup["MUNICIPIO_CHAVE"] = lookup["NO_MUNICIPIO"].apply(chave_municipio)
    else:
        lookup["MUNICIPIO_CHAVE"] = ""

    # --- Aprovação média (2023)
    ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (
        lookup[["NO_UF", "CO_MUNICIPIO", "NO_MUNICIPIO", "MUNICIPIO_CHAVE"]]
        .drop_duplicates("CO_MUNICIPIO")
        .merge(ini, on="CO_MUNICIPIO", how="left")
        .merge(fin, on="CO_MUNICIPIO", how="left")
        .merge(med, on="CO_MUNICIPIO", how="left")
    )

    # --- Evasão total (Fundamental/Médio) — colunas do seu arquivo
    #     Ajuste os nomes se necessário:
    eva_cols = {
        "1_CAT3_CATFUN": "EVASAO_FUNDAMENTAL",
        "1_CAT3_CATMED": "EVASAO_MEDIO",
    }
    eva_pick = ["CO_MUNICIPIO"] + [c for c in eva_cols if c in df_eva.columns]
    eva = df_eva[eva_pick].copy()
    eva["CO_MUNICIPIO"] = to7(eva["CO_MUNICIPIO"])
    for col_src, col_dst in eva_cols.items():
        if col_src in eva.columns:
            eva[col_dst] = pd.to_numeric(
                eva[col_src].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
    eva = eva[["CO_MUNICIPIO"] + list(eva_cols.values())]
    base = base.merge(eva, on="CO_MUNICIPIO", how="left")

    # --- reprovação e urgência
    base["Reprovacao_Iniciais"] = (1 - pd.to_numeric(base["TAXA_APROVACAO_INICIAIS"], errors="coerce")) * 100
    base["Reprovacao_Finais"]   = (1 - pd.to_numeric(base["TAXA_APROVACAO_FINAIS"],  errors="coerce")) * 100
    for c in ["EVASAO_FUNDAMENTAL", "EVASAO_MEDIO", "Reprovacao_Iniciais", "Reprovacao_Finais"]:
        base[c] = pd.to_numeric(base[c], errors="coerce")
    base["Urgencia"] = base[["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1, skipna=True)

    # --- filtro por cidades-alvo (sem MIXING CENTER)
    alvo = pd.DataFrame({"ALVO_CHAVE": [chave_municipio(x) for x in ALVO_CIDADES]})
    base["ALVO_CHAVE"] = base["NO_MUNICIPIO"].apply(chave_municipio)
    base = base.merge(alvo, left_on="ALVO_CHAVE", right_on="ALVO_CHAVE", how="inner")

    # --- evolução histórica (média entre etapas por ano) somente para as cidades-alvo
    evo_ini = _long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_med, "APROVACAO_MEDIO")
    evolucao = (
        evo_ini.merge(evo_fin, on=["CO_MUNICIPIO", "ANO"], how="outer")
               .merge(evo_med, on=["CO_MUNICIPIO", "ANO"], how="outer")
    )
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
        ["APROVACAO_INICIAIS", "APROVACAO_FINAIS", "APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)
    evolucao = evolucao.merge(base[["CO_MUNICIPIO","NO_UF","NO_MUNICIPIO"]].drop_duplicates(), on="CO_MUNICIPIO", how="left")
    evolucao = evolucao[evolucao["NO_MUNICIPIO"].notna()].copy()

    return base.reset_index(drop=True), evolucao.reset_index(drop=True)


# =========================
# 2) UI
# =========================
st.set_page_config(page_title="IA • Aprovação/Evasão", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

with st.spinner("Processando dados…"):
    base, evolucao = build_data()

# ---------------- KPIs ----------------
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Municípios (alvo)", f"{base['CO_MUNICIPIO'].nunique()}")
with c2:
    st.metric("Aprovação — Finais (média)",
              f"{(pd.to_numeric(base['TAXA_APROVACAO_FINAIS'], errors='coerce').mean()*100):.1f}%")
with c3:
    st.metric("Evasão — Fundamental (média)", f"{base['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4:
    st.metric("Urgência — média", f"{base['Urgencia'].mean():.1f}")

# ---------------- Abas ----------------
tab1, tab2, tab3 = st.tabs(["🔎 Tabelas", "📈 Gráficos", "⚙️ Diagnóstico"])

with tab1:
    st.subheader("Urgentes (Top 20 por urgência)")
    cols_u = ["NO_UF","NO_MUNICIPIO","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
              "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"]
    st.dataframe(
        base.sort_values("Urgencia", ascending=False)[cols_u].head(20),
        use_container_width=True
    )

    st.subheader("Evolução — aprovação (municípios-alvo)")
    show_cols = ["NO_UF","NO_MUNICIPIO","ANO",
                 "APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]
    st.dataframe(evolucao[show_cols].sort_values(["NO_UF","NO_MUNICIPIO","ANO"]),
                 use_container_width=True)

with tab2:
    st.subheader("Tendência geral (média do recorte) — aprovação")
    tmp = evolucao.copy()
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce") * 100.0
    m = tmp.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean()
    fig1 = px.line(
        m.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
        x="ANO", y="Aprovação (%)", color="Etapa", markers=True
    )
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Gap — Iniciais − Finais (p.p.)")
    gap = (tmp.groupby("ANO")[["APROVACAO_INICIAIS","APROVACAO_FINAIS"]].mean())
    gap["GAP"] = gap["APROVACAO_INICIAIS"] - gap["APROVACAO_FINAIS"]
    fig2 = px.line(gap.reset_index(), x="ANO", y="GAP", markers=True)
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Top 10 — Urgência (soma: evasões + reprovações)")
    top10 = (base.groupby("NO_MUNICIPIO", as_index=False)["Urgencia"].mean()
                  .sort_values("Urgencia", ascending=False).head(10))
    st.plotly_chart(px.bar(top10, x="NO_MUNICIPIO", y="Urgencia"), use_container_width=True)

with tab3:
    st.write("**Shapes**")
    st.write("base:", base.shape, "| evolução:", evolucao.shape)
    st.write("Colunas (base):")
    st.code(", ".join(base.columns))
    st.write("Arquivos lidos de:")
    st.code("\n".join(str(p) for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO]))
