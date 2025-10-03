# gpt.py
import unicodedata, re
from typing import Iterable
import pandas as pd
import plotly.express as px
import streamlit as st

# =========================================================
# 0) Arquivos (sempre dentro da pasta 'dados/')
# =========================================================
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_MEDIO    = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"     # .ods -> engine="odf"

# =========================================================
# 1) Parâmetros de cidades/UFs
# =========================================================
CIDADES_ALP = [
    "ALAGOA NOVA","BANANEIRAS","CABACEIRAS","CAMPINA GRANDE",
    "CARPINA","CATURITÉ","GUARABIRA","INGÁ","ITATUBA","JOÃO PESSOA",
    "LAGOA SECA","MOGEIRO","MONTES CLAROS","QUEIMADAS","SANTA RITA",
    "SÃO PAULO","SERRA REDONDA",
]
UFS_ALVO = {"PB","PE","SP","MG"}  # restringe homônimos de outros estados

# =========================================================
# 2) Utilitários
# =========================================================
def nrm(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII","ignore").decode("ASCII")
    return s.upper().strip()

CIDADES_NORM = {nrm(c) for c in CIDADES_ALP} - {nrm(x) for x in EXCLUIR}

def _anos_disponiveis(df: pd.DataFrame, a0=2005, a1=2023) -> list[int]:
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m:
            a = int(m.group(1))
            if a0 <= a <= a1:
                anos.append(a)
    return sorted(set(anos))

def media_por_municipio_2023(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    tmp = df.copy()
    tmp["CO_MUNICIPIO"] = (
        tmp["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    out = (
        pd.DataFrame({
            "CO_MUNICIPIO": tmp["CO_MUNICIPIO"],
            rotulo: pd.to_numeric(tmp["VL_INDICADOR_REND_2023"], errors="coerce")
        })
        .groupby("CO_MUNICIPIO", as_index=False)[rotulo].mean()
    )
    return out

def long_por_municipio_ano(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    tmp = df.copy()
    tmp["CO_MUNICIPIO"] = (
        tmp["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    anos = _anos_disponiveis(tmp)
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    for c in cols:
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
    long_df = tmp.melt(
        id_vars=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"],
        value_vars=cols,
        var_name="COL",
        value_name=rotulo
    )
    long_df["ANO"] = long_df["COL"].str.extract(r"(\d{4})").astype(int)
    long_df = long_df.drop(columns=["COL"])
    # média por município/ano (se houver linhas duplicadas)
    long_df = (long_df.groupby(["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","ANO"], as_index=False)[rotulo]
                     .mean())
    return long_df

def filtra_cidades(df: pd.DataFrame) -> pd.DataFrame:
    """Mantém apenas cidades da lista + UFs alvo."""
    base = (df[["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"]]
              .dropna()
              .copy())
    base["NO_MUNICIPIO_NORM"] = base["NO_MUNICIPIO"].apply(nrm)
    base["CO_MUNICIPIO"] = (
        base["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    base = base[
        base["NO_MUNICIPIO_NORM"].isin(CIDADES_NORM) &
        base["NO_UF"].isin(list(UFS_ALVO))
    ]
    return base.drop_duplicates(["CO_MUNICIPIO"]).reset_index(drop=True)

def read_ods(path: str, **kw) -> pd.DataFrame:
    return pd.read_excel(path, engine="odf", **kw)

# =========================================================
# 3) Construção dos dados
# =========================================================
@st.cache_data(show_spinner=True)
def build_data():
    # ---- Leitura
    df_ini = pd.read_excel(ARQ_INICIAIS, header=9)
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9)
    df_med = pd.read_excel(ARQ_MEDIO,    header=9)
    df_eva = read_ods(ARQ_EVASAO,        header=8)

    # ---- Base de municípios-alvo (a partir de qualquer uma das planilhas)
    base_munis = filtra_cidades(df_ini)

    # ---- Aprovação 2023
    ini = media_por_municipio_2023(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio_2023(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio_2023(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base_munis
            .merge(ini, on="CO_MUNICIPIO", how="left")
            .merge(fin, on="CO_MUNICIPIO", how="left")
            .merge(med, on="CO_MUNICIPIO", how="left"))

    # Percentuais para exibição
    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
        if c in base.columns:
            base[c+"_PERC"] = (base[c] * 100).round(2)

    base["TAXA_APROVACAO_MEDIA"] = base[
        ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)
    base["TAXA_APROVACAO_MEDIA_PERC"] = (base["TAXA_APROVACAO_MEDIA"]*100).round(2)

    # ---- Evasão (apenas totais de Fundamental e Médio)
    cols_eva = ["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","1_CAT3_CATFUN","1_CAT3_CATMED"]
    eva = df_eva[[c for c in cols_eva if c in df_eva.columns]].copy()
    eva["CO_MUNICIPIO"] = (
        eva["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )

    # converter "1,23" → 1.23
    for c in ["1_CAT3_CATFUN","1_CAT3_CATMED"]:
        if c in eva.columns:
            eva[c] = pd.to_numeric(eva[c].astype(str).str.replace(",", ".", regex=False), errors="coerce")

    eva = eva.rename(columns={
        "1_CAT3_CATFUN": "EVASAO_FUNDAMENTAL",
        "1_CAT3_CATMED": "EVASAO_MEDIO"
    })

    base = base.merge(eva[["CO_MUNICIPIO","EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]],
                      on="CO_MUNICIPIO", how="left")

    # Reprovação aproximada (100 - aprovação) e "Urgência"
    base["REPROV_INICIAIS"] = (1 - base["TAXA_APROVACAO_INICIAIS"]) * 100
    base["REPROV_FINAIS"]   = (1 - base["TAXA_APROVACAO_FINAIS"])   * 100
    base["URGENCIA"] = (
        base["EVASAO_FUNDAMENTAL"].fillna(0) +
        base["EVASAO_MEDIO"].fillna(0) +
        base["REPROV_INICIAIS"].fillna(0) +
        base["REPROV_FINAIS"].fillna(0)
    ).round(2)

    # ---- Evolução (séries por ano)
    evo_ini = long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = long_por_municipio_ano(df_med, "APROVACAO_MEDIO")

    evolucao = (evo_ini
                .merge(evo_fin, on=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","ANO"], how="outer")
                .merge(evo_med, on=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","ANO"], how="outer"))

    # mantém só municípios da base
    evolucao = evolucao.merge(base[["CO_MUNICIPIO"]], on="CO_MUNICIPIO", how="inner")
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
        ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]:
        evolucao[c+"_PERC"] = (evolucao[c]*100).round(2)

    # ordena e retorna
    base = base.sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True)
    evolucao = evolucao.sort_values(["NO_UF","NO_MUNICIPIO","ANO"]).reset_index(drop=True)
    return base, evolucao

# =========================================================
# 4) App
# =========================================================
st.set_page_config(page_title="Instituto Alpargatas — Painel", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

with st.spinner("Processando dados…"):
    base, evolucao = build_data()

# ---------------- KPIs ----------------
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios", f"{base['CO_MUNICIPIO'].nunique()}")
with c2: st.metric("Aprovação média 2023", f"{base['TAXA_APROVACAO_MEDIA_PERC'].mean():.1f}%")
top_ap = base.sort_values("TAXA_APROVACAO_MEDIA_PERC", ascending=False).iloc[0]
with c3: st.metric("Maior aprovação (2023)", f"{top_ap['TAXA_APROVACAO_MEDIA_PERC']:.1f}%", top_ap["NO_MUNICIPIO"])
top_u = base.sort_values("URGENCIA", ascending=False).iloc[0]
with c4: st.metric("Maior urgência", f"{top_u['URGENCIA']:.1f}", top_u["NO_MUNICIPIO"])

st.divider()

# ---------------- Filtros ----------------
ufs = sorted(base["NO_UF"].unique())
uf_sel = st.multiselect("UF", ufs, default=ufs)
munis = base[base["NO_UF"].isin(uf_sel)]
muni_opts = munis["NO_MUNICIPIO"].tolist()
muni_sel = st.multiselect("Municípios", muni_opts, default=muni_opts)

base_f = base[base["NO_MUNICIPIO"].isin(muni_sel) & base["NO_UF"].isin(uf_sel)]
evo_f  = evolucao[evolucao["NO_MUNICIPIO"].isin(muni_sel) & evolucao["NO_UF"].isin(uf_sel)]

# ---------------- Tabela ----------------
st.subheader("📄 Tabela principal")
cols_show = [
    "NO_UF","NO_MUNICIPIO","CO_MUNICIPIO",
    "TAXA_APROVACAO_INICIAIS_PERC","TAXA_APROVACAO_FINAIS_PERC","TAXA_APROVACAO_MEDIO_PERC",
    "TAXA_APROVACAO_MEDIA_PERC",
    "EVASAO_FUNDAMENTAL","EVASAO_MEDIO","REPROV_INICIAIS","REPROV_FINAIS","URGENCIA"
]
st.dataframe(base_f[cols_show].rename(columns={
    "TAXA_APROVACAO_INICIAIS_PERC":"Aprov. Iniciais (%)",
    "TAXA_APROVACAO_FINAIS_PERC":"Aprov. Finais (%)",
    "TAXA_APROVACAO_MEDIO_PERC":"Aprov. Médio (%)",
    "TAXA_APROVACAO_MEDIA_PERC":"Aprov. Média (%)",
    "EVASAO_FUNDAMENTAL":"Evasão Fund.",
    "EVASAO_MEDIO":"Evasão Médio",
    "REPROV_INICIAIS":"Reprov. Iniciais",
    "REPROV_FINAIS":"Reprov. Finais",
}), use_container_width=True, hide_index=True)

# ---------------- Gráficos: Ranking ----------------
st.subheader("🏅 Rankings")
colA, colB = st.columns(2)

rank_ap = (base_f.sort_values("TAXA_APROVACAO_MEDIA_PERC", ascending=False)
                 .head(15))
figA = px.bar(rank_ap,
              x="TAXA_APROVACAO_MEDIA_PERC", y="NO_MUNICIPIO",
              orientation="h",
              color="NO_UF",
              labels={"TAXA_APROVACAO_MEDIA_PERC":"Aprovação média 2023 (%)",
                      "NO_MUNICIPIO":"Município"})
figA.update_yaxes(categoryorder="total ascending")
colA.plotly_chart(figA, use_container_width=True)

rank_u = (base_f.sort_values("URGENCIA", ascending=False).head(15))
figB = px.bar(rank_u,
              x="URGENCIA", y="NO_MUNICIPIO",
              orientation="h",
              color="NO_UF",
              labels={"URGENCIA":"Urgência (Evasão + Reprovação)",
                      "NO_MUNICIPIO":"Município"})
figB.update_yaxes(categoryorder="total ascending")
colB.plotly_chart(figB, use_container_width=True)

# ---------------- Gráfico: Evolução ----------------
st.subheader("📈 Evolução temporal (média das etapas)")
if not evo_f.empty:
    # média por município/ano (em %)
    evo_plot = (evo_f.groupby(["NO_MUNICIPIO","ANO"], as_index=False)["APROVACAO_MEDIA_GERAL_PERC"].mean())
    figL = px.line(evo_plot, x="ANO", y="APROVACAO_MEDIA_GERAL_PERC",
                   color="NO_MUNICIPIO",
                   markers=True,
                   labels={"APROVACAO_MEDIA_GERAL_PERC":"Aprovação média (%)"})
    st.plotly_chart(figL, use_container_width=True)
else:
    st.info("Selecione ao menos um município para ver a evolução.")

