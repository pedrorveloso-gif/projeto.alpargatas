# final_alpa.py
import os, re, unicodedata
from pathlib import Path
from typing import Optional, Iterable
import pandas as pd
import plotly.express as px
import streamlit as st

# -------------------- ARQUIVOS (em dados/) --------------------
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_MEDIO    = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"
REQUERIDOS   = [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO]

# -------------------- LISTA DE CIDADES ------------------------
CIDADES_ALP = [
    "ALAGOA NOVA","BANANEIRAS","CABACEIRAS","CAMPINA GRANDE",
    "CARPINA","CATURITÉ","GUARABIRA","INGÁ","ITATUBA","JOÃO PESSOA",
    "LAGOA SECA","MOGEIRO","MONTES CLAROS","QUEIMADAS","SANTA RITA",
    "SÃO PAULO","SERRA REDONDA",
]
# excluir explicitamente esse ponto
EXCLUIR = {"CAMPINA GRANDE - MIXING CENTER"}

def nrm(x) -> str:
    if pd.isna(x): return ""
    s = unicodedata.normalize("NFKD", str(x)).encode("ASCII","ignore").decode("ASCII")
    return s.upper().strip()

CIDADES_NORM = {nrm(c) for c in CIDADES_ALP} - {nrm(x) for x in EXCLUIR}

# -------------------- DIAGNÓSTICO DE ARQUIVOS -----------------
def listar_dados():
    if os.path.isdir("dados"):
        itens = sorted(os.listdir("dados"))
        st.code("\n".join(itens) if itens else "(vazio)", language="text")
    else:
        st.caption("Pasta `dados/` não encontrada.")

def checar_arquivos(paths: Iterable[str]) -> None:
    faltando = [p for p in paths if not os.path.exists(p)]
    st.subheader("📁 Arquivos esperados em `dados/`")
    c1, c2 = st.columns(2)
    with c1:
        for p in paths:
            st.write(("✅ " if os.path.exists(p) else "❌ ") + p)
    with c2:
        st.write("**Conteúdo em `dados/`:**")
        listar_dados()
    if faltando:
        st.error("Há arquivo(s) faltando. Suba com os **nomes exatos** acima e recarregue.")
        st.stop()

# -------------------- LEITURA ROBUSTA -------------------------
COLS_CHAVE = {"NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"}

def _tenta_header_xlsx(path: str, header_linha: int) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_excel(path, header=header_linha, engine="openpyxl")
        if COLS_CHAVE.issubset(df.columns):
            return df
    except Exception:
        pass
    return None

def ler_planilha_inep(path: str, prefer: int = 9) -> pd.DataFrame:
    df = _tenta_header_xlsx(path, prefer)
    if df is not None:
        return df
    for h in range(0, 30):
        df = _tenta_header_xlsx(path, h)
        if df is not None:
            st.info(f"`{Path(path).name}`: cabeçalho auto-detectado na linha {h}.")
            return df
    st.error(f"`{Path(path).name}`: não encontrei colunas {sorted(COLS_CHAVE)}.")
    st.stop()
    return pd.DataFrame()

def ler_ods_evasao(path: str, prefer: int = 8) -> pd.DataFrame:
    # tenta header preferido
    try:
        df = pd.read_excel(path, engine="odf", header=prefer)
        if {"NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"}.issubset(df.columns):
            return df
    except Exception:
        pass
    # varre
    for h in range(0, 30):
        try:
            df = pd.read_excel(path, engine="odf", header=h)
            if {"NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"}.issubset(df.columns):
                st.info(f"`{Path(path).name}`: cabeçalho auto-detectado na linha {h}.")
                return df
        except Exception:
            continue
    st.warning("Não consegui ler `evasao.ods`. O painel seguirá sem evasão.")
    return pd.DataFrame(columns=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"])

# -------------------- TRANSFORMAÇÕES --------------------------
def anos_disponiveis(df: pd.DataFrame, a0=2005, a1=2023) -> list[int]:
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m:
            a = int(m.group(1))
            if a0 <= a <= a1: anos.append(a)
    return sorted(set(anos))

def filtra_cidades(df_ref: pd.DataFrame) -> pd.DataFrame:
    base = df_ref[["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"]].dropna().copy()
    base["CO_MUNICIPIO"] = (
        base["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    base["NM_NORM"] = base["NO_MUNICIPIO"].apply(nrm)
    # mantém lista oficial e remove qualquer coisa com "MIXING CENTER"
    base = base[ base["NM_NORM"].isin(CIDADES_NORM) ]
    base = base[ ~base["NM_NORM"].str.contains("MIXING CENTER", na=False) ]
    return base.drop_duplicates(["CO_MUNICIPIO"]).reset_index(drop=True)

def media_2023(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    if "VL_INDICADOR_REND_2023" not in df.columns:
        return pd.DataFrame(columns=["CO_MUNICIPIO", rotulo])
    tmp = df.copy()
    tmp["CO_MUNICIPIO"] = (
        tmp["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    out = (pd.DataFrame({
            "CO_MUNICIPIO": tmp["CO_MUNICIPIO"],
            rotulo: pd.to_numeric(tmp["VL_INDICADOR_REND_2023"], errors="coerce")
        }).groupby("CO_MUNICIPIO", as_index=False)[rotulo].mean())
    return out

def long_por_ano(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    anos = anos_disponiveis(df)
    if not anos:
        return pd.DataFrame(columns=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","ANO",rotulo])
    tmp = df.copy()
    tmp["CO_MUNICIPIO"] = (
        tmp["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    for a in anos:
        c = f"VL_INDICADOR_REND_{a}"
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
    long_df = tmp.melt(
        id_vars=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO"],
        value_vars=[f"VL_INDICADOR_REND_{a}" for a in anos],
        var_name="COL", value_name=rotulo
    )
    long_df["ANO"] = long_df["COL"].str.extract(r"(\d{4})").astype(int)
    long_df = long_df.drop(columns=["COL"])
    return (long_df.groupby(["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","ANO"], as_index=False)[rotulo]
                  .mean())

# -------------------- CACHE PRINCIPAL -------------------------
@st.cache_data(show_spinner=True)
def build_data():
    checar_arquivos(REQUERIDOS)

    df_ini = ler_planilha_inep(ARQ_INICIAIS)
    df_fin = ler_planilha_inep(ARQ_FINAIS)
    df_med = ler_planilha_inep(ARQ_MEDIO)
    df_eva = ler_ods_evasao(ARQ_EVASAO)

    base_ref = filtra_cidades(df_ini)

    ini = media_2023(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_2023(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_2023(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base_ref
            .merge(ini, on="CO_MUNICIPIO", how="left")
            .merge(fin, on="CO_MUNICIPIO", how="left")
            .merge(med, on="CO_MUNICIPIO", how="left"))

    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
        if c in base.columns:
            base[c+"_PERC"] = (base[c]*100).round(2)

    base["TAXA_APROVACAO_MEDIA"] = base[
        ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)
    base["TAXA_APROVACAO_MEDIA_PERC"] = (base["TAXA_APROVACAO_MEDIA"]*100).round(2)

    # Evasão
    if not df_eva.empty:
        cols = ["CO_MUNICIPIO","1_CAT3_CATFUN","1_CAT3_CATMED"]
        cols = [c for c in cols if c in df_eva.columns]
        eva = df_eva[cols].copy()
        if "CO_MUNICIPIO" in eva.columns:
            eva["CO_MUNICIPIO"] = (
                eva["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
            )
        for c in ("1_CAT3_CATFUN","1_CAT3_CATMED"):
            if c in eva.columns:
                eva[c] = pd.to_numeric(eva[c].astype(str).str.replace(",", ".", regex=False), errors="coerce")
        eva = eva.rename(columns={"1_CAT3_CATFUN":"EVASAO_FUNDAMENTAL","1_CAT3_CATMED":"EVASAO_MEDIO"})
        base = base.merge(eva, on="CO_MUNICIPIO", how="left")
    else:
        base["EVASAO_FUNDAMENTAL"] = pd.NA
        base["EVASAO_MEDIO"] = pd.NA

    base["REPROV_INICIAIS"] = (1 - base["TAXA_APROVACAO_INICIAIS"]) * 100
    base["REPROV_FINAIS"]   = (1 - base["TAXA_APROVACAO_FINAIS"])   * 100
    base["URGENCIA"] = (
        base["EVASAO_FUNDAMENTAL"].fillna(0) +
        base["EVASAO_MEDIO"].fillna(0) +
        base["REPROV_INICIAIS"].fillna(0) +
        base["REPROV_FINAIS"].fillna(0)
    ).round(2)

    # Evolução
    evo_ini = long_por_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = long_por_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = long_por_ano(df_med, "APROVACAO_MEDIO")
    evolucao = (evo_ini
                .merge(evo_fin, on=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","ANO"], how="outer")
                .merge(evo_med, on=["NO_UF","NO_MUNICIPIO","CO_MUNICIPIO","ANO"], how="outer"))
    evolucao = evolucao.merge(base[["CO_MUNICIPIO"]], on="CO_MUNICIPIO", how="inner")
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
        ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]:
        evolucao[c+"_PERC"] = (evolucao[c]*100).round(2)

    base = base.sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True)
    evolucao = evolucao.sort_values(["NO_UF","NO_MUNICIPIO","ANO"]).reset_index(drop=True)
    return base, evolucao

# -------------------- APP --------------------
st.set_page_config(page_title="Instituto Alpargatas — Painel", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

with st.spinner("Carregando e processando…"):
    base, evolucao = build_data()

if base.empty:
    st.error("Nenhum município filtrado. Confira os nomes dos arquivos e o conteúdo.")
    st.stop()

# KPIs
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios", f"{base['CO_MUNICIPIO'].nunique()}")
with c2: st.metric("Aprovação média 2023", f"{base['TAXA_APROVACAO_MEDIA_PERC'].mean():.1f}%")
top_ap = base.sort_values("TAXA_APROVACAO_MEDIA_PERC", ascending=False).iloc[0]
with c3: st.metric("Maior aprovação (2023)", f"{top_ap['TAXA_APROVACAO_MEDIA_PERC']:.1f}%", top_ap["NO_MUNICIPIO"])
top_u = base.sort_values("URGENCIA", ascending=False).iloc[0]
with c4: st.metric("Maior urgência", f"{top_u['URGENCIA']:.1f}", top_u["NO_MUNICIPIO"])

st.divider()

# Filtros
ufs = sorted(base["NO_UF"].dropna().unique())
uf_sel = st.multiselect("UF", ufs, default=ufs)
munis = base[base["NO_UF"].isin(uf_sel)]
muni_opts = munis["NO_MUNICIPIO"].tolist()
muni_sel = st.multiselect("Municípios", muni_opts, default=muni_opts)

base_f = base[base["NO_MUNICIPIO"].isin(muni_sel) & base["NO_UF"].isin(uf_sel)]
evo_f  = evolucao[evolucao["NO_MUNICIPIO"].isin(muni_sel) & evolucao["NO_UF"].isin(uf_sel)]

# Tabela
st.subheader("📄 Tabela principal")
cols_show = [
    "NO_UF","NO_MUNICIPIO","CO_MUNICIPIO",
    "TAXA_APROVACAO_INICIAIS_PERC","TAXA_APROVACAO_FINAIS_PERC","TAXA_APROVACAO_MEDIO_PERC",
    "TAXA_APROVACAO_MEDIA_PERC","EVASAO_FUNDAMENTAL","EVASAO_MEDIO","REPROV_INICIAIS","REPROV_FINAIS","URGENCIA"
]
st.dataframe(
    base_f[cols_show].rename(columns={
        "TAXA_APROVACAO_INICIAIS_PERC":"Aprov. Iniciais (%)",
        "TAXA_APROVACAO_FINAIS_PERC":"Aprov. Finais (%)",
        "TAXA_APROVACAO_MEDIO_PERC":"Aprov. Médio (%)",
        "TAXA_APROVACAO_MEDIA_PERC":"Aprov. Média (%)",
        "EVASAO_FUNDAMENTAL":"Evasão Fund.",
        "EVASAO_MEDIO":"Evasão Médio",
        "REPROV_INICIAIS":"Reprov. Iniciais",
        "REPROV_FINAIS":"Reprov. Finais",
    }),
    use_container_width=True, hide_index=True
)

# Rankings
st.subheader("🏅 Rankings")
colA, colB = st.columns(2)

rank_ap = base_f.sort_values("TAXA_APROVACAO_MEDIA_PERC", ascending=False).head(15)
figA = px.bar(rank_ap, x="TAXA_APROVACAO_MEDIA_PERC", y="NO_MUNICIPIO",
              orientation="h", color="NO_UF",
              labels={"TAXA_APROVACAO_MEDIA_PERC":"Aprovação média 2023 (%)",
                      "NO_MUNICIPIO":"Município"})
figA.update_yaxes(categoryorder="total ascending")
colA.plotly_chart(figA, use_container_width=True)

rank_u = base_f.sort_values("URGENCIA", ascending=False).head(15)
figB = px.bar(rank_u, x="URGENCIA", y="NO_MUNICIPIO",
              orientation="h", color="NO_UF",
              labels={"URGENCIA":"Urgência (Evasão + Reprovação)",
                      "NO_MUNICIPIO":"Município"})
figB.update_yaxes(categoryorder="total ascending")
colB.plotly_chart(figB, use_container_width=True)

# Evolução
st.subheader("📈 Evolução temporal (média das etapas)")
if not evo_f.empty:
    evo_plot = (evo_f.groupby(["NO_MUNICIPIO","ANO"], as_index=False)
                    ["APROVACAO_MEDIA_GERAL_PERC"].mean())
    figL = px.line(evo_plot, x="ANO", y="APROVACAO_MEDIA_GERAL_PERC",
                   color="NO_MUNICIPIO", markers=True,
                   labels={"APROVACAO_MEDIA_GERAL_PERC":"Aprovação média (%)"})
    st.plotly_chart(figL, use_container_width=True)
else:
    st.info("Selecione ao menos um município para ver a evolução.")



