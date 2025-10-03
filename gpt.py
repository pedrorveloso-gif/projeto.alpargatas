# gpt.py — Painel Streamlit sem Dados_alpa.xlsx
# Requer: streamlit, pandas, plotly, odfpy, openpyxl

import unicodedata, re
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================
# 0) Arquivos (mesmo diretório/ subpasta do repo)
# ============================
ARQ_INICIAIS = "anos_iniciais.xlsx"
ARQ_FINAIS   = "anos_finais.xlsx"
ARQ_MEDIO    = "ensino_medio.xlsx"
ARQ_EVASAO   = "evasao.ods"     # .ods → odfpy

# ============================
# 1) Cidades alvo (SEM Mixing Center)
# ============================
CIDADES_INTERESSE = [
    "ALAGOA NOVA", "BANANEIRAS", "CABACEIRAS", "CAMPINA GRANDE",
    "CARPINA", "CATURITÉ", "GUARABIRA", "INGÁ", "ITATUBA",
    "JOÃO PESSOA", "LAGOA SECA", "MOGEIRO", "MONTES CLAROS",
    "QUEIMADAS", "SANTA RITA", "SÃO PAULO", "SERRA REDONDA"
]

# ============================
# 2) Utilitários
# ============================
def strip_accents_up(s: object) -> str:
    if pd.isna(s): return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def to7(series: pd.Series) -> pd.Series:
    return series.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

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
    t = df.copy()
    t["CO_MUNICIPIO"] = to7(t["CO_MUNICIPIO"])
    anos = _anos_disponiveis(t, 2005, 2023)
    if not anos:  # segurança
        return pd.DataFrame(columns=["CO_MUNICIPIO","ANO",rotulo])
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    for c in cols: t[c] = pd.to_numeric(t[c], errors="coerce")
    long_ = t[["CO_MUNICIPIO"] + cols].melt("CO_MUNICIPIO", value_name=rotulo)
    long_["ANO"] = long_["variable"].str.extract(r"(\d{4})").astype(int)
    long_.drop(columns="variable", inplace=True)
    return long_.groupby(["CO_MUNICIPIO","ANO"], as_index=False)[rotulo].mean()

def media_2023_por_municipio(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    t = df.copy()
    t["CO_MUNICIPIO"] = to7(t["CO_MUNICIPIO"])
    t[rotulo] = pd.to_numeric(t["VL_INDICADOR_REND_2023"], errors="coerce")
    return t.groupby("CO_MUNICIPIO", as_index=False)[rotulo].mean()

def _num_percent_str(s: pd.Series) -> pd.Series:
    # trata "12,3", "45%" etc → float
    return pd.to_numeric(
        s.astype(str)
         .str.replace("%","",regex=False)
         .str.replace(",",".",regex=False),
        errors="coerce"
    )

# ============================
# 3) Carga + filtro de bases
# ============================
@st.cache_data(show_spinner=True)
def build_data():
    # --- Aprovação (planilhas INEP)
    df_ini = pd.read_excel(ARQ_INICIAIS, header=9)
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9)
    df_med = pd.read_excel(ARQ_MEDIO,    header=9)

    # Normaliza nome para filtro
    for df in (df_ini, df_fin, df_med):
        if "NO_MUNICIPIO" not in df.columns:
            raise KeyError("Planilha INEP sem coluna NO_MUNICIPIO.")
        df["NO_MUNICIPIO_RAW"]  = df["NO_MUNICIPIO"]
        df["NO_MUNICIPIO_NORM"] = df["NO_MUNICIPIO_RAW"].apply(strip_accents_up)

    cidades_norm = [strip_accents_up(x) for x in CIDADES_INTERESSE]

    df_ini = df_ini[df_ini["NO_MUNICIPIO_NORM"].isin(cidades_norm)].copy()
    df_fin = df_fin[df_fin["NO_MUNICIPIO_NORM"].isin(cidades_norm)].copy()
    df_med = df_med[df_med["NO_MUNICIPIO_NORM"].isin(cidades_norm)].copy()

    # --- Evasão (Censo)
    df_eva = pd.read_excel(ARQ_EVASAO, header=8, engine="odf")
    # Renomeia para garantir consistência
    if "NO_MUNICIPIO" not in df_eva.columns:
        raise KeyError("Planilha de evasão sem coluna NO_MUNICIPIO.")
    df_eva["NO_MUNICIPIO_RAW"]  = df_eva["NO_MUNICIPIO"]
    df_eva["NO_MUNICIPIO_NORM"] = df_eva["NO_MUNICIPIO_RAW"].apply(strip_accents_up)
    df_eva = df_eva[df_eva["NO_MUNICIPIO_NORM"].isin(cidades_norm)].copy()

    # Seleciona/renomeia colunas principais de evasão
    mapa_cols = {
        "1_CAT3_CATFUN": "EVASAO_FUNDAMENTAL",
        "1_CAT3_CATMED": "EVASAO_MEDIO",
    }
    cols_pick = ["NO_MUNICIPIO","NO_MUNICIPIO_NORM","CO_MUNICIPIO"] + list(mapa_cols.keys())
    cols_pick = [c for c in cols_pick if c in df_eva.columns]
    df_eva = df_eva[cols_pick].rename(columns=mapa_cols)
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]:
        if c in df_eva.columns:
            df_eva[c] = _num_percent_str(df_eva[c])

    # --- Lista base de municípios (por código IBGE)
    # Preferimos o nome vindo de iniciais (se não houver, caímos para finais/médio)
    bases_nome = pd.concat([
        df_ini[["CO_MUNICIPIO","NO_MUNICIPIO_NORM","NO_MUNICIPIO_RAW"]],
        df_fin[["CO_MUNICIPIO","NO_MUNICIPIO_NORM","NO_MUNICIPIO_RAW"]],
        df_med[["CO_MUNICIPIO","NO_MUNICIPIO_NORM","NO_MUNICIPIO_RAW"]],
    ], ignore_index=True)
    bases_nome["CO_MUNICIPIO"] = to7(bases_nome["CO_MUNICIPIO"])
    nome_ref = (bases_nome.dropna(subset=["CO_MUNICIPIO"])
                          .drop_duplicates("CO_MUNICIPIO")
                          .rename(columns={"NO_MUNICIPIO_RAW":"NO_MUNICIPIO"}))[["CO_MUNICIPIO","NO_MUNICIPIO"]]

    # --- Métricas 2023 (proporção 0–1)
    ini23 = media_2023_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin23 = media_2023_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med23 = media_2023_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (nome_ref
            .merge(ini23, on="CO_MUNICIPIO", how="left")
            .merge(fin23, on="CO_MUNICIPIO", how="left")
            .merge(med23, on="CO_MUNICIPIO", how="left")
            .merge(df_eva[["CO_MUNICIPIO","EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]], on="CO_MUNICIPIO", how="left"))

    # --- Derivados
    base["Reprovacao_Iniciais"] = (1 - pd.to_numeric(base["TAXA_APROVACAO_INICIAIS"], errors="coerce")) * 100
    base["Reprovacao_Finais"]   = (1 - pd.to_numeric(base["TAXA_APROVACAO_FINAIS"],  errors="coerce")) * 100

    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]:
        base[c] = _num_percent_str(base[c])

    base["Urgencia"] = base[["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1, skipna=True)

    # --- Evolução histórica (média por município-ano, 2005–2023)
    evo_ini = _long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_med, "APROVACAO_MEDIO")
    evolucao = (evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                       .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer")
                       .merge(nome_ref, on="CO_MUNICIPIO", how="left"))

    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean(axis=1, skipna=True)

    return base, evolucao

# ============================
# 4) UI
# ============================
st.set_page_config(page_title="Instituto Alpargatas — Painel", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

with st.spinner("Processando dados…"):
    base, evolucao = build_data()

# ---------------- KPIs ----------------
c1,c2,c3,c4 = st.columns(4)
with c1:
    st.metric("Municípios (selecionados)", f"{base['CO_MUNICIPIO'].nunique()}")
with c2:
    st.metric("Aprovação — Finais (média)", f"{(pd.to_numeric(base['TAXA_APROVACAO_FINAIS'], errors='coerce').mean()*100):.1f}%")
with c3:
    st.metric("Evasão — Fundamental (média)", f"{base['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4:
    st.metric("Urgência — média", f"{base['Urgencia'].mean():.1f}")

# ---------------- TABS ----------------
tab1, tab2, tab3 = st.tabs(["🔎 Tabelas","📈 Gráficos","⚙️ Diagnóstico"])

with tab1:
    st.subheader("Urgentes (Top 20 por urgência)")
    urgentes = (base.sort_values("Urgencia", ascending=False)
                     .loc[:, ["NO_MUNICIPIO","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
                              "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"]]
                     .head(20))
    st.dataframe(urgentes, use_container_width=True)

    st.subheader("Base (métricas 2023)")
    mostra = base.copy()
    mostra["APROVACAO_INICIAIS_%"] = (pd.to_numeric(mostra["TAXA_APROVACAO_INICIAIS"], errors="coerce")*100).round(2)
    mostra["APROVACAO_FINAIS_%"]   = (pd.to_numeric(mostra["TAXA_APROVACAO_FINAIS"],   errors="coerce")*100).round(2)
    mostra["APROVACAO_MEDIO_%"]    = (pd.to_numeric(mostra["TAXA_APROVACAO_MEDIO"],    errors="coerce")*100).round(2)
    cols = ["NO_MUNICIPIO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%",
            "EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"]
    st.dataframe(mostra[cols], use_container_width=True)

with tab2:
    st.subheader("Tendência geral — aprovação (média dos municípios selecionados)")
    tmp = evolucao.copy()
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce")*100
    m = tmp.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean()
    fig1 = px.line(
        m.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
        x="ANO", y="Aprovação (%)", color="Etapa", markers=True
    )
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Gap — Iniciais − Finais (p.p.)")
    gap = (tmp.groupby("ANO")[["APROVACAO_INICIAIS","APROVACAO_FINAIS"]].mean())
    gap["GAP_pp"] = gap["APROVACAO_INICIAIS"] - gap["APROVACAO_FINAIS"]
    fig2 = px.line(gap.reset_index(), x="ANO", y="GAP_pp", markers=True)
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Evolução por município (selecione)")
    munis = evolucao["NO_MUNICIPIO"].dropna().drop_duplicates().sort_values().tolist()
    sel = st.selectbox("Município", munis)
    if sel:
        e1 = evolucao[evolucao["NO_MUNICIPIO"]==sel].copy()
        for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]:
            e1[c] = pd.to_numeric(e1[c], errors="coerce")*100
        fig3 = px.line(
            e1.melt(id_vars=["ANO"], value_vars=["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"],
                    var_name="Indicador", value_name="Valor (%)"),
            x="ANO", y="Valor (%)", color="Indicador", markers=True, title=sel
        )
        st.plotly_chart(fig3, use_container_width=True)

with tab3:
    st.write("**Shapes**")
    st.write("base:", base.shape, "| evolucao:", evolucao.shape)
    st.write("**Tipos (base)**")
    st.code(str(base.dtypes))
