# final_alpa.py
# App: Instituto Alpargatas — Painel Municípios (sem Dados_alpa)
# - Mantém a base que rodou
# - Ajustes: tabela usa mediana no lugar de 0; gráfico fallback no modelo “Tendência Geral”
# - KPIs: "Municípios presentes na pesquisa"=18; anos 2005/2023

import os, re, unicodedata
from pathlib import Path
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Caminhos ----------------
ARQ_INICIAIS  = "dados/anos_iniciais.xlsx"
ARQ_FINAIS    = "dados/anos_finais.xlsx"
ARQ_MEDIO     = "dados/ensino_medio.xlsx"
ARQ_URGENTES  = "dados/urgentes.csv"   # injeção manual de evasão/urgência

# ---------------- Cidades alvo (sem Mixing Center) ----------------
CIDADES_ALP = [
    "ALAGOA NOVA","BANANEIRAS","CABACEIRAS","CAMPINA GRANDE",
    "CARPINA","CATURITÉ","GUARABIRA","INGÁ","ITATUBA","JOÃO PESSOA",
    "LAGOA SECA","MOGEIRO","MONTES CLAROS","QUEIMADAS","SANTA RITA",
    "SÃO PAULO","SERRA REDONDA","BAÍA DA TRAIÇÃO"
]
CIDADES_PESQUISA_FIXO = 18  # pedido

# ---------------- Utils ----------------
def nrm(x):
    if pd.isna(x): return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII","ignore").decode("ASCII")
    s = s.replace("–","-").replace("—","-")
    return " ".join(s.upper().split())

CIDADES_NORM = {nrm(c) for c in CIDADES_ALP}

def achar_header(path, max_rows=80):
    tmp = pd.read_excel(path, header=None, nrows=max_rows)
    for i, row in tmp.iterrows():
        vals = [nrm(v) for v in row.tolist()]
        if any("UF" in v for v in vals) and \
           any("CODIGO" in v and "MUNICIPIO" in v for v in vals) and \
           any("NOME" in v and "MUNICIPIO" in v for v in vals):
            return i
    return 0

def colmap_padrao(df):
    alvo = {
        "NO_UF": {"SIGLA DA UF","UF","SIGLA_UF","NO_UF"},
        "CO_MUNICIPIO": {"CODIGO DO MUNICIPIO","CODIGO DO MUNICÍPIO","CO_MUNICIPIO",
                         "CODIGO MUNICIPIO","CÓDIGO DO MUNICIPIO"},
        "NO_MUNICIPIO": {"NOME DO MUNICIPIO","NOME DO MUNICÍPIO","NO_MUNICIPIO",
                         "MUNICIPIO","MUNICÍPIO"},
    }
    norm_cols = {c: nrm(c) for c in df.columns}
    inv = {}
    for canon, candidatos in alvo.items():
        hit = None
        for orig, normed in norm_cols.items():
            if any(normed == nrm(cand) for cand in candidatos):
                hit = orig; break
        if not hit:
            raise KeyError(f"não encontrei coluna para {canon}. Cabeçalhos: {list(df.columns)}")
        inv[hit] = canon
    return inv

def to_num(s):
    # robusto contra "None", "-", "%" e vírgula decimal
    ser = pd.Series(s).astype(str).str.strip()
    ser = ser.replace({"": np.nan, "None": np.nan, "NONE": np.nan, "-": np.nan})
    ser = (ser.str.replace("%","", regex=False)
              .str.replace("\u2212","-", regex=False)
              .str.replace(",", ".", regex=False))
    return pd.to_numeric(ser, errors="coerce")

def mapear_colunas_indicadores(df):
    mapping = {}
    for col in df.columns:
        s = nrm(col)
        m = re.search(r"(\d{4})", s)
        if not m:
            continue
        ano = int(m.group(1))
        if ano < 2000 or ano > 2100:
            continue
        if ("APROV" in s) or ("INDICADOR" in s and "REND" in s) or s.startswith("VL_INDICADOR_REND_"):
            mapping[ano] = col
    return mapping

def ler_planilha_inep(path):
    hdr = achar_header(path)
    df  = pd.read_excel(path, header=hdr)
    m   = colmap_padrao(df)
    df  = df.rename(columns=m)
    df["CO_MUNICIPIO"] = (
        df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    df["NO_MUNICIPIO"] = df["NO_MUNICIPIO"].astype(str).str.strip()
    df["NO_UF"]        = df["NO_UF"].astype(str).str.strip()
    return df

def encontrar_col_indicador_mais_recente(df):
    mapping = mapear_colunas_indicadores(df)
    if not mapping:
        raise KeyError("Nenhuma coluna de aprovação/rendimento por ano foi reconhecida.")
    ano = max(mapping.keys())
    return mapping[ano], ano, mapping

def media_por_municipio(df, rotulo):
    col, ano, _ = encontrar_col_indicador_mais_recente(df)
    vals = to_num(df[col])
    out = (pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo: vals})
             .groupby("CO_MUNICIPIO", as_index=False)[rotulo]
             .mean())
    return out, ano

def evolucao_long(df):
    """wide -> long (CO_MUNICIPIO, ANO, VALOR) com mapeamento robusto."""
    _, _, mapping = encontrar_col_indicador_mais_recente(df)
    if not mapping:
        return pd.DataFrame(columns=["CO_MUNICIPIO","ANO","VALOR"])
    tmp = df[["CO_MUNICIPIO"] + list(mapping.values())].copy()
    ren = {orig: f"VALOR_{ano}" for ano, orig in mapping.items()}
    tmp = tmp.rename(columns=ren)
    valor_cols = [c for c in tmp.columns if c.startswith("VALOR_")]
    for c in valor_cols:
        tmp[c] = to_num(tmp[c])
    long = tmp.melt(id_vars="CO_MUNICIPIO", value_vars=valor_cols,
                    var_name="COL", value_name="VALOR")
    long["ANO"] = long["COL"].str.extract(r"(\d{4})").astype(int)
    long = long.drop(columns=["COL"])
    # média por ano para evitar duplicidade (legibilidade do gráfico)
    long = (long.groupby(["CO_MUNICIPIO","ANO"], as_index=False)["VALOR"]
                 .mean())
    return long

# ---------- URGENTES.CSV ----------
def ler_urgentes(path_csv: str) -> pd.DataFrame:
    if not os.path.exists(path_csv):
        return pd.DataFrame()
    try:
        u = pd.read_csv(path_csv)
        if u.shape[1] == 1:
            u = pd.read_csv(path_csv, sep=";")
    except Exception:
        u = pd.read_csv(path_csv, sep=";")

    # renomes comuns -> padroniza nomes
    ren = {}
    for c in list(u.columns):
        cn = nrm(c)
        if cn == "UF_SIGLA": ren[c] = "NO_UF"
        if cn in {"EVASAO-FUNDAMENTAL","EVASAO - FUNDAMENTAL","EVASAO FUNDAMENTAL"}:
            ren[c] = "Evasao_Fundamental"
        if cn in {"EVASAO-MEDIO","EVASAO - MEDIO","EVASAO MEDIO"}:
            ren[c] = "Evasao_Medio"
        if cn in {"MEDIA_HISTORICA_%","MEDIA HISTORICA %","MEDIA-HISTORICA","MEDIA_HISTORICA"}:
            ren[c] = "MEDIA_HISTORICA_%"
    if ren:
        u = u.rename(columns=ren)

    for col in [c for c in ["NO_UF","MUNICIPIO_NOME_ALP","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA"] if c in u]:
        u[col] = u[col].astype(str).str.strip()

    base_nome = u["MUNICIPIO_NOME_ALP"] if "MUNICIPIO_NOME_ALP" in u else u.get("NO_MUNICIPIO")
    if base_nome is None:
        return pd.DataFrame()
    u["NORM_MUN"] = base_nome.apply(nrm)

    # preferir Total/Total quando existir
    if "NO_LOCALIZACAO" in u and "NO_DEPENDENCIA" in u:
        loc = u["NO_LOCALIZACAO"].fillna("").str.upper()
        dep = u["NO_DEPENDENCIA"].fillna("").str.upper()
        prio = (loc.eq("TOTAL").astype(int) + dep.eq("TOTAL").astype(int))
        u = u.assign(_prio=prio).sort_values("_prio", ascending=False)
        u = u.drop_duplicates(subset=["NORM_MUN","NO_UF"], keep="first").drop(columns=["_prio"])
    else:
        u = u.drop_duplicates(subset=["NORM_MUN","NO_UF"], keep="first")

    # numéricos
    num_cols = [
        "Evasao_Fundamental","Evasao_Medio",
        "TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS",
        "Reprovacao_Iniciais","Reprovacao_Finais",
        "Urgencia","MEDIA_HISTORICA_%"
    ]
    for c in [c for c in num_cols if c in u]:
        u[c] = to_num(u[c])

    return u

# ---------------- App ----------------
st.set_page_config(page_title="Instituto Alpargatas — Painel (hotfix)", layout="wide")

st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")
with st.expander("📁 Arquivos esperados em `dados/`", expanded=False):
    for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_URGENTES]:
        st.write(("✅" if os.path.exists(p) else "❌"), p)
    if os.path.isdir("dados"):
        st.code("\n".join(sorted(os.listdir("dados"))), language="text")

@st.cache_data(show_spinner=True)
def build_data():
    # --- INEP
    df_ini = ler_planilha_inep(ARQ_INICIAIS)
    df_fin = ler_planilha_inep(ARQ_FINAIS)
    df_med = ler_planilha_inep(ARQ_MEDIO)

    # Normalização + filtro Alpargatas
    for df in (df_ini, df_fin, df_med):
        df["NORM_MUN"] = df["NO_MUNICIPIO"].apply(nrm)
    base = (df_ini[["NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NORM_MUN"]]
            .drop_duplicates())
    base = base[base["NORM_MUN"].isin(CIDADES_NORM)].copy()

    # Médias mais recentes
    ini, ano_ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin, ano_fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med, ano_med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))

    # Fração -> % (0–100)
    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
        if c in base.columns:
            base[c + "_%"] = (base[c]*100).round(2)

    # Evolução (já agregada por ano)
    long_ini = evolucao_long(df_ini).rename(columns={"VALOR":"APROVACAO_INICIAIS"})
    long_fin = evolucao_long(df_fin).rename(columns={"VALOR":"APROVACAO_FINAIS"})
    long_med = evolucao_long(df_med).rename(columns={"VALOR":"APROVACAO_MEDIO"})

    evol = (long_ini.merge(long_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                   .merge(long_med, on=["CO_MUNICIPIO","ANO"], how="outer"))
    evol = evol.merge(base[["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","NORM_MUN"]].drop_duplicates(),
                      on="CO_MUNICIPIO", how="left")
    evol = evol[evol["NORM_MUN"].isin(CIDADES_NORM)].copy()
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        if c in evol.columns:
            evol[c + "_%"] = (evol[c]*100).round(2)

    # --- URGENTES.CSV (injeção manual)
    urg = ler_urgentes(ARQ_URGENTES)
    if not urg.empty:
        urg["NO_UF"] = urg["NO_UF"].fillna("")
        urg["NORM_MUN"] = urg["NORM_MUN"].fillna("")
        urgentes_set = set(urg["NORM_MUN"].tolist())
        base = base[base["NORM_MUN"].isin(urgentes_set)].copy()
        evol = evol[evol["NORM_MUN"].isin(urgentes_set)].copy()

        cols_injetar = [c for c in [
            "NO_UF","NORM_MUN","Evasao_Fundamental","Evasao_Medio",
            "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia",
            "MEDIA_HISTORICA_%","NO_LOCALIZACAO","NO_DEPENDENCIA",
            "MUNICIPIO_NOME_ALP","NO_MUNICIPIO"
        ] if c in urg.columns]
        inj = urg[cols_injetar].drop_duplicates(["NO_UF","NORM_MUN"])

        base = base.merge(inj, on=["NO_UF","NORM_MUN"], how="left", suffixes=("", "_inj"))
        if "NO_MUNICIPIO_inj" in base.columns:
            base["NO_MUNICIPIO"] = base["NO_MUNICIPIO"].fillna(base["NO_MUNICIPIO_inj"])
            base.drop(columns=["NO_MUNICIPIO_inj"], inplace=True)

    # KPIs extras
    base["APROVACAO_MEDIA_GERAL_%"] = base[
        [c for c in ["TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%"] if c in base]
    ].mean(axis=1, skipna=True)

    meta = {"ANO_INI": ano_ini, "ANO_FIN": ano_fin, "ANO_MED": ano_med}
    return base, evol, meta

with st.spinner("Carregando e processando…"):
    base, evol, meta = build_data()

# ====== Aba: Visão geral / Gráficos / Tabelas / Diagnóstico ======
tab_intro, tab_graficos, tab_tabelas, tab_diag = st.tabs(
    ["Visão geral", "Gráficos", "Tabelas", "Diagnóstico"]
)

with tab_intro:
    st.header("📌 Introdução")
    st.write(
        "Este painel consolida dados do INEP/IDEB e do Instituto Alpargatas para mapear **municípios com maior urgência educacional**. "
        "A métrica de *Grau de Urgência* combina **evasão** e **reprovação**, apoiando a priorização de ações."
    )
    st.header("🧭 Metodologia de Análise")
    st.write(
        "1) Padronizamos planilhas do INEP; 2) Calculamos aprovações por etapa (iniciais, finais, médio); "
        "3) Injetamos a tabela **urgentes.csv** com evasão e urgência; 4) Construímos séries históricas por município."
    )

with tab_graficos:
    # KPIs (fixos conforme pedido)
    c1,c2,c3,c4 = st.columns(4)
    with c1: st.metric("Municípios presentes na pesquisa", f"{CIDADES_PESQUISA_FIXO}")
    with c2: st.metric("Ano (Iniciais)", 2005)
    with c3: st.metric("Ano (Finais)",   2023)
    with c4: st.metric("Ano (Médio)",    2023)

    st.subheader("🧭 Evolução por município (aprov. %)")
    mun = st.selectbox("Escolha um município", sorted(base["NO_MUNICIPIO"].dropna().unique()))
    suavizar = st.checkbox("Suavizar (média móvel 3 anos)", value=False)

    e = evol[evol["NO_MUNICIPIO"] == mun].copy()
    cols_etapas = [c for c in ["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%"] if c in e]

    # monta long e agrega por ano
    e_long = (e.melt(id_vars=["ANO"], value_vars=cols_etapas, var_name="Etapa", value_name="Aprovacao")
                .dropna(subset=["Aprovacao"]))
    if not e_long.empty:
        e_long["Etapa"] = (e_long["Etapa"].str.replace("_%","", regex=False)
                                         .str.replace("APROVACAO_","", regex=False)
                                         .str.title())
        e_long = (e_long.groupby(["Etapa","ANO"], as_index=False)["Aprovacao"].mean())
        e_long = e_long.sort_values(["Etapa","ANO"])
        if suavizar:
            e_long["Aprovacao"] = (e_long
                                   .groupby("Etapa")["Aprovacao"]
                                   .transform(lambda s: s.rolling(3, min_periods=1).mean()))
        fig2 = px.line(
            e_long, x="ANO", y="Aprovacao", color="Etapa", markers=True,
            labels={"Aprovacao":"Aprovação (%)", "ANO":"Ano"}
        )
        fig2.update_yaxes(range=[0,100], tickformat=".0f")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Sem série legível para este município. Exibindo a tendência geral do recorte.")

    # Sempre mostrar o gráfico no modelo solicitado (média do recorte)
    st.markdown("### 📊 Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)")
    geral = evol.copy()
    serie = (geral
             .groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
             .mean(numeric_only=True))
    serie = serie.sort_values("ANO")
    fig_g = px.line(
        serie.melt(id_vars="ANO", value_vars=["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"],
                   var_name="Etapa", value_name="Aprovação (%)"),
        x="ANO", y="Aprovação (%)", color="Etapa", markers=True
    )
    fig_g.update_yaxes(range=[0,100], tickformat=".0f")
    st.plotly_chart(fig_g, use_container_width=True)

with tab_tabelas:
    st.subheader("📋 Tabela (com urgência & evasão)")

    show_cols = [
        "NO_UF","NO_MUNICIPIO",
        "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%",
        "Evasao_Fundamental","Reprovacao_Iniciais","Reprovacao_Finais","Urgencia",
        "APROVACAO_MEDIA_GERAL_%","MEDIA_HISTORICA_%"
    ]
    show_cols = [c for c in show_cols if c in base.columns]
    tbl = base[show_cols].copy()

    # --------- preenche NaN com MEDIANA por coluna (pedido) ----------
    for c in tbl.columns:
        if pd.api.types.is_numeric_dtype(tbl[c]):
            med = pd.to_numeric(tbl[c], errors="coerce").median(skipna=True)
            tbl[c] = pd.to_numeric(tbl[c], errors="coerce").fillna(med).round(2)
        else:
            tbl[c] = tbl[c].astype(str).replace({"nan":"—","None":"—"}).replace("", "—")

    st.dataframe(
        tbl.sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True),
        use_container_width=True
    )

with tab_diag:
    st.markdown("### 🔎 Debug: colunas de indicadores reconhecidas")
    for nome, caminho in [("Iniciais", ARQ_INICIAIS), ("Finais", ARQ_FINAIS), ("Médio", ARQ_MEDIO)]:
        try:
            df = pd.read_excel(caminho, header=achar_header(caminho))
            mapping = mapear_colunas_indicadores(df)
            st.write(f"**{nome}** → Anos detectados:", sorted(mapping.keys()))
            st.code("\n".join([f"{a}: {c}" for a,c in sorted(mapping.items())]), language="text")
        except Exception as e:
            st.warning(f"{nome}: {e}")
