# final_alpa.py — Painel Municípios (hotfix com abas e urgentes.csv)
import os, re, unicodedata
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Caminhos ----------------
ARQ_INICIAIS  = "dados/anos_iniciais.xlsx"
ARQ_FINAIS    = "dados/anos_finais.xlsx"
ARQ_MEDIO     = "dados/ensino_medio.xlsx"
ARQ_URGENTES  = "dados/urgentes.csv"        # <- INJEÇÃO MANUAL

# ---------------- Cidades alvo (sem Mixing Center) ----------------
CIDADES_ALP = [
    "ALAGOA NOVA","BANANEIRAS","CABACEIRAS","CAMPINA GRANDE",
    "CARPINA","CATURITÉ","GUARABIRA","INGÁ","ITATUBA","JOÃO PESSOA",
    "LAGOA SECA","MOGEIRO","MONTES CLAROS","QUEIMADAS","SANTA RITA",
    "SÃO PAULO","SERRA REDONDA"
]

# ---------------- Utils ----------------
def nrm(x):
    if pd.isna(x): return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII","ignore").decode("ASCII")
    s = s.replace("–","-").replace("—","-")
    return " ".join(s.upper().split())

CIDADES_NORM = {nrm(c) for c in CIDADES_ALP}

def achar_header(path, max_rows=80):
    """Acha a linha de cabeçalho (onde aparecem UF + CODIGO + NOME)."""
    tmp = pd.read_excel(path, header=None, nrows=max_rows)
    for i, row in tmp.iterrows():
        vals = [nrm(v) for v in row.tolist()]
        if any("UF" in v for v in vals) and \
           any("CODIGO" in v and "MUNICIPIO" in v for v in vals) and \
           any("NOME" in v and "MUNICIPIO" in v for v in vals):
            return i
    return 0

def colmap_padrao(df):
    """Mapeia para NO_UF, CO_MUNICIPIO, NO_MUNICIPIO (independente de acento)."""
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
    return pd.to_numeric(
        pd.Series(s).astype(str)
        .str.replace("%","", regex=False)
        .str.replace("\u2212","-", regex=False)   # menos unicode
        .str.replace(",", ".", regex=False),
        errors="coerce"
    )

def mapear_colunas_indicadores(df):
    mapping = {}
    for col in df.columns:
        s = nrm(col)
        m = re.search(r"(\d{4})", s)
        if not m: 
            continue
        ano = int(m.group(1))
        if 2000 <= ano <= 2100 and (("APROV" in s) or ("INDICADOR" in s and "REND" in s) or s.startswith("VL_INDICADOR_REND_")):
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
    """wide -> long (CO_MUNICIPIO, ANO, VALOR) usando mapeamento robusto (VALOR em 0..1)."""
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
    return long

# --------- URGENTES.CSV (leitura robusta) ----------
def ler_urgentes(path_csv: str) -> pd.DataFrame:
    if not os.path.exists(path_csv):
        return pd.DataFrame()
    try:
        u = pd.read_csv(path_csv)
        if u.shape[1] == 1:
            u = pd.read_csv(path_csv, sep=";")
    except Exception:
        u = pd.read_csv(path_csv, sep=";")

    ren = {}
    for c in list(u.columns):
        cn = nrm(c)
        if cn == "UF_SIGLA": ren[c] = "NO_UF"
        if cn in {"EVASAO-FUNDAMENTAL","EVASAO - FUNDAMENTAL","EVASAO FUNDAMENTAL"}:
            ren[c] = "Evasao_Fundamental"
        if cn in {"EVASAO-MEDIO","EVASAO - MEDIO","EVASAO MEDIO"}:
            ren[c] = "Evasao_Medio"
        if cn in {"MEDIA_HISTORICA","MEDIA HISTORICA","MEDIA-HISTORICA","MEDIA HISTORICA %",
                  "MEDIA_HISTORICA_%","MEDIA HISTORICA (%)","MEDIA_HISTORICA(%)"}:
            ren[c] = "MEDIA_HISTORICA_%"
        if cn == "MUNICIPIO_CHAVE": ren[c] = "MUNICIPIO_CHAVE"
        if cn in {"MUNICIPIO_NOME_ALP","MUNICIPIO_NOME","MUNICIPIO_NOME_ALPA"}:
            ren[c] = "MUNICIPIO_NOME_ALP"
        if cn in {"NO_MUNICIPIO","MUNICIPIO"}:
            ren[c] = "NO_MUNICIPIO"
    if ren:
        u = u.rename(columns=ren)

    for col in [c for c in ["NO_UF","MUNICIPIO_NOME_ALP","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA"] if c in u]:
        u[col] = u[col].astype(str).str.strip()
    base_nome = u["MUNICIPIO_NOME_ALP"] if "MUNICIPIO_NOME_ALP" in u else u.get("NO_MUNICIPIO")
    if base_nome is None:
        return pd.DataFrame()
    u["NORM_MUN"] = base_nome.apply(nrm)

    if "NO_LOCALIZACAO" in u and "NO_DEPENDENCIA" in u:
        loc = u["NO_LOCALIZACAO"].fillna("").str.upper()
        dep = u["NO_DEPENDENCIA"].fillna("").str.upper()
        prio = (loc.eq("TOTAL").astype(int) + dep.eq("TOTAL").astype(int))
        u = u.assign(_prio=prio).sort_values("_prio", ascending=False)
        u = u.drop_duplicates(subset=["NORM_MUN","NO_UF"], keep="first").drop(columns=["_prio"])
    else:
        u = u.drop_duplicates(subset=["NORM_MUN","NO_UF"], keep="first")

    for c in ["Evasao_Fundamental","Evasao_Medio","TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS",
              "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia","MEDIA_HISTORICA_%"]:
        if c in u.columns:
            u[c] = to_num(u[c])

    return u

# --------- Normalizadores de % ----------
def _normalize_pct(series: pd.Series) -> pd.Series:
    """Garante escala 0..100. Aceita 0..1, 0..100, ou 0..10000 (ex.: 9940 -> 99.40)."""
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().max() <= 1.5:
        s = s * 100.0
    if s.dropna().max() > 1000:
        s = s / 100.0
    return s.round(2)

def recompute_percentages(df: pd.DataFrame) -> pd.DataFrame:
    for lab in ["INICIAIS","FINAIS","MEDIO"]:
        src = None
        for cand in [f"TAXA_APROVACAO_{lab}", f"TAXA_APROVACAO_{lab}_urg", f"TAXA_APROVACAO_{lab}_%"]:
            if cand in df.columns:
                src = df[cand]; break
        if src is not None:
            df[f"TAXA_APROVACAO_{lab}_%"] = _normalize_pct(src)
    return df

# ---------------- Build data ----------------
@st.cache_data(show_spinner=True)
def build_data():
    df_ini = ler_planilha_inep(ARQ_INICIAIS)
    df_fin = ler_planilha_inep(ARQ_FINAIS)
    df_med = ler_planilha_inep(ARQ_MEDIO)

    for df in (df_ini, df_fin, df_med):
        df["NORM_MUN"] = df["NO_MUNICIPIO"].apply(nrm)

    base = (df_ini[["NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NORM_MUN"]]
            .drop_duplicates())

    ini, ano_ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin, ano_fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med, ano_med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))

    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
        if c in base.columns:
            base[c + "_%"] = _normalize_pct(base[c])

    long_ini = evolucao_long(df_ini).rename(columns={"VALOR":"APROVACAO_INICIAIS"})
    long_fin = evolucao_long(df_fin).rename(columns={"VALOR":"APROVACAO_FINAIS"})
    long_med = evolucao_long(df_med).rename(columns={"VALOR":"APROVACAO_MEDIO"})

    evol = (long_ini.merge(long_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                  .merge(long_med, on=["CO_MUNICIPIO","ANO"], how="outer"))
    evol = evol.merge(base[["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","NORM_MUN"]].drop_duplicates(),
                      on="CO_MUNICIPIO", how="left")
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        if c in evol.columns:
            evol[c + "_%"] = _normalize_pct(evol[c])

    urg = ler_urgentes(ARQ_URGENTES)
    if not urg.empty:
        urgentes_set = set(urg["NORM_MUN"])
        base = base[base["NORM_MUN"].isin(urgentes_set)].copy()
        evol = evol[evol["NORM_MUN"].isin(urgentes_set)].copy()

        cols_inj = [c for c in [
            "NO_UF","NORM_MUN","NO_MUNICIPIO","Evasao_Fundamental","Evasao_Medio",
            "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia","MEDIA_HISTORICA_%",
            "TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS"
        ] if c in urg.columns]
        inj = urg[cols_inj].drop_duplicates(["NO_UF","NORM_MUN"])

        base = base.merge(inj, on=["NO_UF","NORM_MUN"], how="left", suffixes=("", "_urg"))
        base = recompute_percentages(base)

        if "Reprovacao_Iniciais" not in base or base["Reprovacao_Iniciais"].isna().all():
            base["Reprovacao_Iniciais"] = (100 - base["TAXA_APROVACAO_INICIAIS_%"]).clip(lower=0)
        if "Reprovacao_Finais" not in base or base["Reprovacao_Finais"].isna().all():
            base["Reprovacao_Finais"] = (100 - base["TAXA_APROVACAO_FINAIS_%"]).clip(lower=0)

        for c in ["Evasao_Fundamental","Evasao_Medio"]:
            if c in base.columns:
                base[c] = to_num(base[c]).fillna(0)

        if "Urgencia" not in base.columns or base["Urgencia"].isna().all():
            base["Urgencia"] = (
                base[["Evasao_Fundamental","Reprovacao_Iniciais","Reprovacao_Finais"]]
                    .sum(axis=1, skipna=True)
            )

    base["APROVACAO_MEDIA_GERAL_%"] = base[
        [c for c in ["TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%"] if c in base]
    ].mean(axis=1, skipna=True).round(2)

    meta = {"ANO_INI": int(ano_ini), "ANO_FIN": int(ano_fin), "ANO_MED": int(ano_med),
            "tem_urgentes": int(not urg.empty)}
    return base, evol, meta

# ------------------------------- UI --------------------------------
st.set_page_config(page_title="Instituto Alpargatas — Painel (hotfix)", layout="wide")
st.title("📊 Instituto Alpargatas — Painel (hotfix)")

tabs = st.tabs(["Visão geral", "Gráficos", "Tabelas", "Diagnóstico"])

with tabs[0]:
    st.header("📌 Introdução")
    st.write(
        "Este site apresenta os resultados da análise de dados cujo objetivo foi **mapear os municípios com maior urgência educacional** "
        "e avaliar como os projetos do **Instituto Alpargatas (2020–2024)** estão respondendo a esses desafios. "
        "A análise foi baseada em dados do Instituto Alpargatas, do **INEP (Censo Escolar)** e do **IDEB**, "
        "resultando em uma **métrica de urgência** para a priorização de ações."
    )
    st.header("🧭 Metodologia de Análise")
    st.write(
        "Para alcançar o objetivo, a análise seguiu uma metodologia focada na criação de um **ranking de municípios críticos**. "
        "A abordagem principal foi o desenvolvimento de uma métrica de **“Grau de Urgência” educacional**, que permitiu "
        "classificar as cidades e direcionar os esforços de forma estratégica. "
        "A análise consolidou **dados de desempenho escolar, taxas de evasão e aprovação** para gerar um índice que reflete "
        "a necessidade de intervenção em cada localidade."
    )

with tabs[3]:
    with st.expander("📁 Arquivos esperados em `dados/`", expanded=True):
        for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_URGENTES]:
            st.write(("✅" if os.path.exists(p) else "❌"), p)
        try:
            st.code("\n".join(os.listdir("dados")), language="text")
        except Exception:
            pass

with st.spinner("Carregando e processando…"):
    base, evol, meta = build_data()

# filtros (sidebar valem para todas as abas abaixo)
ufs_opts = sorted(base["NO_UF"].dropna().unique().tolist())
with st.sidebar:
    st.subheader("Filtros")
    sel_ufs = st.multiselect("UF", options=ufs_opts, default=ufs_opts)
    base_uf = base[base["NO_UF"].isin(sel_ufs)] if sel_ufs else base.copy()
    munis_opts = sorted(base_uf["NO_MUNICIPIO"].dropna().unique().tolist())
    sel_munis = st.multiselect("Municípios", options=munis_opts, default=munis_opts)

# aplica filtros
base_f = base.copy()
if 'sel_ufs' in locals() and sel_ufs:
    base_f = base_f[base_f["NO_UF"].isin(sel_ufs)]
if 'sel_munis' in locals() and sel_munis:
    base_f = base_f[base_f["NO_MUNICIPIO"].isin(sel_munis)]

evol_f = evol.copy()
if 'sel_ufs' in locals() and sel_ufs:
    evol_f = evol_f[evol_f["NO_UF"].isin(sel_ufs)]
if 'sel_munis' in locals() and sel_munis:
    evol_f = evol_f[evol_f["NO_MUNICIPIO"].isin(sel_munis)]

# ---------------- GRÁFICOS ----------------
with tabs[1]:
    st.info("Exibindo **apenas os municípios urgentes** (dados injetados de `dados/urgentes.csv`).", icon="⚠️")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Municípios no recorte", f"{base_f['NO_MUNICIPIO'].nunique()}")
    with c2:
        v = base_f["TAXA_APROVACAO_FINAIS_%"].mean()
        st.metric("Aprovação — Finais (média)", f"{(0 if pd.isna(v) else v):.1f}%")
    with c3:
        v = base_f.get("Evasao_Fundamental", pd.Series(dtype=float)).mean()
        st.metric("Evasão — Fundamental (média)", f"{(0 if pd.isna(v) else v):.1f}%")
    with c4:
        v = (base_f.get("Urgencia", pd.Series(dtype=float)).mean())/100.0
        st.metric("Score de risco (média)", f"{(0 if pd.isna(v) else v):.2f}")

    st.subheader("Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)")
    serie = (evol_f.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
                   .mean(numeric_only=True))
    serie = serie.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
    serie["Etapa"] = (serie["Etapa"].str.replace("_%","", regex=False)
                                   .str.replace("APROVACAO_","", regex=False)
                                   .str.title())
    fig_geral = px.line(serie, x="ANO", y="Aprovação (%)", color="Etapa", markers=True)
    fig_geral.update_layout(yaxis_title="Aprovação (%)", xaxis_title="Ano", yaxis_range=[0,100])
    st.plotly_chart(fig_geral, use_container_width=True)

    st.subheader("Evolução por município (aprov. %)")
    mun = st.selectbox("Escolha um município", sorted(base_f["NO_MUNICIPIO"].unique()))
    e = evol_f[evol_f["NO_MUNICIPIO"] == mun].sort_values("ANO")
    if e.empty:
        st.info("Sem série histórica disponível para este município.")
    else:
        e2 = e.melt(
            id_vars=["ANO"],
            value_vars=[c for c in ["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%"] if c in e],
            var_name="Etapa", value_name="Aprovação (%)"
        )
        e2["Etapa"] = (e2["Etapa"].str.replace("_%","", regex=False)
                                   .str.replace("APROVACAO_","", regex=False)
                                   .str.title())
        fig2 = px.line(e2, x="ANO", y="Aprovação (%)", color="Etapa", markers=True)
        fig2.update_layout(yaxis_title="Aprovação (%)", xaxis_title="Ano", yaxis_range=[0,100])
        st.plotly_chart(fig2, use_container_width=True)

# ---------------- TABELAS ----------------
with tabs[2]:
    st.subheader("Tabela (com urgência & evasão)")

    def _normalize_pct(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        if s.dropna().max() <= 1.5: s = s * 100
        if s.dropna().max() > 1000: s = s / 100
        return s.round(2)

    def fmt(df: pd.DataFrame, cols_pct: list[str]) -> pd.DataFrame:
        out = df.copy()
        for c in cols_pct:
            if c in out: out[c] = _normalize_pct(out[c])
        for c in ["Evasao_Fundamental","Evasao_Medio","Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"]:
            if c in out: out[c] = pd.to_numeric(out[c], errors="coerce").round(2)
        return out

    show_cols = [
        "NO_UF","NO_MUNICIPIO",
        "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%",
        "Evasao_Fundamental","Reprovacao_Iniciais","Reprovacao_Finais","Urgencia",
        "APROVACAO_MEDIA_GERAL_%","MEDIA_HISTORICA_%"
    ]
    show_cols = [c for c in show_cols if c in base_f.columns]
    tbl = fmt(base_f[show_cols], [
        "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%",
        "APROVACAO_MEDIA_GERAL_%","MEDIA_HISTORICA_%"
    ]).sort_values(["NO_UF","NO_MUNICIPIO"])
    st.dataframe(tbl.reset_index(drop=True), use_container_width=True)

# ---------------- DIAGNÓSTICO ----------------
with tabs[3]:
    st.subheader("Info do processamento")
    st.write(f"Municípios no painel: **{base['NO_MUNICIPIO'].nunique()}**")
    st.write(f"Ano (Iniciais): **{meta['ANO_INI']}** — Ano (Finais): **{meta['ANO_FIN']}** — Ano (Médio): **{meta['ANO_MED']}**")
    st.caption("Escalas padronizadas para 0–100. Evasão nula exibida como 0. Score de risco = urgência média/100.")
