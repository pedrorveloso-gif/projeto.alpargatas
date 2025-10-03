# final_alpa.py
import re, unicodedata, os
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Caminhos (sempre dentro de dados/) ----------------
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_MEDIO    = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"        # .ods -> odfpy já está no requirements

# ---------------- Lista de cidades (sem Mixing Center) ---------------
CIDADES_ALP = [
    "ALAGOA NOVA","BANANEIRAS","CABACEIRAS","CAMPINA GRANDE",
    "CARPINA","CATURITÉ","GUARABIRA","INGÁ","ITATUBA","JOÃO PESSOA",
    "LAGOA SECA","MOGEIRO","MONTES CLAROS","QUEIMADAS","SANTA RITA",
    "SÃO PAULO","SERRA REDONDA"
]

# ---------------- Utilitários ---------------------------------------
def nrm(x):
    if pd.isna(x): return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII","ignore").decode("ASCII")
    return " ".join(s.upper().replace("–","-").replace("—","-").split())

CIDADES_NORM = {nrm(c) for c in CIDADES_ALP}

def achar_header(path, max_rows=60):
    """Procura linha onde aparecem UF + CODIGO + NOME (normalizados)."""
    tmp = pd.read_excel(path, header=None, nrows=max_rows)
    for i, row in tmp.iterrows():
        vals = [nrm(v) for v in row.tolist()]
        if any("UF" in v for v in vals) and \
           any("CODIGO" in v and "MUNICIPIO" in v for v in vals) and \
           any("NOME" in v and "MUNICIPIO" in v for v in vals):
            return i
    return 0  # fallback (linha 0)

def colmap_padrao(df):
    """Mapeia cabeçalhos reais -> nomes canônicos: NO_UF, CO_MUNICIPIO, NO_MUNICIPIO."""
    alvo = {
        "NO_UF": {"SIGLA DA UF","UF","SIGLA_UF","NO_UF"},
        "CO_MUNICIPIO": {"CODIGO DO MUNICIPIO","CODIGO DO MUNICÍPIO",
                         "CODIGO MUNICIPIO","CÓDIGO DO MUNICIPIO","CO_MUNICIPIO"},
        "NO_MUNICIPIO": {"NOME DO MUNICIPIO","NOME DO MUNICÍPIO","MUNICIPIO","MUNICÍPIO","NO_MUNICIPIO"},
    }
    norm_cols = {c: nrm(c) for c in df.columns}
    inv = {}
    for canon, candidatos in alvo.items():
        hit = None
        for orig, normed in norm_cols.items():
            if any(normed == nrm(cand) for cand in candidatos):
                hit = orig
                break
        if not hit:
            raise KeyError(f"não encontrei coluna para {canon}. Cabeçalhos encontrados: {list(df.columns)}")
        inv[hit] = canon
    return inv

def ler_planilha_inep(path):
    hdr = achar_header(path)
    df  = pd.read_excel(path, header=hdr)
    m   = colmap_padrao(df)
    df  = df.rename(columns=m)

    # padroniza tipos
    df["CO_MUNICIPIO"] = (
        df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    df["NO_MUNICIPIO"] = df["NO_MUNICIPIO"].astype(str).str.strip()
    df["NO_UF"]        = df["NO_UF"].astype(str).str.strip()

    # mantém também as colunas de indicadores (2005–2023) se existirem
    anos_cols = [c for c in df.columns if re.fullmatch(r"VL_INDICADOR_REND_\d{4}", str(c))]
    base_cols = ["NO_UF","CO_MUNICIPIO","NO_MUNICIPIO"]
    return df[base_cols + anos_cols]

def encontrar_col_indicador_mais_recente(df):
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m: anos.append(int(m.group(1)))
    if not anos:
        raise KeyError("Nenhuma coluna VL_INDICADOR_REND_YYYY encontrada.")
    ano_max = max(anos)
    return f"VL_INDICADOR_REND_{ano_max}", ano_max

def media_por_municipio(df, rotulo):
    col, ano = encontrar_col_indicador_mais_recente(df)
    vals = pd.to_numeric(df[col], errors="coerce")
    out = pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo: vals}).groupby("CO_MUNICIPIO", as_index=False).mean()
    return out, ano

def evolucao_long(df):
    """wide -> long (CO_MUNICIPIO, ANO, VALOR) para todas VL_INDICADOR_REND_YYYY."""
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m: anos.append(int(m.group(1)))
    if not anos:
        return pd.DataFrame(columns=["CO_MUNICIPIO","ANO","VALOR"])
    cols = [f"VL_INDICADOR_REND_{a}" for a in sorted(anos)]
    num  = df[["CO_MUNICIPIO"] + cols].copy()
    for c in cols:
        num[c] = pd.to_numeric(num[c], errors="coerce")
    long = num.melt(id_vars="CO_MUNICIPIO", value_vars=cols, var_name="COL", value_name="VALOR")
    long["ANO"] = long["COL"].str.extract(r"(\d{4})").astype(int)
    long = long.drop(columns=["COL"])
    return long

@st.cache_data(show_spinner=True)
def build_data():
    # leitura robusta
    df_ini = ler_planilha_inep(ARQ_INICIAIS)
    df_fin = ler_planilha_inep(ARQ_FINAIS)
    df_med = ler_planilha_inep(ARQ_MEDIO)

    # filtra apenas cidades ALP (por nome normalizado)
    for df in (df_ini, df_fin, df_med):
        df["NORM_MUN"] = df["NO_MUNICIPIO"].apply(nrm)
    base = (df_ini[["NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NORM_MUN"]]
            .drop_duplicates())
    base = base[base["NORM_MUN"].isin(CIDADES_NORM)].copy()

    # médias (pega ano mais recente existente em cada arquivo)
    ini, ano_ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin, ano_fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med, ano_med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))

    # versões em %
    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
        if c in base.columns:
            base[c + "_%"] = (base[c] * 100).round(2)

    # evolução (long) só dos municípios filtrados
    long_ini = evolucao_long(df_ini)
    long_fin = evolucao_long(df_fin)
    long_med = evolucao_long(df_med)

    evol = (long_ini.rename(columns={"VALOR":"APROVACAO_INICIAIS"})
                  .merge(long_fin.rename(columns={"VALOR":"APROVACAO_FINAIS"}),
                         on=["CO_MUNICIPIO","ANO"], how="outer")
                  .merge(long_med.rename(columns={"VALOR":"APROVACAO_MEDIO"}),
                         on=["CO_MUNICIPIO","ANO"], how="outer"))

    evol = evol.merge(base[["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","NORM_MUN"]].drop_duplicates(),
                      on="CO_MUNICIPIO", how="left")
    evol = evol[evol["NORM_MUN"].isin(CIDADES_NORM)].copy()
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        if c in evol.columns:
            evol[c + "_%"] = (evol[c]*100).round(2)

    meta = {"ANO_INI": ano_ini, "ANO_FIN": ano_fin, "ANO_MED": ano_med}
    return base, evol, meta

# ========================= UI =========================
st.set_page_config(page_title="Instituto Alpargatas — Painel Municípios", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

# checagem rápida dos arquivos
with st.expander("📁 Arquivos esperados em `dados/`", expanded=False):
    req = [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO]
    ok  = []
    for p in req:
        exists = os.path.exists(p)
        st.write(("✅" if exists else "❌"), p)
        ok.append(exists)
    st.code("\n".join(os.listdir("dados")), language="text")
if not all(ok):
    st.error("Arquivos faltando em `dados/`. Veja a lista acima.")
    st.stop()

with st.spinner("Carregando e processando…"):
    base, evol, meta = build_data()

# ---------------- KPIs ----------------
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios filtrados", f"{base['CO_MUNICIPIO'].nunique()}")
with c2: st.metric(f"Ano (Iniciais)", meta["ANO_INI"])
with c3: st.metric(f"Ano (Finais)",   meta["ANO_FIN"])
with c4: st.metric(f"Ano (Médio)",    meta["ANO_MED"])

st.markdown("### 📋 Tabela — Taxas mais recentes (%)")
cols_show = ["NO_UF","NO_MUNICIPIO",
             "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%"]
st.dataframe(base[cols_show].sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True), use_container_width=True)

st.markdown("### 📊 Barras — Aprovação (Iniciais) %")
tmp = base.sort_values("TAXA_APROVACAO_INICIAIS_%", ascending=False)
fig = px.bar(tmp, x="NO_MUNICIPIO", y="TAXA_APROVACAO_INICIAIS_%", color="NO_UF",
             labels={"NO_MUNICIPIO":"Município","TAXA_APROVACAO_INICIAIS_%":"Iniciais (%)","NO_UF":"UF"})
st.plotly_chart(fig, use_container_width=True)

st.markdown("### 📈 Evolução por município")
mun = st.selectbox("Escolha um município", sorted(base["NO_MUNICIPIO"].unique()))
e = evol[evol["NO_MUNICIPIO"] == mun].sort_values("ANO")
if e.empty:
    st.info("Sem série histórica disponível para este município.")
else:
    e2 = e.melt(id_vars=["ANO"], value_vars=[c for c in ["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%"] if c in e],
                var_name="Etapa", value_name="Aprovação (%)")
    e2["Etapa"] = e2["Etapa"].str.replace("_%","").str.replace("APROVACAO_","").str.title()
    fig2 = px.line(e2, x="ANO", y="Aprovação (%)", color="Etapa", markers=True)
    st.plotly_chart(fig2, use_container_width=True)
