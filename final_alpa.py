# final_alpa.py
import os, re, unicodedata
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Caminhos ----------------
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_MEDIO    = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"   # lido se existir

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
    """
    Procura colunas de aprovação/rendimento por ano.
    Aceita exemplos:
      - 'VL_INDICADOR_REND_2023'
      - 'Taxa de Aprovação 2021 (%)'
      - 'TX_APROVACAO_2019'
    Retorna {ano:int -> nome_col:str}
    """
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

# ---------- Média histórica (anos ≠ mais recente) ----------
def media_historica_por_municipio(df, rotulo_hist):
    _, ano_recente, mapping = encontrar_col_indicador_mais_recente(df)
    cols_hist = [mapping[a] for a in sorted(mapping) if a != ano_recente]
    if not cols_hist:
        cols_hist = [mapping[ano_recente]]  # fallback se só houver 1 ano
    tmp = df[["CO_MUNICIPIO"] + cols_hist].copy()
    for c in cols_hist:
        tmp[c] = to_num(tmp[c])
    tmp[rotulo_hist] = tmp[cols_hist].mean(axis=1)
    out = (tmp[["CO_MUNICIPIO", rotulo_hist]]
             .groupby("CO_MUNICIPIO", as_index=False)[rotulo_hist]
             .mean())
    return out
# -----------------------------------------------------------

def evolucao_long(df):
    """wide -> long (CO_MUNICIPIO, ANO, VALOR) usando mapeamento robusto."""
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

# ---------- Evasão (ODS, robusto e opcional) ----------
def ler_evasao(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    last_err = None
    for h in (8, 9, 7, 0):
        try:
            df = pd.read_excel(path, engine="odf", header=h)
            break
        except Exception as e:
            last_err = e
            df = None
    if df is None:
        raise last_err

    norm = {c: nrm(c) for c in df.columns}

    def pick(*cands, contains=None):
        alvo = {nrm(x) for x in cands}
        # match exato em normalizado
        for orig, n in norm.items():
            if n in alvo:
                return orig
        # fallback: contém termos
        if contains:
            for orig, n in norm.items():
                if all(t in n for t in contains):
                    return orig
        return None

    col_cod  = pick("CO_MUNICIPIO","CODIGO DO MUNICIPIO","CODIGO MUNICIPIO")
    col_nome = pick("NO_MUNICIPIO","NOME DO MUNICIPIO","MUNICIPIO")
    col_uf   = pick("NO_UF","UF","SIGLA DA UF")

    # indicadores (vários formatos comuns)
    col_fun  = pick("1_CAT3_CATFUN", contains=("FUN", "CAT")) or pick(contains=("EVAS","FUND"))
    col_med  = pick("1_CAT3_CATMED", contains=("MED", "CAT")) or pick(contains=("EVAS","MED"))

    if not all([col_cod, col_nome, col_uf, col_fun, col_med]):
        raise KeyError(f"Evasão: não reconheci colunas essenciais. Headers: {list(df.columns)}")

    out = df[[col_cod, col_nome, col_uf, col_fun, col_med]].copy()
    out.columns = ["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","Evasao_Fundamental","Evasao_Medio"]

    out["CO_MUNICIPIO"] = (
        out["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    for c in ["Evasao_Fundamental","Evasao_Medio"]:
        out[c] = pd.to_numeric(
            out[c].astype(str)
                  .str.replace("%","", regex=False)
                  .str.replace(",",".", regex=False),
            errors="coerce"
        )
    out["NORM_MUN"] = out["NO_MUNICIPIO"].apply(nrm)
    out = out[out["NORM_MUN"].isin(CIDADES_NORM)].copy()
    return out
# -------------------------------------------------------

# ---------------- App ----------------
st.set_page_config(page_title="Instituto Alpargatas — Painel Municípios", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

with st.expander("📁 Arquivos esperados em `dados/`", expanded=False):
    for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO]:
        st.write(("✅" if os.path.exists(p) else "❌"), p)
    if os.path.exists("dados"):
        st.code("\n".join(os.listdir("dados")), language="text")

@st.cache_data(show_spinner=True)
def build_data():
    df_ini = ler_planilha_inep(ARQ_INICIAIS)
    df_fin = ler_planilha_inep(ARQ_FINAIS)
    df_med = ler_planilha_inep(ARQ_MEDIO)

    # Normalização e filtro das cidades alvo
    for df in (df_ini, df_fin, df_med):
        df["NORM_MUN"] = df["NO_MUNICIPIO"].apply(nrm)
    base = (df_ini[["NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NORM_MUN"]]
            .drop_duplicates())
    base = base[base["NORM_MUN"].isin(CIDADES_NORM)].copy()

    # Médias (mais recente)
    ini, ano_ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin, ano_fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med, ano_med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    # Médias históricas (anos ≠ mais recente)
    ini_hist = media_historica_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS_HIST")
    fin_hist = media_historica_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS_HIST")
    med_hist = media_historica_por_municipio(df_med, "TAXA_APROVACAO_MEDIO_HIST")

    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left")
                 .merge(ini_hist, on="CO_MUNICIPIO", how="left")
                 .merge(fin_hist, on="CO_MUNICIPIO", how="left")
                 .merge(med_hist, on="CO_MUNICIPIO", how="left"))

    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO",
              "TAXA_APROVACAO_INICIAIS_HIST","TAXA_APROVACAO_FINAIS_HIST","TAXA_APROVACAO_MEDIO_HIST"]:
        if c in base.columns:
            base[c + "_%"] = (base[c]*100).round(2)

    # Evolução (apenas municípios filtrados)
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

    # Evasão (opcional)
    ev = None
    try:
        ev = ler_evasao(ARQ_EVASAO)
    except Exception as e:
        # não interrompe o app
        st.warning(f"Evasão não carregada: {e}")

    if ev is not None:
        base = base.merge(ev[["CO_MUNICIPIO","Evasao_Fundamental","Evasao_Medio"]],
                          on="CO_MUNICIPIO", how="left")
        # Reprovação a partir das aprovações (%)
        if "TAXA_APROVACAO_INICIAIS_%" in base and "TAXA_APROVACAO_FINAIS_%" in base:
            base["Reprovacao_Iniciais"] = (100 - base["TAXA_APROVACAO_INICIAIS_%"]).clip(lower=0)
            base["Reprovacao_Finais"]   = (100 - base["TAXA_APROVACAO_FINAIS_%"]).clip(lower=0)
        if "TAXA_APROVACAO_MEDIO_%" in base:
            base["Reprovacao_Medio"]    = (100 - base["TAXA_APROVACAO_MEDIO_%"]).clip(lower=0)

        # Índice simples de urgência (evasão + reprovação iniciais/finais)
        base["Urgencia"] = base[
            ["Evasao_Fundamental","Evasao_Medio","Reprovacao_Iniciais","Reprovacao_Finais"]
        ].sum(axis=1, skipna=True)

    meta = {"ANO_INI": ano_ini, "ANO_FIN": ano_fin, "ANO_MED": ano_med}
    return base, evol, meta

with st.spinner("Carregando e processando…"):
    base, evol, meta = build_data()

# ---------------- KPIs ----------------
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios filtrados", f"{base['CO_MUNICIPIO'].nunique()}")
with c2: st.metric("Ano (Iniciais)", meta["ANO_INI"])
with c3: st.metric("Ano (Finais)",   meta["ANO_FIN"])
with c4: st.metric("Ano (Médio)",    meta["ANO_MED"])

# ---------------- Tabela ----------------
st.markdown("### 📋 Taxas mais recentes e históricas (%)")
cols_show = ["NO_UF","NO_MUNICIPIO",
             "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%",
             "TAXA_APROVACAO_INICIAIS_HIST_%","TAXA_APROVACAO_FINAIS_HIST_%","TAXA_APROVACAO_MEDIO_HIST_%",
             "Evasao_Fundamental","Evasao_Medio","Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"]
cols_show = [c for c in cols_show if c in base.columns]
st.dataframe(
    base[cols_show].sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True),
    use_container_width=True
)

# ---------------- Gráfico barras (Iniciais) ----------------
st.markdown("### 📊 Aprovação (Iniciais) — %")
tmp = base.sort_values("TAXA_APROVACAO_INICIAIS_%", ascending=False)
fig = px.bar(tmp, x="NO_MUNICIPIO", y="TAXA_APROVACAO_INICIAIS_%", color="NO_UF",
             labels={"NO_MUNICIPIO":"Município","TAXA_APROVACAO_INICIAIS_%":"Iniciais (%)","NO_UF":"UF"})
st.plotly_chart(fig, use_container_width=True)

# ---------------- Evolução ----------------
st.markdown("### 📈 Evolução por município")
mun = st.selectbox("Escolha um município", sorted(base["NO_MUNICIPIO"].unique()))
e = evol[evol["NO_MUNICIPIO"] == mun].sort_values("ANO")
if e.empty:
    st.info("Sem série histórica disponível para este município.")
else:
    e2 = e.melt(id_vars=["ANO"], 
                value_vars=[c for c in ["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%"] if c in e],
                var_name="Etapa", value_name="Aprovação (%)")
    e2["Etapa"] = (e2["Etapa"].str.replace("_%","", regex=False)
                             .str.replace("APROVACAO_","", regex=False)
                             .str.title())
    fig2 = px.line(e2, x="ANO", y="Aprovação (%)", color="Etapa", markers=True)
    st.plotly_chart(fig2, use_container_width=True)

# ---------------- Urgência (se disponível) ----------------
if "Urgencia" in base.columns:
    st.markdown("### 🚨 Top urgência (maior = pior)")
    topn = st.slider("Quantos municípios exibir", 5, 30, 15)
    rank = (base[["NO_UF","NO_MUNICIPIO","Urgencia"]]
            .dropna(subset=["Urgencia"])
            .sort_values("Urgencia", ascending=False)
            .head(topn))
    st.plotly_chart(
        px.bar(rank, x="NO_MUNICIPIO", y="Urgencia", color="NO_UF"),
        use_container_width=True
    )

# ---------------- Debug opcional ----------------
with st.expander("🔎 Debug: colunas de indicadores reconhecidas / evasão"):
    for nome, caminho in [("Iniciais", ARQ_INICIAIS), ("Finais", ARQ_FINAIS), ("Médio", ARQ_MEDIO)]:
        try:
            df = pd.read_excel(caminho, header=achar_header(caminho))
            mapping = mapear_colunas_indicadores(df)
            st.write(f"**{nome}** → Anos detectados:", sorted(mapping.keys()))
            st.code("\n".join([f"{a}: {c}" for a,c in sorted(mapping.items())]), language="text")
        except Exception as e:
            st.warning(f"{nome}: {e}")
    # evasão
    if os.path.exists(ARQ_EVASAO):
        try:
            ev = ler_evasao(ARQ_EVASAO)
            st.write("**Evasão** → linhas lidas:", len(ev))
            st.dataframe(ev.head(10), use_container_width=True)
        except Exception as e:
            st.warning(f"Evasão: {e}")
