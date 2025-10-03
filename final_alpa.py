# final_alpa.py
import os, re, unicodedata
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Caminhos ----------------
ARQ_INICIAIS  = "dados/anos_iniciais.xlsx"
ARQ_FINAIS    = "dados/anos_finais.xlsx"
ARQ_MEDIO     = "dados/ensino_medio.xlsx"
ARQ_EVASAO    = "dados/evasao.ods"          # opcional (não usamos aqui)
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

# ---------- URGENTES.CSV ----------
def ler_urgentes(path_csv: str) -> pd.DataFrame:
    if not os.path.exists(path_csv):
        return pd.DataFrame()

    # leitura robusta (vírgula ou ponto-e-vírgula)
    try:
        u = pd.read_csv(path_csv)
        if u.shape[1] == 1:
            u = pd.read_csv(path_csv, sep=";")
    except Exception:
        u = pd.read_csv(path_csv, sep=";")

    # renomes mais comuns -> nomes que usaremos no merge
    ren = {}
    for c in list(u.columns):
        cn = nrm(c)
        if cn == "UF_SIGLA": ren[c] = "NO_UF"
        if cn in {"EVASAO-FUNDAMENTAL","EVASAO - FUNDAMENTAL","EVASAO FUNDAMENTAL"}:
            ren[c] = "Evasao_Fundamental"
        if cn in {"EVASAO-MEDIO","EVASAO - MEDIO","EVASAO MEDIO"}:
            ren[c] = "Evasao_Medio"
        if cn in {
            "MEDIA_HISTORICA","MEDIA HISTORICA","MEDIA-HISTORICA",
            "MEDIA HISTORICA %","MEDIA_HISTORICA_%","MEDIA HISTORICA (%)",
            "MEDIA_HISTORICA(%)","MEDIA HISTORICA PERCENT"
        }:
            ren[c] = "MEDIA_HISTORICA_%"
    if ren:
        u = u.rename(columns=ren)

    # normaliza strings
    for col in [c for c in ["NO_UF","MUNICIPIO_NOME_ALP","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA"] if c in u]:
        u[col] = u[col].astype(str).str.strip()

    # chave de match por nome (prefere coluna da Alpa)
    base_nome = u["MUNICIPIO_NOME_ALP"] if "MUNICIPIO_NOME_ALP" in u else u.get("NO_MUNICIPIO")
    if base_nome is None:
        return pd.DataFrame()
    u["NORM_MUN"] = base_nome.apply(nrm)

    # preferir linhas 'Total/Total' quando houver múltiplas por município
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
        "Reprovacao_Iniciais","Reprovacao_Finais",
        "Urgencia","MEDIA_HISTORICA_%"
    ]
    for c in [c for c in num_cols if c in u]:
        u[c] = to_num(u[c])

    # só o que interessa para injeção
    cols = [c for c in [
        "NO_UF","NORM_MUN","Evasao_Fundamental","Evasao_Medio",
        "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia",
        "MEDIA_HISTORICA_%","NO_LOCALIZACAO","NO_DEPENDENCIA"
    ] if c in u.columns]
    return u[cols]

# ---------------- App ----------------
st.set_page_config(page_title="Instituto Alpargatas — Painel Municípios", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

with st.expander("📁 Arquivos esperados em `dados/`", expanded=False):
    for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO, ARQ_URGENTES]:
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

    # Médias (ano mais recente de cada arquivo)
    ini, ano_ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin, ano_fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med, ano_med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))

    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
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

    # ---------- URGENTES.CSV (injeta + filtra apenas urgentes)
    urg = ler_urgentes(ARQ_URGENTES)
    if not urg.empty:
        urgentes_set = set(urg["NORM_MUN"])
        base = base[base["NORM_MUN"].isin(urgentes_set)].copy()
        evol = evol[evol["NORM_MUN"].isin(urgentes_set)].copy()

        # merge (evita colisões com sufixo no lado direito)
        base = base.merge(urg, on=["NO_UF","NORM_MUN"], how="left", suffixes=("", "_urg"))

        # reprovação caso não venha no CSV
        if "Reprovacao_Iniciais" not in base.columns and "TAXA_APROVACAO_INICIAIS_%" in base:
            base["Reprovacao_Iniciais"] = (100 - base["TAXA_APROVACAO_INICIAIS_%"]).clip(lower=0)
        if "Reprovacao_Finais" not in base.columns and "TAXA_APROVACAO_FINAIS_%" in base:
            base["Reprovacao_Finais"] = (100 - base["TAXA_APROVACAO_FINAIS_%"]).clip(lower=0)

        # urgência caso não tenha vindo
        if "Urgencia" not in base.columns:
            base["Urgencia"] = base[
                [c for c in ["Evasao_Fundamental","Evasao_Medio","Reprovacao_Iniciais","Reprovacao_Finais"] if c in base]
            ].sum(axis=1, skipna=True)

    # média geral atual (%) para ranking
    base["APROVACAO_MEDIA_GERAL_%"] = base[
        [c for c in ["TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%"] if c in base]
    ].mean(axis=1, skipna=True)

    meta = {"ANO_INI": ano_ini, "ANO_FIN": ano_fin, "ANO_MED": ano_med}
    return base, evol, meta

with st.spinner("Carregando e processando…"):
    base, evol, meta = build_data()

# ---------------- KPIs ----------------
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios no painel", f"{base['CO_MUNICIPIO'].nunique()}")
with c2: st.metric("Ano (Iniciais)", meta["ANO_INI"])
with c3: st.metric("Ano (Finais)",   meta["ANO_FIN"])
with c4: st.metric("Ano (Médio)",    meta["ANO_MED"])

# ---------------- Tabela ----------------
st.markdown("### 📋 Tabela (com urgência & evasão)")
cols_show = [
    "NO_UF","NO_MUNICIPIO",
    "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%",
    "Evasao_Fundamental","Evasao_Medio",
    "Reprovacao_Iniciais","Reprovacao_Finais",
    "Urgencia","APROVACAO_MEDIA_GERAL_%","MEDIA_HISTORICA_%"
]
cols_show = [c for c in cols_show if c in base.columns]
st.dataframe(
    base[cols_show].sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True),
    use_container_width=True
)

# ---------------- Gráfico barras ----------------
st.markdown("### 🔥 Top urgência")
topn = st.slider("Quantos municípios exibir", 5, 30, 15, 1)
rank = (base[["NO_UF","NO_MUNICIPIO","Urgencia"]]
        .dropna(subset=["Urgencia"])
        .sort_values("Urgencia", ascending=False)
        .head(topn))
st.plotly_chart(
    px.bar(rank, x="NO_MUNICIPIO", y="Urgencia", color="NO_UF",
           labels={"NO_MUNICIPIO":"Município","Urgencia":"Índice de urgência","NO_UF":"UF"}),
    use_container_width=True
)

# ---------------- Evolução ----------------
st.markdown("### 📈 Evolução por município (aprov. %)")
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
    st.plotly_chart(px.line(e2, x="ANO", y="Aprovação (%)", color="Etapa", markers=True),
                    use_container_width=True)

# ---------------- Debug opcional ----------------
with st.expander("🔎 Debug: colunas de indicadores reconhecidas"):
    for nome, caminho in [("Iniciais", ARQ_INICIAIS), ("Finais", ARQ_FINAIS), ("Médio", ARQ_MEDIO)]:
        try:
            df = pd.read_excel(caminho, header=achar_header(caminho))
            mapping = mapear_colunas_indicadores(df)
            st.write(f"**{nome}** → Anos detectados:", sorted(mapping.keys()))
            st.code("\n".join([f"{a}: {c}" for a,c in sorted(mapping.items())]), language="text")
        except Exception as e:
            st.warning(f"{nome}: {e}")


