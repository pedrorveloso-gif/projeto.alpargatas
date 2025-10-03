# final_alpa.py — versão com Histórico + Evasão + Urgência
import os, re, unicodedata
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------- Caminhos ----------------
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_MEDIO    = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"   # ODS (lido com odfpy)

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
    """Converte textos tipo '95,3%' -> 95.3 (ou proporção se você multiplicar depois)."""
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
    Exemplos aceitos:
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
    """Média por município da coluna mais recente (já em proporção 0–1 se origem for 0–1)."""
    col, ano, _ = encontrar_col_indicador_mais_recente(df)
    vals = to_num(df[col])  # se origem é 0–1, mantenha; se é 0–100, lá adiante multiplicamos/dividimos
    out = (pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo: vals})
             .groupby("CO_MUNICIPIO", as_index=False)[rotulo]
             .mean())
    return out, ano

def medias_historicas(df, rotulo_hist):
    """Média dos anos ≠ mais recente (histórico) por município."""
    _, ano_rec, mapping = encontrar_col_indicador_mais_recente(df)
    cols_hist = [c for a,c in mapping.items() if a != ano_rec]
    if not cols_hist:
        # se não tem histórico, use o próprio recente para não quebrar
        cols_hist = [mapping[ano_rec]]
    tmp = df[["CO_MUNICIPIO"] + cols_hist].copy()
    for c in cols_hist:
        tmp[c] = to_num(tmp[c])
    out = tmp.groupby("CO_MUNICIPIO", as_index=False)[cols_hist].mean()
    out[rotulo_hist] = out[cols_hist].mean(axis=1, skipna=True)
    return out[["CO_MUNICIPIO", rotulo_hist]]

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

# --------- Evasão (robusto e opcional) ---------
def _find_col(df, candidatos):
    cand_norm = {nrm(c) for c in candidatos}
    for c in df.columns:
        if nrm(c) in cand_norm:
            return c
    return None

def carrega_evasao(path):
    """Lê evasao.ods e devolve CO_MUNICIPIO, Evasao_Fundamental, Evasao_Medio (em % numérico)."""
    if not os.path.exists(path):
        return None  # app segue sem evasão
    # tenta header comum; se falhar, tenta 0
    try:
        df = pd.read_excel(path, engine="odf", header=8)
    except Exception:
        df = pd.read_excel(path, engine="odf", header=0)

    col_cod = _find_col(df, ["CO_MUNICIPIO","CO MUNICIPIO","CODIGO DO MUNICIPIO","CÓDIGO DO MUNICIPIO"])
    col_fun = _find_col(df, ["1_CAT3_CATFUN","EVASAO FUNDAMENTAL","EVASÃO FUNDAMENTAL"])
    col_med = _find_col(df, ["1_CAT3_CATMED","EVASAO MEDIO","EVASÃO MÉDIO","EVASAO MÉDIO","EVASAO ENSINO MEDIO"])
    if not col_cod or not col_fun or not col_med:
        # não quebra o app
        return None

    out = df[[col_cod, col_fun, col_med]].copy()
    out.columns = ["CO_MUNICIPIO","Evasao_Fundamental","Evasao_Medio"]

    # normaliza código
    out["CO_MUNICIPIO"] = (
        out["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )
    # normaliza números: "12,3%" -> 12.3
    for c in ["Evasao_Fundamental","Evasao_Medio"]:
        out[c] = (out[c].astype(str)
                        .str.replace("%","", regex=False)
                        .str.replace(",", ".", regex=False))
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

# ---------------- App ----------------
st.set_page_config(page_title="Instituto Alpargatas — Painel Municípios", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

with st.expander("📁 Arquivos esperados em `dados/`", expanded=False):
    for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO]:
        st.write(("✅" if os.path.exists(p) else "❌"), p)
    try:
        st.code("\n".join(os.listdir("dados")), language="text")
    except Exception:
        st.code("(pasta `dados/` não encontrada)", language="text")

@st.cache_data(show_spinner=True)
def build_data():
    # --- Leitura INEP
    df_ini = ler_planilha_inep(ARQ_INICIAIS)
    df_fin = ler_planilha_inep(ARQ_FINAIS)
    df_med = ler_planilha_inep(ARQ_MEDIO)

    # --- Filtro cidades alvo
    for df in (df_ini, df_fin, df_med):
        df["NORM_MUN"] = df["NO_MUNICIPIO"].apply(nrm)
    base = (df_ini[["NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NORM_MUN"]]
            .drop_duplicates())
    base = base[base["NORM_MUN"].isin(CIDADES_NORM)].copy()

    # --- Médias (recente)
    ini, ano_ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin, ano_fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med, ano_med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")
    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))
    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
        if c in base.columns:
            base[c + "_%"] = (base[c]*100).round(2)

    # --- Médias históricas (anos ≠ mais recente)
    ini_h = medias_historicas(df_ini, "TAXA_APROVACAO_INICIAIS_HIST")
    fin_h = medias_historicas(df_fin, "TAXA_APROVACAO_FINAIS_HIST")
    med_h = medias_historicas(df_med, "TAXA_APROVACAO_MEDIO_HIST")
    base = (base.merge(ini_h, on="CO_MUNICIPIO", how="left")
                 .merge(fin_h, on="CO_MUNICIPIO", how="left")
                 .merge(med_h, on="CO_MUNICIPIO", how="left"))
    for c in ["TAXA_APROVACAO_INICIAIS_HIST","TAXA_APROVACAO_FINAIS_HIST","TAXA_APROVACAO_MEDIO_HIST"]:
        if c in base.columns:
            base[c + "_%"] = (base[c]*100).round(2)

    # --- Evasão (opcional)
    ev = carrega_evasao(ARQ_EVASAO)
    if ev is not None:
        base = base.merge(ev, on="CO_MUNICIPIO", how="left")

        # Reprovação a partir da aprovação (em %)
        if "TAXA_APROVACAO_INICIAIS_%" in base:
            base["Reprovacao_Iniciais"] = (100 - base["TAXA_APROVACAO_INICIAIS_%"]).clip(lower=0)
        if "TAXA_APROVACAO_FINAIS_%" in base:
            base["Reprovacao_Finais"]   = (100 - base["TAXA_APROVACAO_FINAIS_%"]).clip(lower=0)
        if "TAXA_APROVACAO_MEDIO_%" in base:
            base["Reprovacao_Medio"]    = (100 - base["TAXA_APROVACAO_MEDIO_%"]).clip(lower=0)

        # Urgência = Evasão(Fund + Médio) + Reprovação(Iniciais + Finais)
        comp = []
        for c in ["Evasao_Fundamental","Evasao_Medio","Reprovacao_Iniciais","Reprovacao_Finais"]:
            if c in base: comp.append(c)
        if comp:
            base["Urgencia"] = base[comp].sum(axis=1, skipna=True)

    # --- Evolução
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

    meta = {"ANO_INI": ano_ini, "ANO_FIN": ano_fin, "ANO_MED": ano_med, "tem_evasao": ev is not None}
    return base, evol, meta

with st.spinner("Carregando e processando…"):
    base, evol, meta = build_data()

# ---------------- KPIs ----------------
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios filtrados", f"{base['CO_MUNICIPIO'].nunique()}")
with c2: st.metric("Ano (Iniciais)", meta["ANO_INI"])
with c3: st.metric("Ano (Finais)",   meta["ANO_FIN"])
with c4: st.metric("Ano (Médio)",    meta["ANO_MED"])

# KPI extra: urgência média (se houver evasão)
if meta.get("tem_evasao", False) and "Urgencia" in base:
    st.metric("Urgência média", f"{base['Urgencia'].mean(skipna=True):.2f}")

# ---------------- Tabela ----------------
st.markdown("### 📋 Taxas mais recentes (%) + Histórico + Evasão")
cols_show = [
    "NO_UF","NO_MUNICIPIO",
    "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%",
    "TAXA_APROVACAO_INICIAIS_HIST_%","TAXA_APROVACAO_FINAIS_HIST_%","TAXA_APROVACAO_MEDIO_HIST_%",
]
if "Evasao_Fundamental" in base: cols_show += ["Evasao_Fundamental"]
if "Evasao_Medio" in base:       cols_show += ["Evasao_Medio"]
for c in ["Reprovacao_Iniciais","Reprovacao_Finais","Reprovacao_Medio","Urgencia"]:
    if c in base: cols_show.append(c)

st.dataframe(
    base[cols_show].sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True),
    use_container_width=True
)

# ---------------- Gráfico barras (Iniciais) ----------------
st.markdown("### 📊 Aprovação (Iniciais) — %")
if "TAXA_APROVACAO_INICIAIS_%" in base:
    tmp = base.sort_values("TAXA_APROVACAO_INICIAIS_%", ascending=False)
    fig = px.bar(tmp, x="NO_MUNICIPIO", y="TAXA_APROVACAO_INICIAIS_%", color="NO_UF",
                 labels={"NO_MUNICIPIO":"Município","TAXA_APROVACAO_INICIAIS_%":"Iniciais (%)","NO_UF":"UF"})
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Coluna de Iniciais não disponível.")

# ---------------- Evolução ----------------
st.markdown("### 📈 Evolução por município")
opts = sorted(base["NO_MUNICIPIO"].unique()) if "NO_MUNICIPIO" in base else []
mun = st.selectbox("Escolha um município", opts) if opts else None
if mun:
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

# ---------------- Top urgência ----------------
if meta.get("tem_evasao", False) and "Urgencia" in base:
    st.markdown("### 🚨 Top urgência")
    topn = st.slider("Quantos municípios exibir", min_value=5, max_value=30, value=15, step=1)
    rank_urg = (base[["NO_UF","NO_MUNICIPIO","Urgencia"]]
                .dropna(subset=["Urgencia"])
                .sort_values("Urgencia", ascending=False)
                .head(topn))
    st.plotly_chart(
        px.bar(rank_urg, x="NO_MUNICIPIO", y="Urgencia", color="NO_UF",
               title=f"Top {len(rank_urg)} — urgência (maior = pior)")
          .update_layout(xaxis_title="", yaxis_title="Índice"),
        use_container_width=True
    )

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
