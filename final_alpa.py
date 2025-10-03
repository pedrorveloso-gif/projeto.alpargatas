# final_alpa.py — Painel Municípios (INEP) + URGENTES hardcoded
import os, re, unicodedata
import pandas as pd
import plotly.express as px
import streamlit as st

# ==========================================================
# 0) ARQUIVOS INEP (os que já rodavam)
# ==========================================================
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_MEDIO    = "dados/ensino_medio.xlsx"

# ==========================================================
# 1) TABELA URGENTES — INJETADA AQUI (a partir das suas imagens)
#    Se quiser alterar, edite os dicionários abaixo.
#    Números de evasão/urgência/média histórica estão em %.
# ==========================================================
URGENTES_DATA = [
    # Serra Redonda (3 linhas)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"SERRA REDONDA","NO_MUNICIPIO":"Serra Redonda","NO_LOCALIZACAO":"Urbana","NO_DEPENDENCIA":"Total","Evasao_Fundamental":6.15,"Evasao_Medio":13.6,"TAXA_APROVACAO_INICIAIS":0.92035,"TAXA_APROVACAO_FINAIS":0.7574,"Reprovacao_Iniciais":7.965,"Reprovacao_Finais":24.26,"Urgencia":51.975,"MEDIA_HISTORICA":77.64},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"SERRA REDONDA","NO_MUNICIPIO":"Serra Redonda","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Pública","Evasao_Fundamental":6.10,"Evasao_Medio":13.6,"TAXA_APROVACAO_INICIAIS":0.92035,"TAXA_APROVACAO_FINAIS":0.7574,"Reprovacao_Iniciais":7.965,"Reprovacao_Finais":24.26,"Urgencia":51.925,"MEDIA_HISTORICA":77.64},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"SERRA REDONDA","NO_MUNICIPIO":"Serra Redonda","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Total","Evasao_Fundamental":5.80,"Evasao_Medio":13.6,"TAXA_APROVACAO_INICIAIS":0.92035,"TAXA_APROVACAO_FINAIS":0.7574,"Reprovacao_Iniciais":7.965,"Reprovacao_Finais":24.26,"Urgencia":51.625,"MEDIA_HISTORICA":77.64},

    # Santa Rita (4 linhas)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"SANTA RITA","NO_MUNICIPIO":"Santa Rita","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Pública","Evasao_Fundamental":5.50,"Evasao_Medio":15.2,"TAXA_APROVACAO_INICIAIS":0.92710,"TAXA_APROVACAO_FINAIS":0.8197,"Reprovacao_Iniciais":7.290,"Reprovacao_Finais":18.03,"Urgencia":46.020,"MEDIA_HISTORICA":78.37},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"SANTA RITA","NO_MUNICIPIO":"Santa Rita","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Total","Evasao_Fundamental":5.00,"Evasao_Medio":14.5,"TAXA_APROVACAO_INICIAIS":0.92710,"TAXA_APROVACAO_FINAIS":0.8197,"Reprovacao_Iniciais":7.290,"Reprovacao_Finais":18.03,"Urgencia":44.820,"MEDIA_HISTORICA":78.37},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"SANTA RITA","NO_MUNICIPIO":"Santa Rita","NO_LOCALIZACAO":"Rural","NO_DEPENDENCIA":"Total","Evasao_Fundamental":5.90,"Evasao_Medio":13.4,"TAXA_APROVACAO_INICIAIS":0.92710,"TAXA_APROVACAO_FINAIS":0.8197,"Reprovacao_Iniciais":7.290,"Reprovacao_Finais":18.03,"Urgencia":44.620,"MEDIA_HISTORICA":78.37},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"SANTA RITA","NO_MUNICIPIO":"Santa Rita","NO_LOCALIZACAO":"Urbana","NO_DEPENDENCIA":"Total","Evasao_Fundamental":4.70,"Evasao_Medio":14.5,"TAXA_APROVACAO_INICIAIS":0.92710,"TAXA_APROVACAO_FINAIS":0.8197,"Reprovacao_Iniciais":7.290,"Reprovacao_Finais":18.03,"Urgencia":44.520,"MEDIA_HISTORICA":78.37},

    # Bananeiras (3 linhas)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"BANANEIRAS","NO_MUNICIPIO":"Bananeiras","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Pública","Evasao_Fundamental":4.20,"Evasao_Medio":18.1,"TAXA_APROVACAO_INICIAIS":0.97130,"TAXA_APROVACAO_FINAIS":0.8612,"Reprovacao_Iniciais":2.870,"Reprovacao_Finais":13.88,"Urgencia":39.050,"MEDIA_HISTORICA":79.88},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"BANANEIRAS","NO_MUNICIPIO":"Bananeiras","NO_LOCALIZACAO":"Urbana","NO_DEPENDENCIA":"Total","Evasao_Fundamental":3.50,"Evasao_Medio":18.2,"TAXA_APROVACAO_INICIAIS":0.97130,"TAXA_APROVACAO_FINAIS":0.8612,"Reprovacao_Iniciais":2.870,"Reprovacao_Finais":13.88,"Urgencia":38.450,"MEDIA_HISTORICA":79.88},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"BANANEIRAS","NO_MUNICIPIO":"Bananeiras","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Total","Evasao_Fundamental":3.60,"Evasao_Medio":17.0,"TAXA_APROVACAO_INICIAIS":0.97130,"TAXA_APROVACAO_FINAIS":0.8612,"Reprovacao_Iniciais":2.870,"Reprovacao_Finais":13.88,"Urgencia":37.350,"MEDIA_HISTORICA":79.88},

    # João Pessoa (4 linhas)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"JOÃO PESSOA","NO_MUNICIPIO":"João Pessoa","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Pública","Evasao_Fundamental":4.30,"Evasao_Medio":10.6,"TAXA_APROVACAO_INICIAIS":0.94490,"TAXA_APROVACAO_FINAIS":0.8333,"Reprovacao_Iniciais":5.510,"Reprovacao_Finais":16.67,"Urgencia":37.080,"MEDIA_HISTORICA":83.61},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"JOÃO PESSOA","NO_MUNICIPIO":"João Pessoa","NO_LOCALIZACAO":"Urbana","NO_DEPENDENCIA":"Total","Evasao_Fundamental":4.10,"Evasao_Medio":10.2,"TAXA_APROVACAO_INICIAIS":0.94490,"TAXA_APROVACAO_FINAIS":0.8333,"Reprovacao_Iniciais":5.510,"Reprovacao_Finais":16.67,"Urgencia":36.480,"MEDIA_HISTORICA":83.61},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"JOÃO PESSOA","NO_MUNICIPIO":"João Pessoa","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Total","Evasao_Fundamental":4.10,"Evasao_Medio":10.2,"TAXA_APROVACAO_INICIAIS":0.94490,"TAXA_APROVACAO_FINAIS":0.8333,"Reprovacao_Iniciais":5.510,"Reprovacao_Finais":16.67,"Urgencia":36.480,"MEDIA_HISTORICA":83.61},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"JOÃO PESSOA","NO_MUNICIPIO":"João Pessoa","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Privada","Evasao_Fundamental":3.90,"Evasao_Medio":9.3,"TAXA_APROVACAO_INICIAIS":0.94490,"TAXA_APROVACAO_FINAIS":0.8333,"Reprovacao_Iniciais":5.510,"Reprovacao_Finais":16.67,"Urgencia":35.380,"MEDIA_HISTORICA":83.61},

    # Ingá (1)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"INGÁ","NO_MUNICIPIO":"Ingá","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Pública","Evasao_Fundamental":4.10,"Evasao_Medio":19.3,"TAXA_APROVACAO_INICIAIS":0.98430,"TAXA_APROVACAO_FINAIS":0.8970,"Reprovacao_Iniciais":1.570,"Reprovacao_Finais":10.30,"Urgencia":35.270,"MEDIA_HISTORICA":76.76},

    # Caturité (3)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"CATURITÉ","NO_MUNICIPIO":"Caturité","NO_LOCALIZACAO":"Urbana","NO_DEPENDENCIA":"Total","Evasao_Fundamental":3.90,"Evasao_Medio":18.1,"TAXA_APROVACAO_INICIAIS":0.97730,"TAXA_APROVACAO_FINAIS":0.8953,"Reprovacao_Iniciais":2.270,"Reprovacao_Finais":10.47,"Urgencia":34.740,"MEDIA_HISTORICA":84.80},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"CATURITÉ","NO_MUNICIPIO":"Caturité","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Pública","Evasao_Fundamental":3.20,"Evasao_Medio":18.1,"TAXA_APROVACAO_INICIAIS":0.97730,"TAXA_APROVACAO_FINAIS":0.8953,"Reprovacao_Iniciais":2.270,"Reprovacao_Finais":10.47,"Urgencia":34.040,"MEDIA_HISTORICA":84.80},
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"CATURITÉ","NO_MUNICIPIO":"Caturité","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Total","Evasao_Fundamental":3.20,"Evasao_Medio":18.1,"TAXA_APROVACAO_INICIAIS":0.97730,"TAXA_APROVACAO_FINAIS":0.8953,"Reprovacao_Iniciais":2.270,"Reprovacao_Finais":10.47,"Urgencia":34.040,"MEDIA_HISTORICA":84.80},

    # Baía da Traição (1)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"BAÍA DA TRAIÇÃO","NO_MUNICIPIO":"Baía da Traição","NO_LOCALIZACAO":"Rural","NO_DEPENDENCIA":"Total","Evasao_Fundamental":3.40,"Evasao_Medio":8.9,"TAXA_APROVACAO_INICIAIS":0.92035,"TAXA_APROVACAO_FINAIS":0.8661,"Reprovacao_Iniciais":7.965,"Reprovacao_Finais":13.39,"Urgencia":33.655,"MEDIA_HISTORICA":85.41},

    # Campina Grande (1)
    {"UF_SIGLA":"PB","MUNICIPIO_NOME_ALP":"CAMPINA GRANDE","NO_MUNICIPIO":"Campina Grande","NO_LOCALIZACAO":"Total","NO_DEPENDENCIA":"Pública","Evasao_Fundamental":5.60,"Evasao_Medio":10.4,"TAXA_APROVACAO_INICIAIS":0.98190,"TAXA_APROVACAO_FINAIS":0.8416,"Reprovacao_Iniciais":1.810,"Reprovacao_Finais":15.84,"Urgencia":33.650,"MEDIA_HISTORICA":82.14},
]

# ==========================================================
# 2) Funções utilitárias (as do app que já rodava)
# ==========================================================
def nrm(x):
    if pd.isna(x): return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII","ignore").decode("ASCII")
    s = s.replace("–","-").replace("—","-")
    return " ".join(s.upper().split())

def to_num(s):
    return pd.to_numeric(
        pd.Series(s).astype(str)
        .str.replace("%","", regex=False)
        .str.replace("\u2212","-", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce"
    )

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

def mapear_colunas_indicadores(df):
    mapping = {}
    for col in df.columns:
        s = nrm(col)
        m = re.search(r"(\d{4})", s)
        if not m: 
            continue
        ano = int(m.group(1))
        if 2000 <= ano <= 2100 and (
            "APROV" in s or ("INDICADOR" in s and "REND" in s) or s.startswith("VL_INDICADOR_REND_")
        ):
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
    _, _, mapping = encontrar_col_indicador_mais_recente(df)
    if not mapping:
        return pd.DataFrame(columns=["CO_MUNICIPIO","ANO","VALOR"])
    tmp = df[["CO_MUNICIPIO"] + list(mapping.values())].copy()
    ren = {orig: f"VALOR_{ano}" for ano, orig in mapping.items()}
    tmp = tmp.rename(columns=ren)
    valor_cols = [c for c in tmp.columns if c.startswith("VALOR_")]
    for c in valor_cols: tmp[c] = to_num(tmp[c])
    long = tmp.melt(id_vars="CO_MUNICIPIO", value_vars=valor_cols,
                    var_name="COL", value_name="VALOR")
    long["ANO"] = long["COL"].str.extract(r"(\d{4})").astype(int)
    long = long.drop(columns=["COL"])
    return long

# ==========================================================
# 3) Funções para preparar URGENTES e casar com INEP
# ==========================================================
def urgentes_df() -> pd.DataFrame:
    u = pd.DataFrame(URGENTES_DATA).copy()
    # normaliza chaves
    u["UF_CHAVE"]  = u.get("UF_SIGLA","").map(nrm)
    u["MUN_CHAVE"] = u.get("NO_MUNICIPIO","").map(nrm)
    # numéricos
    for c in ["Evasao_Fundamental","Evasao_Medio","Urgencia","MEDIA_HISTORICA",
              "TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS",
              "Reprovacao_Iniciais","Reprovacao_Finais"]:
        if c in u: u[c] = to_num(u[c])
    # preferir a linha mais representativa por município:
    # 1) Total/Total; 2) Total/Pública; 3) Total/*; 4) Urbana/Total; 5) primeira
    def rank_row(r):
        loc = (r.get("NO_LOCALIZACAO") or "").strip().lower()
        dep = (r.get("NO_DEPENDENCIA") or "").strip().lower()
        if loc=="total" and dep=="total":   return 0
        if loc=="total" and dep=="pública": return 1
        if loc=="total":                    return 2
        if loc=="urbana" and dep=="total":  return 3
        return 9
    u["rank"] = u.apply(rank_row, axis=1)
    u = u.sort_values(["UF_CHAVE","MUN_CHAVE","rank"]).groupby(["UF_CHAVE","MUN_CHAVE"], as_index=False).first()
    u["MEDIA_HISTORICA_%"] = u["MEDIA_HISTORICA"].round(2)
    return u.drop(columns=["rank"], errors="ignore")

# ==========================================================
# 4) APP
# ==========================================================
st.set_page_config(page_title="Instituto Alpargatas — Municípios", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios (com URGENTES hardcoded)")

with st.expander("📁 Arquivos esperados em `dados/`", expanded=False):
    for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO]:
        st.write(("✅" if os.path.exists(p) else "❌"), p)
    try:
        st.code("\n".join(os.listdir("dados")), language="text")
    except Exception:
        st.code("(pasta dados/ não encontrada)", language="text")

@st.cache_data(show_spinner=True)
def build_data():
    # --- INEP
    df_ini = ler_planilha_inep(ARQ_INICIAIS)
    df_fin = ler_planilha_inep(ARQ_FINAIS)
    df_med = ler_planilha_inep(ARQ_MEDIO)

    # base única por município
    for df in (df_ini, df_fin, df_med):
        df["MUN_CHAVE"] = df["NO_MUNICIPIO"].apply(nrm)
        df["UF_CHAVE"]  = df["NO_UF"].apply(nrm)
    base = df_ini[["NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","MUN_CHAVE","UF_CHAVE"]].drop_duplicates()

    # --- Médias (ano mais recente de cada arquivo)
    ini, ano_ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin, ano_fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med, ano_med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))

    for c in ["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"]:
        if c in base.columns: base[c + "_%"] = (base[c]*100).round(2)

    # --- Evolução (para o gráfico)
    long_ini = evolucao_long(df_ini)
    long_fin = evolucao_long(df_fin)
    long_med = evolucao_long(df_med)
    evol = (long_ini.rename(columns={"VALOR":"APROVACAO_INICIAIS"})
                 .merge(long_fin.rename(columns={"VALOR":"APROVACAO_FINAIS"}),
                        on=["CO_MUNICIPIO","ANO"], how="outer")
                 .merge(long_med.rename(columns={"VALOR":"APROVACAO_MEDIO"}),
                        on=["CO_MUNICIPIO","ANO"], how="outer"))
    evol = evol.merge(base[["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","MUN_CHAVE"]].drop_duplicates(),
                      on="CO_MUNICIPIO", how="left")
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        if c in evol.columns: evol[c + "_%"] = (evol[c]*100).round(2)

    # --- URGENTES (hardcoded) — mantém SOMENTE os urgentes
    urg = urgentes_df()
    base = base.merge(
        urg[["UF_CHAVE","MUN_CHAVE","Evasao_Fundamental","Evasao_Medio","Urgencia","MEDIA_HISTORICA_%"]],
        on=["UF_CHAVE","MUN_CHAVE"], how="inner"
    )

    meta = {"ANO_INI": ano_ini, "ANO_FIN": ano_fin, "ANO_MED": ano_med, "N_URG": int(base["CO_MUNICIPIO"].nunique())}
    return base, evol, meta, urg

with st.spinner("Carregando e processando…"):
    base, evol, meta, urg_tab = build_data()

# ==========================================================
# KPIs
# ==========================================================
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios URGENTES", f"{meta['N_URG']}")
with c2: st.metric("Ano (Iniciais)", meta["ANO_INI"])
with c3: st.metric("Ano (Finais)",   meta["ANO_FIN"])
with c4: st.metric("Ano (Médio)",    meta["ANO_MED"])

st.divider()

# ==========================================================
# Tabela principal (somente urgentes)
# ==========================================================
st.markdown("### 📋 Municípios urgentes — aprovação, evasão e urgência")
cols_show = [
    "NO_UF","NO_MUNICIPIO",
    "TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%",
    "Evasao_Fundamental","Evasao_Medio","Urgencia","MEDIA_HISTORICA_%"
]
cols_show = [c for c in cols_show if c in base.columns]
st.dataframe(
    base[cols_show].sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True),
    use_container_width=True
)

st.divider()

# ==========================================================
# Gráfico — Urgência por município (ranking)
# ==========================================================
st.markdown("### 🔥 Urgência por município (maior = pior)")
rank = (base[["NO_MUNICIPIO","NO_UF","Urgencia"]]
        .dropna(subset=["Urgencia"])
        .sort_values("Urgencia", ascending=False))
fig_u = px.bar(rank, x="Urgencia", y="NO_MUNICIPIO", color="NO_UF",
               orientation="h", labels={"NO_MUNICIPIO":"Município","Urgencia":"Grau de urgência","NO_UF":"UF"})
st.plotly_chart(fig_u, use_container_width=True)

st.divider()

# ==========================================================
# Evolução — apenas para os urgentes selecionados (média)
# ==========================================================
st.markdown("### 📈 Série temporal — aprovação média (apenas urgentes)")
evo_f = evol[evol["NO_MUNICIPIO"].isin(base["NO_MUNICIPIO"].unique())]
serie = (evo_f.groupby(["ANO"], as_index=False)["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%"]
              .mean(numeric_only=True))
serie = serie.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
serie["Etapa"] = (serie["Etapa"].str.replace("_%","", regex=False)
                               .str.replace("APROVACAO_","", regex=False)
                               .str.title())
fig_e = px.line(serie, x="ANO", y="Aprovação (%)", color="Etapa", markers=True)
st.plotly_chart(fig_e, use_container_width=True)

# ==========================================================
# Debug — tabela URGENTES usada
# ==========================================================
with st.expander("🔎 Ver tabela URGENTES (hardcoded)"):
    st.dataframe(urg_tab, use_container_width=True)
