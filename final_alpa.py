# final_alpa.py
# Streamlit app: Painel Municípios (sem Dados_alpa)
# - Lê somente dados/*.xlsx e dados/evasao.ods
# - Calcula aprovação (atual e histórica), reprovação, cruza com evasão e monta urgência
# - Exclui "CAMPINA GRANDE MIXING CENTER"
# - KPIs + gráficos

import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================
# Config da página (primeiro comando Streamlit)
# ============================
st.set_page_config(page_title="Instituto Alpargatas — Municípios", layout="wide")

# ============================
# Caminhos (fixos em dados/)
# ============================
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_MEDIO    = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"   # Pandas lê .ods com odfpy

# Exclusões explícitas de "sites" que não são municípios
EXCLUIR = ["CAMPINA GRANDE MIXING CENTER"]

# ============================
# Utilitários
# ============================
def nrm(txt: object) -> str:
    """Normaliza: remove acentos, vira CAIXA-ALTA e tira espaços. NaN -> ''."""
    if pd.isna(txt):
        return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    """
    Chave 'suave' para casar textos de município com grafias diferentes.
    - caixa alta
    - remove variações de hífen
    - corta sufixos poluentes (ex.: ' - ...', ' MIXING CENTER', etc.)
    """
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n:
        n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    return n

def _norm_col(s: object) -> str:
    return nrm(s).replace("  ", " ")

def _find_col(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    """Procura uma coluna por nomes-alvo normalizados."""
    alvo = {_norm_col(x) for x in candidatos}
    for orig in df.columns:
        if _norm_col(orig) in alvo:
            return orig
    return None

def _map_ano_cols(df: pd.DataFrame) -> dict[int, str]:
    """
    Mapeia {ano:int -> nome_col_original} para colunas do tipo:
    VL_INDICADOR_YYYY ou VL_INDICADOR_REND_YYYY (ignora underscore/caixa).
    """
    mapa = {}
    for c in df.columns:
        m = re.search(r"VL[_ ]?INDICADOR(?:_REND)?[_ ]?(\d{4})", _norm_col(c))
        if m:
            ano = int(m.group(1))
            mapa[ano] = c
    return mapa

def _coerce_cod_ibge(series: pd.Series) -> pd.Series:
    """Força código IBGE como string de 7 dígitos."""
    return series.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def _read_inep(path: str, headers_try=(9, 8, 7, 0)) -> pd.DataFrame:
    """
    Lê planilha INEP tentando alguns headers comuns (mais robusto).
    Para .xlsx o engine é auto; para .ods usar engine='odf'.
    """
    last_err = None
    for h in headers_try:
        try:
            return pd.read_excel(path, header=h)
        except Exception as e:
            last_err = e
    # Se nada deu certo, levanta último erro
    raise last_err if last_err else FileNotFoundError(path)

def medias_atual_e_hist(df: pd.DataFrame, rotulo_prefix: str) -> tuple[pd.DataFrame, int]:
    """
    Retorna:
      - DataFrame por CO_MUNICIPIO com:
        {rotulo}_P, {rotulo}_HIST_P (proporções 0-1) e as versões em %
      - ano_recente detectado
    """
    df = df.copy()
    col_cod = _find_col(df, ["CO MUNICIPIO", "CODIGO DO MUNICIPIO", "CODIGO MUNICIPIO", "CO_MUNICIPIO"])
    if not col_cod:
        raise KeyError("Coluna do código de município não encontrada (ex.: CO_MUNICIPIO).")

    df[col_cod] = _coerce_cod_ibge(df[col_cod])

    mapa = _map_ano_cols(df)
    if not mapa:
        raise KeyError("Nenhuma coluna VL_INDICADOR(_REND)_AAAA encontrada.")

    ano_recente = max(mapa.keys())
    col_atual = mapa[ano_recente]
    cols_hist = [mapa[a] for a in mapa if a != ano_recente]
    if not cols_hist:
        cols_hist = [col_atual]  # fallback (não deve acontecer em geral)

    usar_cols = [col_atual] + cols_hist
    num = df[[col_cod] + usar_cols].copy()
    for c in usar_cols:
        num[c] = pd.to_numeric(num[c], errors="coerce")

    # média por município (NaN são ignorados por padrão)
    grp = num.groupby(col_cod, as_index=False)[usar_cols].mean()

    out = grp[[col_cod]].copy()
    out[f"{rotulo_prefix}_P"] = grp[col_atual]
    out[f"{rotulo_prefix}_HIST_P"] = grp[cols_hist].mean(axis=1, skipna=True)

    out[f"{rotulo_prefix}_%"] = (out[f"{rotulo_prefix}_P"] * 100).round(2)
    out[f"{rotulo_prefix}_HIST_%"] = (out[f"{rotulo_prefix}_HIST_P"] * 100).round(2)
    out[f"{rotulo_prefix}_P"] = out[f"{rotulo_prefix}_P"].round(4)
    out[f"{rotulo_prefix}_HIST_P"] = out[f"{rotulo_prefix}_HIST_P"].round(4)

    return out.rename(columns={col_cod: "CO_MUNICIPIO"}), ano_recente

def long_por_municipio_ano(df: pd.DataFrame, etapa_rotulo: str) -> pd.DataFrame:
    """
    Constrói série longa: CO_MUNICIPIO, ANO, <etapa_rotulo> (proporção 0–1)
    """
    df = df.copy()
    col_cod = _find_col(df, ["CO MUNICIPIO", "CODIGO DO MUNICIPIO", "CODIGO MUNICIPIO", "CO_MUNICIPIO"])
    if not col_cod:
        raise KeyError("Coluna do código de município não encontrada (ex.: CO_MUNICIPIO).")

    df[col_cod] = _coerce_cod_ibge(df[col_cod])
    mapa = _map_ano_cols(df)
    if not mapa:
        raise KeyError("Nenhuma VL_INDICADOR(_REND)_AAAA encontrada (2005–2023).")

    cols = list(mapa.values())
    tmp = df[[col_cod] + cols].copy()
    for c in cols:
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce")

    long_df = tmp.melt(id_vars=col_cod, value_vars=cols, var_name="COL", value_name=etapa_rotulo)
    long_df["ANO"] = long_df["COL"].str.extract(r"(\d{4})").astype(int)
    long_df.drop(columns=["COL"], inplace=True)

    # média por município-ano
    long_grp = long_df.groupby([col_cod, "ANO"], as_index=False)[etapa_rotulo].mean()
    return long_grp.rename(columns={col_cod: "CO_MUNICIPIO"})

# ============================
# Evasão (abandono)
# ============================
def carrega_evasao(path: str) -> pd.DataFrame:
    """
    Lê evasão (.ods) com header=8 (formato usado) e retorna DF com:
    CO_MUNICIPIO, NO_MUNICIPIO, NO_UF, NO_LOCALIZACAO, NO_DEPENDENCIA,
    Evasao_Fundamental (1_CAT3_CATFUN), Evasao_Medio (1_CAT3_CATMED)
    """
    df = pd.read_excel(path, engine="odf", header=8)
    # tenta resolver nomes das colunas de forma robusta
    col_cod = _find_col(df, ["CO MUNICIPIO", "CO_MUNICIPIO"])
    col_nome = _find_col(df, ["NO MUNICIPIO", "NO_MUNICIPIO"])
    col_uf = _find_col(df, ["NO UF", "NO_UF", "UF"])
    col_loc = _find_col(df, ["NO LOCALIZACAO", "NO_LOCALIZACAO"])
    col_dep = _find_col(df, ["NO DEPENDENCIA", "NO_DEPENDENCIA"])

    # indicadores
    col_fun = _find_col(df, ["1_CAT3_CATFUN"])
    col_med = _find_col(df, ["1_CAT3_CATMED"])

    req = [col_cod, col_nome, col_uf, col_fun, col_med]
    if any(x is None for x in req):
        faltam = ["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","1_CAT3_CATFUN","1_CAT3_CATMED"]
        raise KeyError(f"Evasão: não achei colunas essenciais {faltam} no arquivo.")

    out = df[[col_cod, col_nome, col_uf, col_loc, col_dep, col_fun, col_med]].copy()
    out.columns = ["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","NO_LOCALIZACAO","NO_DEPENDENCIA",
                   "Evasao_Fundamental","Evasao_Medio"]

    out["CO_MUNICIPIO"] = _coerce_cod_ibge(out["CO_MUNICIPIO"])

    # trocar vírgula por ponto e para numérico
    for c in ["Evasao_Fundamental","Evasao_Medio"]:
        out[c] = (out[c].astype(str)
                        .str.replace(",", ".", regex=False)
                        .str.replace("%", "", regex=False))
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # normaliza nome e aplica exclusões por chave
    out["MUNICIPIO_CHAVE"] = out["NO_MUNICIPIO"].apply(chave_municipio)

    # excluir qualquer nome que contenha 'MIXING CENTER' (ou exatamente a string fornecida)
    excluir_norm = {chave_municipio(x) for x in EXCLUIR}
    out = out[~out["MUNICIPIO_CHAVE"].isin(excluir_norm)].copy()

    return out

# ============================
# Checagem de arquivos
# ============================
def _check_files():
    missing = [p for p in [ARQ_INICIAIS, ARQ_FINAIS, ARQ_MEDIO, ARQ_EVASAO] if not Path(p).exists()]
    if missing:
        st.error("Arquivos não encontrados:\n" + "\n".join(f"• {m}" for m in missing))
        st.stop()

# ============================
# Build data (cache)
# ============================
@st.cache_data(show_spinner=True)
def build_data():
    _check_files()

    # --- Ler INEP (aprovação)
    df_ini = _read_inep(ARQ_INICIAIS)
    df_fin = _read_inep(ARQ_FINAIS)
    df_med = _read_inep(ARQ_MEDIO)

    # --- Médias atuais e históricas (por município)
    ini, ano_ini = medias_atual_e_hist(df_ini, "APROVACAO_INICIAIS")
    fin, ano_fin = medias_atual_e_hist(df_fin, "APROVACAO_FINAIS")
    med, ano_med = medias_atual_e_hist(df_med, "APROVACAO_MEDIO")

    # --- Base com códigos (CO_MUNICIPIO), juntando as três etapas
    base = (ini.merge(fin, on="CO_MUNICIPIO", how="outer")
               .merge(med, on="CO_MUNICIPIO", how="outer"))

    # médias gerais atual e histórica
    base["APROVACAO_MEDIA_GERAL_P"] = base[
        ["APROVACAO_INICIAIS_P","APROVACAO_FINAIS_P","APROVACAO_MEDIO_P"]
    ].mean(axis=1, skipna=True)

    base["APROVACAO_MEDIA_HIST_P"] = base[
        ["APROVACAO_INICIAIS_HIST_P","APROVACAO_FINAIS_HIST_P","APROVACAO_MEDIO_HIST_P"]
    ].mean(axis=1, skipna=True)

    base["APROVACAO_MEDIA_GERAL_%"] = (base["APROVACAO_MEDIA_GERAL_P"]*100).round(2)
    base["APROVACAO_MEDIA_HIST_%"]  = (base["APROVACAO_MEDIA_HIST_P"]*100).round(2)

    # --- Evasão
    ev = carrega_evasao(ARQ_EVASAO)

    # Nome/UF/localização/dependência via evasão (quando disponível)
    meta_cols = ["NO_MUNICIPIO","NO_UF","NO_LOCALIZACAO","NO_DEPENDENCIA","MUNICIPIO_CHAVE"]
    base = base.merge(ev[["CO_MUNICIPIO"] + meta_cols], on="CO_MUNICIPIO", how="left")

    # --- Juntar evasão
    base = base.merge(ev[["CO_MUNICIPIO","Evasao_Fundamental","Evasao_Medio"]],
                      on="CO_MUNICIPIO", how="left")

    # --- Reprovação (em %) a partir da aprovação atual (em %)
    base["APROVACAO_INICIAIS_%"] = (base["APROVACAO_INICIAIS_P"] * 100).round(2)
    base["APROVACAO_FINAIS_%"]   = (base["APROVACAO_FINAIS_P"]   * 100).round(2)
    base["APROVACAO_MEDIO_%"]    = (base["APROVACAO_MEDIO_P"]    * 100).round(2)

    base["Reprovacao_Iniciais"] = (100 - base["APROVACAO_INICIAIS_%"]).clip(lower=0)
    base["Reprovacao_Finais"]   = (100 - base["APROVACAO_FINAIS_%"]).clip(lower=0)
    base["Reprovacao_Medio"]    = (100 - base["APROVACAO_MEDIO_%"]).clip(lower=0)

    # --- Urgência (soma simples — ajuste à vontade)
    base["Urgencia"] = (
        base[["Evasao_Fundamental","Evasao_Medio",
              "Reprovacao_Iniciais","Reprovacao_Finais"]]
        .sum(axis=1, skipna=True)
    )

    # --- Série temporal (média geral por município e ano)
    evo_ini = long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = long_por_municipio_ano(df_med, "APROVACAO_MEDIO")

    evolucao = (evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                      .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
        ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]:
        evolucao[c + "_%"] = (evolucao[c]*100).round(2)

    # junta nomes/UF para facilitar filtros/legendas
    evolucao = evolucao.merge(
        base[["CO_MUNICIPIO","NO_MUNICIPIO","NO_UF","MUNICIPIO_CHAVE"]].drop_duplicates("CO_MUNICIPIO"),
        on="CO_MUNICIPIO", how="left"
    )

    # Exclusão extra por chave (caso meta venha de outra fonte no futuro)
    excluir_norm = {chave_municipio(x) for x in EXCLUIR}
    base = base[~base["MUNICIPIO_CHAVE"].isin(excluir_norm)].copy()
    evolucao = evolucao[~evolucao["MUNICIPIO_CHAVE"].isin(excluir_norm)].copy()

    meta = {
        "ano_recente": int(max(ano_ini, ano_fin, ano_med)),
        "n_munis": int(base["CO_MUNICIPIO"].nunique()),
    }
    return base, evolucao, meta

# ============================
# UI
# ============================
st.title("📊 Instituto Alpargatas — Painel Municípios (sem Dados_alpa)")

# Carrega dados
with st.spinner("Processando dados…"):
    base, evolucao, meta = build_data()

# Filtros
ufs = sorted([u for u in base["NO_UF"].dropna().unique().tolist()])
col_f1, col_f2 = st.columns([1,2])
with col_f1:
    sel_ufs = st.multiselect("UF", options=ufs, default=ufs)
with col_f2:
    base_uf = base[base["NO_UF"].isin(sel_ufs)] if sel_ufs else base.copy()
    munis_opts = sorted([m for m in base_uf["NO_MUNICIPIO"].dropna().unique().tolist()])
    sel_munis = st.multiselect("Municípios", options=munis_opts, default=munis_opts)

# aplica filtros
base_f = base.copy()
if sel_ufs:
    base_f = base_f[base_f["NO_UF"].isin(sel_ufs)]
if sel_munis:
    base_f = base_f[base_f["NO_MUNICIPIO"].isin(sel_munis)]

# KPIs
c1,c2,c3,c4 = st.columns(4)
with c1:
    st.metric("Municípios", f"{base_f['CO_MUNICIPIO'].nunique():,}".replace(",", "."))
with c2:
    st.metric("Aprovação média — atual (%)",
              f"{base_f['APROVACAO_MEDIA_GERAL_%'].mean():.2f}" if not base_f.empty else "—")
with c3:
    st.metric("Aprovação média — histórica (%)",
              f"{base_f['APROVACAO_MEDIA_HIST_%'].mean():.2f}" if not base_f.empty else "—")
with c4:
    st.metric("Urgência média",
              f"{base_f['Urgencia'].mean():.2f}" if not base_f.empty else "—")

st.divider()

# Tabela essencial
cols_show = [
    "NO_UF","NO_MUNICIPIO",
    "APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%",
    "APROVACAO_INICIAIS_HIST_%","APROVACAO_FINAIS_HIST_%","APROVACAO_MEDIO_HIST_%",
    "APROVACAO_MEDIA_GERAL_%","APROVACAO_MEDIA_HIST_%",
    "Evasao_Fundamental","Evasao_Medio",
    "Reprovacao_Iniciais","Reprovacao_Finais","Reprovacao_Medio",
    "Urgencia"
]
cols_show = [c for c in cols_show if c in base_f.columns]
st.subheader("Tabela (seleção atual)")
st.dataframe(
    base_f[cols_show].sort_values(["NO_UF","NO_MUNICIPIO"]).reset_index(drop=True),
    use_container_width=True
)

st.divider()

# Gráfico: comparativo Atual × Histórico por etapa
st.subheader("Comparativo Atual × Histórico")
etapa = st.radio("Etapa", ["Iniciais","Finais","Médio"], horizontal=True)
map_cols_2023 = {
    "Iniciais": "APROVACAO_INICIAIS_%",
    "Finais":   "APROVACAO_FINAIS_%",
    "Médio":    "APROVACAO_MEDIO_%"
}
map_cols_hist = {
    "Iniciais": "APROVACAO_INICIAIS_HIST_%",
    "Finais":   "APROVACAO_FINAIS_HIST_%",
    "Médio":    "APROVACAO_MEDIO_HIST_%"
}
c_atual = map_cols_2023[etapa]
c_hist  = map_cols_hist[etapa]

viz = (base_f[["NO_MUNICIPIO", c_atual, c_hist]]
       .dropna(subset=[c_atual, c_hist])
       .melt(id_vars="NO_MUNICIPIO", var_name="Tipo", value_name="Valor")
       .replace({c_atual: f"{etapa} — Atual", c_hist: f"{etapa} — Histórico"}))

st.plotly_chart(
    px.bar(viz, x="NO_MUNICIPIO", y="Valor", color="Tipo", barmode="group",
           title=f"{etapa}: Atual × Histórico (%)")
      .update_layout(xaxis_title="", yaxis_title="%", legend_title=""),
    use_container_width=True
)

st.divider()

# Gráfico: série temporal (média geral ao longo dos anos)
st.subheader("Série temporal — aprovação média geral (%)")
evo_f = evolucao.copy()
if sel_ufs:
    evo_f = evo_f[evo_f["NO_UF"].isin(sel_ufs)]
if sel_munis:
    evo_f = evo_f[evo_f["NO_MUNICIPIO"].isin(sel_munis)]

# <- sem numeric_only=True (para evitar TypeError em SeriesGroupBy)
serie = evo_f.groupby("ANO", as_index=False)[["APROVACAO_MEDIA_GERAL_%"]].mean()
st.plotly_chart(
    px.line(serie, x="ANO", y="APROVACAO_MEDIA_GERAL_%", markers=True,
            title="Aprovação média geral (%) — municípios selecionados")
      .update_layout(xaxis_title="Ano", yaxis_title="%"),
    use_container_width=True
)

st.divider()

# Top urgência
st.subheader("Top urgência")
topn = st.slider("Quantos municípios exibir", min_value=5, max_value=30, value=15, step=1)
rank_urg = (base_f[["NO_UF","NO_MUNICIPIO","Urgencia"]]
            .dropna(subset=["Urgencia"])
            .sort_values("Urgencia", ascending=False)
            .head(topn))
st.plotly_chart(
    px.bar(rank_urg, x="NO_MUNICIPIO", y="Urgencia", color="NO_UF",
           title=f"Top {len(rank_urg)} — urgência (maior = pior)")
      .update_layout(xaxis_title="", yaxis_title="Índice"),
    use_container_width=True
)

st.caption("Obs.: Urgência = Evasão(Fund + Médio) + Reprovação(Iniciais + Finais). "
           "Aprovações 'Atual' usam o ano mais recente detectado nos arquivos; "
           "Histórico = média de todos os anos disponíveis, exceto o mais recente.")
