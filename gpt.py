# app_min_streamlit.py
# -------------------------------------------------------------
# App enxuto para Streamlit Cloud lendo arquivos em ./dados/
# Mantém: KPIs, gráficos de tendência, GAP, quadrantes, tabela e download.
# Use caminhos RELATIVOS e priorize CSV/Parquet (sem dependência de ODS).
# -------------------------------------------------------------

from __future__ import annotations
import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import re, unicodedata
import plotly.express as px

# =========================
# Config
# =========================
st.set_page_config(page_title="IA • Aprovação, Evasão e Urgência (enxuto)", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel (versão enxuta)")
DATA_DIR = Path("dados")  # coloque seus arquivos dentro desta pasta do repositório

# =========================
# Utilitários curtos
# =========================
def _slug(s: object) -> str:
    if pd.isna(s): return ""
    t = unicodedata.normalize("NFKD", str(s)).encode("ASCII","ignore").decode("ASCII")
    t = t.replace("–","-").replace("—","-").strip().lower()
    t = re.sub(r"[^a-z0-9]+","_", t)
    return re.sub(r"_+","_", t).strip("_")

def _to_num(x: pd.Series) -> pd.Series:
    return pd.to_numeric(
        x.astype(str)
         .str.replace("%","",regex=False)
         .str.replace(".","",regex=False)  # remove separador de milhar
         .str.replace(",",".",regex=False), # vírgula decimal -> ponto
        errors="coerce"
    )

def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.dropna().empty or s.max() == s.min():
        return pd.Series(0.5, index=s.index)
    return (s - s.min())/(s.max()-s.min())

# =========================
# Leitura flexível (CSV/Parquet/XLSX)
# =========================
@st.cache_data(show_spinner=False)
def load_any(patterns: list[str]) -> pd.DataFrame:
    """Tenta ler o primeiro arquivo que existir na pasta dados/ seguindo a lista de padrões.
    Prioriza CSV > Parquet > XLSX. Retorna DF vazio se nada encontrado."""
    exts_pref = [".csv", ".parquet", ".xlsx", ".ods"]
    # gera lista ordenada por preferência de extensão
    candidates: list[Path] = []
    for pat in patterns:
        for ext in exts_pref:
            candidates += sorted(DATA_DIR.glob(f"{pat}{ext}"))
    for fp in candidates:
        try:
            if fp.suffix == ".csv":
                return pd.read_csv(fp)
            elif fp.suffix == ".parquet":
                return pd.read_parquet(fp)
            elif fp.suffix == ".xlsx":
                return pd.read_excel(fp)
            else:  # .ods
                return pd.read_excel(fp, engine="odf")
        except Exception as e:
            st.warning(f"Falha ao ler {fp.name}: {e}")
    return pd.DataFrame()

# =========================
# Dados esperados
# =========================
# Se você já tem as bases prontas, basta colocar em dados/ como:
#   - evolucao_filtrada.(csv|parquet|xlsx)  -> colunas: MUNICIPIO_NOME, ANO, APROVACAO_INICIAIS/FINAIS/MEDIO (0–1 ou %)
#   - urgentes.(csv|parquet|xlsx)          -> colunas com Evasão (ex: "Evasão - Fundamental" ou "EVASAO_FUNDAMENTAL")
# Caso não tenha evolucao_filtrada pronta, você pode fornecer 3 arquivos crus (anos iniciais/finais/médio)
#   - inep_iniciais.*, inep_finais.*, inep_medio.* com colunas CO_MUNICIPIO e VL_INDICADOR_REND_YYYY

# 1) Tenta carregar evolucao_filtrada pronta
_evo = load_any(["evolucao_filtrada"])  # recomendado  # recomendado

# 2) Caso não exista, constrói a partir de inep_iniciais/finais/medio
if _evo.empty:
    def _anos_disponiveis(df: pd.DataFrame, ano_min=2005, ano_max=2023) -> list[int]:
        anos = []
        for c in df.columns:
            m = re.fullmatch(r"VL_INDICADOR_REND_(\\d{4})", str(c))
            if m:
                a = int(m.group(1))
                if ano_min <= a <= ano_max: anos.append(a)
        return sorted(set(anos))

    def _long_por_municipio_ano(df: pd.DataFrame, etapa_rotulo: str) -> pd.DataFrame:
        if df.empty: return pd.DataFrame(columns=["CO_MUNICIPIO","ANO",etapa_rotulo])
        t = df.copy()
        t["CO_MUNICIPIO"] = (
            t["CO_MUNICIPIO"].astype(str).str.extract(r"(\\d{7})", expand=False).str.zfill(7)
        )
        anos = _anos_disponiveis(t)
        cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
        for c in cols: t[c] = pd.to_numeric(t[c], errors="coerce")
        long_df = t.melt(id_vars="CO_MUNICIPIO", value_vars=cols, var_name="COL", value_name=etapa_rotulo)
        long_df["ANO"] = long_df["COL"].str.extract(r"(\\d{4})").astype(int)
        long_df.drop(columns=["COL"], inplace=True)
        return long_df.groupby(["CO_MUNICIPIO","ANO"], as_index=False)[etapa_rotulo].mean()

    inis = load_any(["inep_iniciais", "anos_iniciais"])  # seus arquivos: anos_iniciais.xlsx  # aceite nomes alternativos
    fins = load_any(["inep_finais", "anos_finais"])   # seus arquivos: anos_finais.xlsx
    medio = load_any(["inep_medio", "ensino_medio"])   # seu arquivo: ensino_medio.xlsx 

    evo_ini = _long_por_municipio_ano(inis, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(fins, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(medio, "APROVACAO_MEDIO")

    _evo = (evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                  .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))

    # se houver nomes oficiais em um dicionário externo, anexe (opcional)
    dic = load_any(["dtb_lookup", "dic_municipios", "dtb"])  # você tem dtb.csv  # opcional: CO_MUNICIPIO, MUNICIPIO_NOME, UF_SIGLA
    if not dic.empty:
        dic = dic.copy()
        dic["CO_MUNICIPIO"] = dic["CO_MUNICIPIO"].astype(str).str.extract(r"(\\d{7})", expand=False).str.zfill(7)
        _evo = _evo.merge(dic[["CO_MUNICIPIO","MUNICIPIO_NOME","UF_SIGLA"]], on="CO_MUNICIPIO", how="left")

# 3) Carrega urgentes (com evasão)
_urg = load_any(["urgentes", "winsor_df", "tabela_essencial", "evasao"])  # você tem evasao.ods  # aceita várias origens

# =========================
# Padronizações rápidas
# =========================
# Nome do município
if "MUNICIPIO_NOME" not in _evo.columns:
    for c in ["NO_MUNICIPIO","MUNICIPIO_NOME_ALP"]:
        if c in _evo.columns:
            _evo = _evo.rename(columns={c:"MUNICIPIO_NOME"})
            break
_evo["MUNICIPIO_NOME"] = _evo.get("MUNICIPIO_NOME", pd.Series(index=_evo.index)).astype(str).str.strip()

# Garantir colunas em % para iniciais/finais
for base in ["APROVACAO_INICIAIS","APROVACAO_FINAIS"]:
    if base + "_%" not in _evo.columns:
        if base in _evo.columns:
            m = pd.to_numeric(_evo[base], errors="coerce").mean()
            _evo[base + "_%"] = (100*pd.to_numeric(_evo[base], errors="coerce") if pd.notna(m) and m<=1.5
                                   else pd.to_numeric(_evo[base], errors="coerce"))
        else:
            _evo[base + "_%"] = np.nan
    else:
        _evo[base + "_%"] = _to_num(_evo[base + "_%"]) 

# Evasão no DF urgentes
if not _urg.empty:
    u = _urg.copy()
    nome_col = next((c for c in ["MUNICIPIO_NOME","MUNICIPIO_NOME_ALP","NO_MUNICIPIO"] if c in u.columns), None)
    u = u.rename(columns={nome_col:"MUNICIPIO_NOME"}) if nome_col else pd.DataFrame(columns=["MUNICIPIO_NOME"])
    u["MUNICIPIO_NOME"] = u["MUNICIPIO_NOME"].astype(str).str.strip()
    evas_map = ["EVASAO_FUNDAMENTAL", "Evasão - Fundamental", "Fundamental - Total"]
    col_evas = next((c for c in evas_map if c in u.columns), None)
    if not col_evas:
        # procura algo que contenha 'evas' e 'fund'
        for c in u.columns:
            if "evas" in _slug(c) and ("fund" in _slug(c) or "fundamental" in _slug(c)):
                col_evas = c; break
    u["EVASAO_FUNDAMENTAL"] = _to_num(u[col_evas]) if col_evas else np.nan
    _urg = u.groupby("MUNICIPIO_NOME", as_index=False)["EVASAO_FUNDAMENTAL"].mean(numeric_only=True)

# =========================
# Dataset estático por município
# =========================
@st.cache_data(show_spinner=False)
def build_static(evo: pd.DataFrame, urg: pd.DataFrame) -> pd.DataFrame:
    if evo.empty: return pd.DataFrame()
    base = (evo.groupby("MUNICIPIO_NOME", as_index=False)
              [["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
              .mean(numeric_only=True))
    # anexar evasão
    if not urg.empty:
        base = base.merge(urg, on="MUNICIPIO_NOME", how="left")
    # derivados
    base["GAP_APROV_%"] = base["APROVACAO_INICIAIS_%"] - base["APROVACAO_FINAIS_%"]
    base["SCORE_RISCO"] = 0.5*(1 - _minmax(base["APROVACAO_FINAIS_%"])) + \
                           0.4* _minmax(base["EVASAO_FUNDAMENTAL"])       + \
                           0.1* _minmax(base["GAP_APROV_%"].fillna(0))
    return base

static_df = build_static(_evo, _urg)

# =========================
# KPIs
# =========================
c1,c2,c3,c4 = st.columns(4)
with c1:
    st.metric("Municípios no recorte", len(static_df["MUNICIPIO_NOME"].unique()) if not static_df.empty else "–")
with c2:
    st.metric("Aprovação — Finais (média)", f"{static_df['APROVACAO_FINAIS_%'].mean():.1f}%" if not static_df.empty else "–")
with c3:
    st.metric("Evasão — Fundamental (média)", f"{static_df['EVASAO_FUNDAMENTAL'].mean():.1f}%" if (not static_df.empty and 'EVASAO_FUNDAMENTAL' in static_df.columns) else "–")
with c4:
    st.metric("Score de risco (média)", f"{static_df['SCORE_RISCO'].mean():.2f}" if (not static_df.empty and 'SCORE_RISCO' in static_df.columns) else "–")

st.caption("Arquivos esperados em ./dados/: evolucao_filtrada.*, urgentes.* (ou inep_iniciais/finais/medio).* ")
st.divider()

# =========================
# Gráficos
# =========================
colA, colB = st.columns(2)
with colA:
    st.subheader("Tendência Geral — Iniciais vs Finais")
    if not _evo.empty and {"ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"}.issubset(_evo.columns):
        m = (_evo.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
                  .mean(numeric_only=True))
        fig = px.line(m.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
                      x="ANO", y="Aprovação (%)", color="Etapa", markers=True)
        fig.update_layout(yaxis_tickformat=".1f")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Para este gráfico, forneça ANO, APROVACAO_INICIAIS_% e APROVACAO_FINAIS_% em evolucao_filtrada.*")

with colB:
    st.subheader("GAP de Aprovação (Iniciais − Finais)")
    if not static_df.empty:
        t = static_df.dropna(subset=["GAP_APROV_%"]).sort_values("GAP_APROV_%", ascending=False).head(25)
        fig = px.bar(t, x="GAP_APROV_%", y="MUNICIPIO_NOME", orientation="h",
                     labels={"MUNICIPIO_NOME":"Município","GAP_APROV_%":"GAP (p.p.)"})
        fig.update_yaxes(categoryorder="total ascending")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("static_df vazio")

st.subheader("Quadrantes — Aprovação (Finais) × Evasão (Fundamental)")
if (not static_df.empty) and ("EVASAO_FUNDAMENTAL" in static_df.columns):
    t = static_df.dropna(subset=["APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]).copy()
    cut_x = t["APROVACAO_FINAIS_%"].median(); cut_y = t["EVASAO_FUNDAMENTAL"].median()
    conds = [
        (t["APROVACAO_FINAIS_%"] <  cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] <  cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
    ]
    labels = ["Crítico", "Atenção", "Apoio pedagógico", "OK"]
    t["Quadrante"] = np.select(conds, labels)
    figq = px.scatter(t, x="APROVACAO_FINAIS_%", y="EVASAO_FUNDAMENTAL", color="Quadrante",
                      size=("SCORE_RISCO" if "SCORE_RISCO" in t.columns else None), size_max=26,
                      hover_data=["MUNICIPIO_NOME","APROVACAO_INICIAIS_%","GAP_APROV_%","SCORE_RISCO"],
                      labels={"APROVACAO_FINAIS_%":"Aprovação Finais (%)","EVASAO_FUNDAMENTAL":"Evasão Fund. (%)"})
    figq.add_vline(x=cut_x); figq.add_hline(y=cut_y)
    st.plotly_chart(figq, use_container_width=True)
else:
    st.info("Para os quadrantes, o arquivo de urgentes precisa ter a coluna de Evasão do Fundamental.")

st.divider()

# =========================
# Tabelas e Download
# =========================
st.subheader("Tabelas")
col1, col2 = st.columns(2)
with col1:
    st.markdown("**Métricas por município (static_df)**")
    if not static_df.empty:
        st.dataframe(static_df.sort_values("SCORE_RISCO", ascending=False), use_container_width=True)
        st.download_button("Baixar static_df.csv", static_df.to_csv(index=False).encode("utf-8"), file_name="static_df.csv")
    else:
        st.info("static_df vazio")
with col2:
    if not _evo.empty:
        prefer = ["MUNICIPIO_NOME","ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO"]
        cols = [c for c in prefer if c in _evo.columns]
        st.markdown("**evolucao_filtrada (visão)**")
        st.dataframe(_evo[cols] if cols else _evo.head(50), use_container_width=True)
    if not _urg.empty:
        st.markdown("**urgentes (visão)**"); st.dataframe(_urg, use_container_width=True)

# =========================
# Diagnóstico rápido
# =========================
st.divider(); st.subheader("Diagnóstico")
def _diag(df, nome):
    if isinstance(df, pd.DataFrame) and not df.empty:
        st.success(f"{nome} OK — shape {df.shape}")
        st.caption(df.dtypes.astype(str))
    else:
        st.error(f"{nome} ausente ou vazio.")

_diag(static_df, "static_df")
_diag(_evo, "evolucao_filtrada / montada")
_diag(_urg, "urgentes")
