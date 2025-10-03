# gpt.py
# Painel Instituto Alpargatas — Municípios + Aprovação + Evasão + Urgência
# Inspirado no estilo modular com docstrings e funções curtas

import pandas as pd
import numpy as np
import unicodedata, re
import streamlit as st
import plotly.express as px
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet._reader")

# =========================================================
# 0) CAMINHOS RELATIVOS AO REPOSITÓRIO
# =========================================================
ARQ_ALP      = "dados/Dados_alpa.xlsx"
ARQ_DTB      = "dados/dtb_municipios.ods"
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_EM       = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"

# =========================================================
# 1) UTILITÁRIOS CURTOS
# =========================================================
def nrm(txt: object) -> str:
    """Normaliza: remove acentos, vira CAIXA-ALTA e tira espaços. NaN -> ''."""
    if pd.isna(txt):
        return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    """
    Chave 'suave' para casar municípios.
    Remove sufixos irrelevantes e corta após ' - '.
    """
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n:
        n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    return n

def acha_linha_header_cidades_uf(df_no_header: pd.DataFrame) -> int | None:
    """Detecta a linha onde aparecem CIDADES e UF no arquivo Alpargatas."""
    for i, row in df_no_header.iterrows():
        vals = [nrm(x) for x in row.tolist()]
        if "CIDADES" in vals and "UF" in vals:
            return i
    return None

def to7(s: pd.Series) -> pd.Series:
    """Força códigos a 7 dígitos (string)."""
    return s.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def _num(s: pd.Series) -> pd.Series:
    """Transforma em número, limpando % e vírgula."""
    return pd.to_numeric(
        s.astype(str).str.replace("%", "", regex=False).str.replace(",", ".", regex=False),
        errors="coerce"
    )

# =========================================================
# 2) DTB / IBGE
# =========================================================
def carrega_dtb(path: str) -> pd.DataFrame:
    """Lê DTB/IBGE e devolve DataFrame limpo com UF, código, nome e chave de município."""
    UF_SIGLAS = {
        "ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA",
        "CEARÁ":"CE","DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOIÁS":"GO",
        "MARANHÃO":"MA","MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG",
        "PARÁ":"PA","PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI",
        "RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS",
        "RONDÔNIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SÃO PAULO":"SP",
        "SERGIPE":"SE","TOCANTINS":"TO"
    }

    raw = pd.read_excel(path, engine="odf", skiprows=6)

    dtb = (raw.rename(columns={
                "UF": "UF_COD_NUM",
                "Nome_UF": "UF_NOME",
                "Código Município Completo": "MUNICIPIO_CODIGO",
                "Nome_Município": "MUNICIPIO_NOME"
            })[["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
           .dropna())

    dtb["UF_SIGLA"]           = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"]   = dtb["MUNICIPIO_CODIGO"].astype(str).str.zfill(7)
    dtb["MUNICIPIO_NOME"]     = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]    = dtb["MUNICIPIO_NOME"].apply(chave_municipio)

    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

# =========================================================
# 3) ALPARGATAS (CIDADES/UF)
# =========================================================
def carrega_alpargatas(path: str) -> pd.DataFrame:
    """Extrai CIDADES e UF das abas 2020–2025 do Excel Alpargatas."""
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        raise RuntimeError("Nenhuma aba 2020–2025 encontrada.")

    frames = []
    for aba in abas:
        nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
        hdr   = acha_linha_header_cidades_uf(nohdr)
        if hdr is None:
            st.warning(f"Aba '{aba}' sem header CIDADES/UF. Pulando…")
            continue

        df = pd.read_excel(path, sheet_name=aba, header=hdr)
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm=="CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm=="UF"), None)
        if not c_cid or not c_uf:
            st.warning(f"Aba '{aba}': não achei colunas CIDADES/UF.")
            continue

        tmp = df[[c_cid,c_uf]].dropna()
        tmp = tmp.rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"})
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
        tmp["UF_SIGLA"]           = tmp["UF_SIGLA"].astype(str).str.strip()
        tmp["MUNICIPIO_CHAVE"]    = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        tmp["FONTE_ABA"]          = aba
        frames.append(tmp)

    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])

# =========================================================
# 4) INEP — Aprovação
# =========================================================
def media_por_municipio(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    """Média da coluna VL_INDICADOR_REND_2023 por município."""
    return (
        pd.DataFrame({
            "CO_MUNICIPIO": to7(df["CO_MUNICIPIO"]),
            rotulo: pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce"),
        })
        .groupby("CO_MUNICIPIO", as_index=False)[rotulo]
        .mean()
    )

# =========================================================
# 5) EVASÃO — INEP
# =========================================================
def carrega_evasao(path: str) -> pd.DataFrame:
    """Carrega evasão (fundamental + médio)."""
    df = pd.read_excel(path, header=8, engine="odf")
    df["CO_MUNICIPIO"] = to7(df["CO_MUNICIPIO"])
    df["EVASAO_FUNDAMENTAL"] = _num(df.get("1_CAT3_CATFUN", pd.Series()))
    df["EVASAO_MEDIO"]       = _num(df.get("1_CAT3_CATMED", pd.Series()))
    return df[["CO_MUNICIPIO","EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]]

# =========================================================
# 6) PIPELINE GERAL
# =========================================================
@st.cache_data(show_spinner=True)
def build_data():
    alpa = carrega_alpargatas(ARQ_ALP)
    dtb  = carrega_dtb(ARQ_DTB)

    # Juntar Alpargatas + DTB
    base = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left")

    # Aprovação
    df_ini = pd.read_excel(ARQ_INICIAIS, header=9)
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9)
    df_med = pd.read_excel(ARQ_EM,       header=9)

    ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))

    # Evasão
    eva = carrega_evasao(ARQ_EVASAO)
    base = base.merge(eva, on="CO_MUNICIPIO", how="left")

    # Reprovação
    base["Reprovacao_Iniciais"] = (1 - pd.to_numeric(base["TAXA_APROVACAO_INICIAIS"], errors="coerce"))*100
    base["Reprovacao_Finais"]   = (1 - pd.to_numeric(base["TAXA_APROVACAO_FINAIS"],  errors="coerce"))*100

    # Urgência
    base["Urgencia"] = base[["EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
                             "Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1, skipna=True)

    urgentes = base.sort_values("Urgencia", ascending=False).head(20)
    return base, urgentes

# =========================================================
# 7) UI — STREAMLIT
# =========================================================
st.set_page_config(page_title="IA • Aprovação/Evasão", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios")

with st.spinner("Processando dados…"):
    base, urgentes = build_data()

c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios (base)", f"{base['MUNICIPIO_CHAVE'].nunique()}")
with c2: st.metric("Aprovação finais (média)", f"{(pd.to_numeric(base['TAXA_APROVACAO_FINAIS'], errors='coerce').mean()*100):.1f}%")
with c3: st.metric("Evasão fundamental (média)", f"{base['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4: st.metric("Urgência média", f"{base['Urgencia'].mean():.1f}")

st.subheader("Top 20 municípios urgentes")
st.dataframe(urgentes[[
    "MUNICIPIO_NOME_ALP","UF_SIGLA","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
    "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"
]])
