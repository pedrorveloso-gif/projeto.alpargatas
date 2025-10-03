# gpt.py
# Painel Alpargatas focado em MUNICÍPIOS + Aprovação + Evasão (sem SAIDA_DIR)

import pandas as pd
import numpy as np
import unicodedata, re
import streamlit as st
import plotly.express as px
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet._reader")

# ============================
# 0) CAMINHOS (relativos ao repo)
# ============================
ARQ_ALP = "dados/Dados_alpa.xlsx"
ARQ_DTB = "dados/dtb_municipios.ods"
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_EM       = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"

# ============================
# 1) Funções utilitárias
# ============================
def nrm(txt):
    if pd.isna(txt): return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII","ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome):
    n = nrm(nome).replace("–","-").replace("—","-")
    if " - " in n: n = n.split(" - ")[0]
    return n.strip()

def to7(s):
    return s.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def _num(s):
    return pd.to_numeric(
        s.astype(str).str.replace("%","",regex=False).str.replace(",",".",regex=False),
        errors="coerce"
    )

# ============================
# 2) Carregamento Alpargatas (somente abas com CIDADES/UF)
# ============================
def carrega_alpargatas(path: str) -> pd.DataFrame:
    """Lê todas as abas (2020–2025), detecta header e extrai CIDADES/UF em um único DataFrame."""
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        raise RuntimeError("Nenhuma aba 2020–2025 encontrada no arquivo Alpargatas.")

    frames = []
    for aba in abas:
        # Lê as primeiras linhas sem header só para acharmos onde começa CIDADES/UF
        nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
        hdr   = acha_linha_header_cidades_uf(nohdr)
        if hdr is None:
            print(f"[AVISO] Não achei cabeçalho CIDADES/UF na aba '{aba}'. Pulando…")
            continue

        df = pd.read_excel(path, sheet_name=aba, header=hdr)

        # Descobre as colunas "Cidades" e "UF" em qualquer grafia
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm == "CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm == "UF"), None)
        if not c_cid or not c_uf:
            print(f"[AVISO] Colunas 'CIDADES'/'UF' não encontradas após header na aba '{aba}'.")
            continue

        tmp = (df[[c_cid, c_uf]].copy()
                 .rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"}))
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
        tmp["UF_SIGLA"]           = tmp["UF_SIGLA"].astype(str).str.strip()
        tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
        tmp = tmp[tmp["MUNICIPIO_NOME_ALP"].str.len() > 0]

        tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        tmp["FONTE_ABA"]       = aba
        frames.append(tmp)

    if not frames:
        raise RuntimeError("Nenhuma aba válida foi processada (CIDADES/UF não encontrado).")

    # remove duplicados entre abas (mesma cidade/UF pode aparecer em mais de uma aba)
    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])


# ============================
# 3) Carregar INEP (taxa de aprovação por município)
# ============================
def media_por_municipio(df, rotulo):
    return (
        pd.DataFrame({
            "CO_MUNICIPIO": to7(df["CO_MUNICIPIO"]),
            rotulo: pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce"),
        })
        .groupby("CO_MUNICIPIO", as_index=False)[rotulo]
        .mean()
    )

# ============================
# 4) Carregar Evasão (com várias séries)
# ============================
def carrega_evasao(path):
    df = pd.read_excel(path, header=8, engine="odf")
    mapa_colunas = {
        "1_CAT3_CATFUN": "EVASAO_FUNDAMENTAL",
        "1_CAT3_CATMED": "EVASAO_MEDIO",
    }
    df = df.rename(columns=mapa_colunas)
    df["CO_MUNICIPIO"] = to7(df["CO_MUNICIPIO"])
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]:
        if c in df.columns:
            df[c] = _num(df[c])
    return df

# ============================
# 5) Pipeline principal
# ============================
@st.cache_data(show_spinner=True)
def build_data():
    alpa = carrega_alpargatas(ARQ_ALP)

    # Aprovação (anos iniciais, finais, médio)
    df_ini = pd.read_excel(ARQ_INICIAIS, header=9)
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9)
    df_med = pd.read_excel(ARQ_EM,       header=9)

    ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base = alpa.copy()
    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                 .merge(fin, on="CO_MUNICIPIO", how="left")
                 .merge(med, on="CO_MUNICIPIO", how="left"))

    # Evasão
    eva = carrega_evasao(ARQ_EVASAO)
    base = base.merge(eva[["CO_MUNICIPIO","EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]],
                      on="CO_MUNICIPIO", how="left")

    # Reprovação
    base["Reprovacao_Iniciais"] = (1 - pd.to_numeric(base["TAXA_APROVACAO_INICIAIS"], errors="coerce"))*100
    base["Reprovacao_Finais"]   = (1 - pd.to_numeric(base["TAXA_APROVACAO_FINAIS"],  errors="coerce"))*100

    # Urgência
    base["Urgencia"] = base[["EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
                             "Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1, skipna=True)

    urgentes = base.sort_values("Urgencia", ascending=False).head(20)
    return base, urgentes

# ============================
# 6) UI
# ============================
st.set_page_config(page_title="IA • Aprovação/Evasão", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel (sem SAIDA_DIR)")

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
