# ===============================================
# PAINEL INSTITUTO ALPARGATAS — EDUCAÇÃO (2020–2025)
# ===============================================

import pandas as pd
import numpy as np
import unicodedata, re
from pathlib import Path
import matplotlib.pyplot as plt
import plotly.express as px
import streamlit as st

# ============================
# 0) AJUSTE OS CAMINHOS AQUI
# ============================
ARQ_ALP = "dados/Projetos_de_Atuac807a771o_-_IA_-_2020_a_2025 (1).xlsx"
ARQ_DTB = "dados/RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.ods"

ods_iniciais = "dados/divulgacao_anos_iniciais_municipios_2023.xlsx"
ods_finais   = "dados/divulgacao_anos_finais_municipios_2023.xlsx"
ods_em       = "dados/divulgacao_ensino_medio_municipios_2023.xlsx"
caminho_evasao = "dados/TX_TRANSICAO_MUNICIPIOS_2021_2022.ods"


# =========================================================
# 1) Utilitários
# =========================================================
def nrm(txt: object) -> str:
    if pd.isna(txt): return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n: n = n.split(" - ")[0]
    for suf in (" MIXING CENTER"," DISTRITO"," DISTRITO INDUSTRIAL"):
        if n.endswith(suf): n = n[:-len(suf)].strip()
    return n

# =========================================================
# 2) Carregar DTB/IBGE
# =========================================================
def carrega_dtb(path: str) -> pd.DataFrame:
    UF_SIGLAS = {"ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA",
        "CEARÁ":"CE","DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOIÁS":"GO",
        "MARANHÃO":"MA","MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG",
        "PARÁ":"PA","PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI",
        "RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS",
        "RONDÔNIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SÃO PAULO":"SP",
        "SERGIPE":"SE","TOCANTINS":"TO"}
    raw = pd.read_excel(path, engine="odf", skiprows=6)
    dtb = (raw.rename(columns={
        "UF":"UF_COD_NUM","Nome_UF":"UF_NOME","Código Município Completo":"MUNICIPIO_CODIGO",
        "Nome_Município":"MUNICIPIO_NOME"})
        [["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
        .dropna())
    dtb["UF_SIGLA"] = dtb["UF_NOME"].str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"] = dtb["MUNICIPIO_CODIGO"].astype(str).str.zfill(7)
    dtb["MUNICIPIO_NOME"]   = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]  = dtb["MUNICIPIO_NOME"].apply(chave_municipio)
    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

# =========================================================
# 3) Carregar Alpargatas
# =========================================================
def carrega_alpargatas(path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(y) in a for y in range(2020,2026))]
    frames = []
    for aba in abas:
        df = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
        hdr = df.applymap(nrm).apply(lambda r: "CIDADES" in r.values and "UF" in r.values, axis=1).idxmax()
        df = pd.read_excel(path, sheet_name=aba, header=hdr)
        c_cid = next((c for c in df.columns if nrm(c)=="CIDADES"), None)
        c_uf  = next((c for c in df.columns if nrm(c)=="UF"), None)
        if not c_cid or not c_uf: continue
        tmp = df[[c_cid,c_uf]].rename(columns={c_cid:"MUNICIPIO_NOME_ALP",c_uf:"UF_SIGLA"}).dropna()
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].str.upper().str.strip()
        tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        frames.append(tmp)
    return pd.concat(frames).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])

# =========================================================
# 4) Cruzar bases
# =========================================================
def cruzar(dtb, alpa):
    cod = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left")
    nao = cod[cod["MUNICIPIO_CODIGO"].isna()][["MUNICIPIO_NOME_ALP","UF_SIGLA"]].drop_duplicates()
    return cod, nao

# =========================================================
# 5) Execução principal
# =========================================================
dtb = carrega_dtb(ARQ_DTB)
alpa = carrega_alpargatas(ARQ_ALP)
codificados, nao_encontrados = cruzar(dtb, alpa)

# ajuste Campina Grande
mask = (codificados["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE")) & (codificados["UF_SIGLA"]=="PB")
codificados.loc[mask,"MUNICIPIO_CODIGO"] = "2504009"

# =========================================================
# 6) Aprovação por etapas
# =========================================================
def media_por_municipio(df, rotulo):
    df = df.copy()
    df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})").str.zfill(7)
    ind = pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce")
    return df.groupby("CO_MUNICIPIO",as_index=False).agg({ "CO_MUNICIPIO":"first", "VL_INDICADOR_REND_2023":"mean" }).rename(columns={"VL_INDICADOR_REND_2023":rotulo})

df_iniciais = pd.read_excel(ods_iniciais, header=9)
df_finais   = pd.read_excel(ods_finais, header=9)
df_em       = pd.read_excel(ods_em, header=9)

ini = media_por_municipio(df_iniciais,"TAXA_APROVACAO_INICIAIS")
fin = media_por_municipio(df_finais,  "TAXA_APROVACAO_FINAIS")
med = media_por_municipio(df_em,      "TAXA_APROVACAO_MEDIO")

res = codificados.merge(ini,on="CO_MUNICIPIO",how="left").merge(fin,on="CO_MUNICIPIO",how="left").merge(med,on="CO_MUNICIPIO",how="left")

# =========================================================
# 7) Evasão
# =========================================================
df_evasao = pd.read_excel(caminho_evasao, header=8)
df_evasao = df_evasao.rename(columns={
    "1_CAT3_CATFUN":"Evasao_Fundamental","1_CAT3_CATMED":"Evasao_Medio"})
df_evasao["CO_MUNICIPIO"] = df_evasao["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})").str.zfill(7)
df_evasao[["Evasao_Fundamental","Evasao_Medio"]] = df_evasao[["Evasao_Fundamental","Evasao_Medio"]].apply(pd.to_numeric,errors="coerce")

resultado = res.merge(df_evasao[["CO_MUNICIPIO","Evasao_Fundamental","Evasao_Medio"]],
                      on="CO_MUNICIPIO", how="left")

resultado["Reprovacao_Iniciais"] = (1 - resultado["TAXA_APROVACAO_INICIAIS"])*100
resultado["Reprovacao_Finais"]   = (1 - resultado["TAXA_APROVACAO_FINAIS"])*100
resultado["Urgencia"] = resultado[["Evasao_Fundamental","Evasao_Medio","Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1)

urgentes = resultado.sort_values("Urgencia",ascending=False).head(20)

# =========================================================
# 8) Painel Streamlit
# =========================================================
st.set_page_config(page_title="IA • Aprovação, Evasão e Urgência", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Consolidado")

c1,c2,c3 = st.columns(3)
c1.metric("Municípios analisados", len(resultado))
c2.metric("Urgência média", f"{resultado['Urgencia'].mean():.1f}")
c3.metric("Top crítico", urgentes.iloc[0]["MUNICIPIO_NOME_ALP"])

st.subheader("Ranking de Urgência")
st.dataframe(urgentes[["MUNICIPIO_NOME_ALP","Urgencia"]])

fig = px.bar(urgentes, x="Urgencia", y="MUNICIPIO_NOME_ALP", orientation="h", title="Top 20 Municípios Críticos")
st.plotly_chart(fig, use_container_width=True)
