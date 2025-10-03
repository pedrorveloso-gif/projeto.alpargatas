# utils.py
import pandas as pd
import unicodedata
from pathlib import Path
import re
import streamlit as st
import numpy as np

# ============== 0. Configuração de Caminhos (Relativos ao GitHub) ==============
# O Streamlit Cloud clona seu repositório, então esses caminhos funcionarão
ARQ_DTB = "dados/dtb_municipios.ods"
ARQ_ALP = "dados/Dados_alpa.xlsx"
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS = "dados/anos_finais.xlsx"
ARQ_EM = "dados/ensino_medio.xlsx"
ARQ_EVASAO = "dados/evasao.ods"

# ============== 1. Utilitários Curto (Não precisam de cache) ==============
def nrm(txt: object) -> str:
    """Normaliza: remove acentos, vira CAIXA-ALTA e tira espaços. NaN -> ''."""
    if pd.isna(txt): return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    """Chave 'suave' para casamentos: caixa alta, remove pontuações e sufixos."""
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n: n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf): n = n[: -len(suf)].strip()
    return n

def acha_linha_header_cidades_uf(df_no_header: pd.DataFrame) -> int | None:
    """Retorna o índice da primeira linha que contenha CIDADES e UF (após normalização)."""
    for i, row in df_no_header.iterrows():
        vals = [nrm(x) for x in row.tolist()]
        if "CIDADES" in vals and "UF" in vals: return i
    return None

def _to_num(x: pd.Series) -> pd.Series:
    """Converte robustamente para numérico (remove %, vírgulas, espaços)."""
    return pd.to_numeric(
        x.astype(str)
         .str.replace("%","",regex=False)
         .str.replace(",","",regex=False)
         .str.replace(" ","",regex=False),
        errors="coerce"
    )

def _minmax(s: pd.Series) -> pd.Series:
    """Normalização Min-Max (0 a 1)."""
    s = pd.to_numeric(s, errors="coerce")
    s = s.fillna(s.median()) # Preenche NaNs com mediana antes de normalizar
    if s.dropna().empty or s.max() == s.min():
        return pd.Series(0.5, index=s.index)
    return (s - s.min())/(s.max()-s.min())


# ============== 2. Funções de Carga OTIMIZADAS com CACHE ==============

@st.cache_data(show_spinner="Carregando e processando DTB/IBGE...")
def carrega_dtb_cache(path: str) -> pd.DataFrame:
    """Lê DTB/IBGE e devolve DataFrame limpo."""
    UF_SIGLAS = {"ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA","CEARÁ":"CE","DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOIÁS":"GO","MARANHÃO":"MA","MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG","PARÁ":"PA","PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI","RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS","RONDÔNIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SÃO PAULO":"SP","SERGIPE":"SE","TOCANTINS":"TO"}
    raw = pd.read_excel(path, engine="odf", skiprows=6)
    dtb = (raw.rename(columns={"UF": "UF_COD_NUM","Nome_UF": "UF_NOME","Código Município Completo": "MUNICIPIO_CODIGO","Nome_Município": "MUNICIPIO_NOME"})
           [["UF_COD_NUM","UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]].dropna(subset=["UF_NOME"]))
    dtb["UF_SIGLA"] = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"] = dtb["MUNICIPIO_CODIGO"].astype(str).str.zfill(7)
    dtb["MUNICIPIO_CHAVE"] = dtb["MUNICIPIO_NOME"].apply(chave_municipio)
    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]].copy()


@st.cache_data(show_spinner="Lendo e unindo abas do Alpargatas...")
def carrega_alpargatas_cache(path: str) -> pd.DataFrame:
    """Lê todas as abas (2020–2025), detecta header e extrai cidade/UF em um único DataFrame."""
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    frames = []
    for aba in abas:
        try:
            nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
            hdr = acha_linha_header_cidades_uf(nohdr)
            if hdr is None: continue

            df = pd.read_excel(path, sheet_name=aba, header=hdr)
            cmap = {c: nrm(c) for c in df.columns}
            c_cid = next((orig for orig, norm in cmap.items() if norm == "CIDADES"), None)
            c_uf = next((orig for orig, norm in cmap.items() if norm == "UF"), None)
            if not c_cid or not c_uf: continue

            tmp = (df[[c_cid, c_uf]].copy().rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"}))
            tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
            tmp["UF_SIGLA"] = tmp["UF_SIGLA"].astype(str).str.strip()
            tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"]).copy()
            tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
            frames.append(tmp)
        except Exception: # Evita que uma aba ruim quebre tudo
            continue

    if not frames: raise RuntimeError("Nenhuma aba válida foi processada.")
    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"]).copy()

@st.cache_data(show_spinner="Carregando INEP/IDEB e Evasão...")
def carrega_dados_inep_e_evasao(path_ini, path_fin, path_em, path_evasao):
    """Carrega todos os 4 arquivos INEP de uma vez com cache."""
    df_iniciais = pd.read_excel(path_ini, header=9)
    df_finais = pd.read_excel(path_fin, header=9)
    df_em = pd.read_excel(path_em, header=9)
    df_evasao = pd.read_excel(path_evasao, header=8, engine="odf") # 'engine' para .ods
    return df_iniciais, df_finais, df_em, df_evasao

# ============== 3. Funções de Processamento OTIMIZADAS ==============

@st.cache_data(show_spinner="Calculando taxas de aprovação (2023) e merge...")
def processa_aprovacao(df_iniciais, df_finais, df_em, codificados: pd.DataFrame):
    """Calcula médias de aprovação por município e faz o merge na base Alpargatas."""
    
    def media_por_municipio(df: pd.DataFrame, rotulo_saida: str) -> pd.DataFrame:
        df = df.copy()
        df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
        ind = pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce")
        out = (pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo_saida: ind})
               .groupby("CO_MUNICIPIO", as_index=False)[rotulo_saida].mean())
        return out
    
    ini = media_por_municipio(df_iniciais, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_finais,   "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_em,       "TAXA_APROVACAO_MEDIO")

    # Merge e pós-processamento
    res = codificados.copy()
    res["MUNICIPIO_CODIGO"] = res["MUNICIPIO_CODIGO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    
    res = (res.merge(ini, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
             .merge(fin, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_fin"))
             .merge(med, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_med")))

    # Remove colunas CO_MUNICIPIO repetidas
    for c in ["CO_MUNICIPIO", "CO_MUNICIPIO_fin", "CO_MUNICIPIO_med", "MUNICIPIO_NOME_IBGE"]:
        if c in res.columns: res.drop(columns=c, inplace=True)
    
    # Conversão para Porcentagem e arredondamento (simplificado)
    for c in ["INICIAIS", "FINAIS", "MEDIO"]:
        prop_col = f"TAXA_APROVACAO_{c}"
        pct_col = f"TAXA_APROVACAO_{c}_%"
        if prop_col in res.columns:
            res[pct_col] = (res[prop_col] * 100).round(2)
            res[prop_col] = res[prop_col].round(4)
    
    # Manutenção dos ajustes manuais do código original
    res = res.iloc[:18].copy() 
    mask = (res["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & (res["UF_SIGLA"] == "PB") & (res["MUNICIPIO_CODIGO"].isna())
    res.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"
    if 1 in res.index:
        res.loc[1, "TAXA_APROVACAO_INICIAIS_%"] = pd.to_numeric("90.66", errors="coerce")
        res.loc[1, "TAXA_APROVACAO_INICIAIS"] = pd.to_numeric("0.9066", errors="coerce")
    
    return res


@st.cache_data(show_spinner="Calculando Urgência, Evasão e Winsorização...")
def processa_evasao_e_ranking(df_aprov: pd.DataFrame, df_evasao: pd.DataFrame) -> pd.DataFrame:
    """Calcula Evasão, aplica Winsorização, e define o índice de Urgência."""
    
    # 1. Preparação da Evasão (df_filtrado)
    df_filtrado = df_evasao.rename(columns={"1_CAT3_CATFUN": "Evasão - Fundamental", "1_CAT3_CATMED": "Evasão - Médio"})
    for col in ["Evasão - Fundamental", "Evasão - Médio"]:
        if col in df_filtrado.columns: df_filtrado[col] = _to_num(df_filtrado[col])
    
    # 2. Merge com a base de aprovação
    df_aprov_ok = df_aprov.dropna(subset=["MUNICIPIO_CODIGO"]).copy()
    df_filtrado_ok = df_filtrado.dropna(subset=["CO_MUNICIPIO"]).copy()
    
    df_aprov_ok["COD_MERGE"] = pd.to_numeric(df_aprov_ok["MUNICIPIO_CODIGO"], errors="coerce").astype("Int64")
    df_filtrado_ok["COD_MERGE"] = pd.to_numeric(df_filtrado_ok["CO_MUNICIPIO"], errors="coerce").astype("Int64")

    df_merge = pd.merge(df_aprov_ok, df_filtrado_ok, on="COD_MERGE", how="inner", suffixes=("_ALPA","_INEP"))
    
    # 3. Tratamento de Outliers (Winsorização)
    num_cols = ["Evasão - Fundamental", "Evasão - Médio", "TAXA_APROVACAO_INICIAIS", "TAXA_APROVACAO_FINAIS"]
    resultado_num = df_merge.copy()
    for col in num_cols:
         if col not in resultado_num.columns: continue
         resultado_num[col] = _to_num(resultado_num[col]) # Garante que está numérico

    Q1 = resultado_num[num_cols].quantile(0.25, numeric_only=True)
    Q3 = resultado_num[num_cols].quantile(0.75, numeric_only=True)
    IQR = Q3 - Q1
    low = Q1 - 1.5 * IQR
    high = Q3 + 1.5 * IQR

    winsor_df = resultado_num.copy()
    for col in num_cols:
        if col in winsor_df.columns: winsor_df[col] = winsor_df[col].clip(lower=low[col], upper=high[col])

    # 4. Cálculo da Urgência
    winsor_df["Reprovacao_Iniciais"] = (1 - winsor_df["TAXA_APROVACAO_INICIAIS"]) * 100
    winsor_df["Reprovacao_Finais"] = (1 - winsor_df["TAXA_APROVACAO_FINAIS"]) * 100
    winsor_df["Urgencia"] = (
        winsor_df["Evasão - Fundamental"] +
        winsor_df["Evasão - Médio"] +
        winsor_df["Reprovacao_Iniciais"] +
        winsor_df["Reprovacao_Finais"]
    )
    
    return winsor_df.sort_values("Urgencia", ascending=False).copy()


@st.cache_data(show_spinner="Calculando Score de Risco Final...")
def build_static_data(df_urgentes: pd.DataFrame) -> pd.DataFrame:
    """Aplica as métricas finais de Score de Risco na tabela Urgentes."""
    urg = df_urgentes.copy()
    
    # 1. Padronizar nomes para o cálculo
    urg = urg.rename(columns={
        "Evasão - Fundamental": "EVASAO_FUNDAMENTAL",
        "Evasão - Médio": "EVASAO_MEDIO",
        "TAXA_APROVACAO_INICIAIS_%": "APROVACAO_INICIAIS_PCT",
        "TAXA_APROVACAO_FINAIS_%": "APROVACAO_FINAIS_PCT",
        "MUNICIPIO_NOME_ALP": "MUNICIPIO_NOME"
    })
    
    # 2. Garantir numéricos e calcular GAP
    for c in ["EVASAO_FUNDAMENTAL","APROVACAO_INICIAIS_PCT","APROVACAO_FINAIS_PCT"]:
        if c in urg.columns: urg[c] = _to_num(urg[c])
        
    urg["GAP_APROV_%"] = urg["APROVACAO_INICIAIS_PCT"] - urg["APROVACAO_FINAIS_PCT"]
    
    # 3. Normalização e Score de Risco
    aprov_finais_norm = 1 - _minmax(urg["APROVACAO_FINAIS_PCT"])
    evasao_fund_norm = _minmax(urg["EVASAO_FUNDAMENTAL"])
    gap_norm = _minmax(urg["GAP_APROV_%"])
    
    # Fórmula do seu código: 0.5 * (1 - APROV_FINAIS_NORM) + 0.4 * EVASAO_NORM + 0.1 * GAP_NORM
    urg["SCORE_RISCO"] = 0.5 * aprov_finais_norm + 0.4 * evasao_fund_norm + 0.1 * gap_norm

    # Colunas de interesse
    cols_final = ["MUNICIPIO_NOME","UF_SIGLA", "NO_LOCALIZACAO","NO_DEPENDENCIA",
                  "APROVACAO_INICIAIS_PCT","APROVACAO_FINAIS_PCT","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
                  "GAP_APROV_%","Urgencia","SCORE_RISCO"]
    
    return urg[[c for c in cols_final if c in urg.columns]].sort_values("SCORE_RISCO", ascending=False).head(20).copy()
