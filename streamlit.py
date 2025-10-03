import pandas as pd
import unicodedata
from pathlib import Path
import re
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st
import plotly.express as px

# Configuração inicial do Streamlit
st.set_page_config(page_title="IA • Aprovação, Evasão e Urgência", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel")
st.caption("Análise de dados de aprovação, evasão e urgência educacional.")

# ============================
# 0) AJUSTE OS CAMINHOS AQUI (TODOS EM .xlsx ou caminho estável)
# ============================
# ERRO CORRIGIDO: ARQ_ALP precisa ser definido
ARQ_ALP = "dados/Dados_alpa.xlsx"
ARQ_DTB = "dados/dtb_municipios.xlsx" 
ODS_INICIAIS = "dados/anos_iniciais.xlsx" 
ODS_FINAIS = "dados/anos_finais.xlsx" 
ODS_EM = "dados/ensino_medio.xlsx" 
CAMINHO_EVASAO = "dados/evasao.ods" # Se você não converteu para XLSX, manter o .ods

# =========================================================
# 1) Utilitários (Funções auxiliares sem St.cache)
# =========================================================
def nrm(txt: object) -> str:
    """Normaliza: remove acentos, vira CAIXA-ALTA e tira espaços. NaN -> ''."""
    if pd.isna(txt):
        return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    """Chave 'suave' para casamentos de município."""
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n:
        n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    return n

def acha_linha_header_cidades_uf(df_no_header: pd.DataFrame) -> int | None:
    """Retorna o índice da primeira linha que contenha CIDADES e UF (após normalização)."""
    for i, row in df_no_header.iterrows():
        vals = [nrm(x) for x in row.tolist()]
        if "CIDADES" in vals and "UF" in vals:
            return i
    return None

def media_por_municipio(df: pd.DataFrame, rotulo_saida: str) -> pd.DataFrame:
    """Calcula a MÉDIA do indicador (VL_INDICADOR_REND_2023) por município (CO_MUNICIPIO)."""
    df = df.copy()
    df["CO_MUNICIPIO"] = (
        df["CO_MUNICIPIO"]
        .astype(str)
        .str.extract(r"(\d{7})", expand=False)
        .str.zfill(7)
    )
    ind = pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce")
    out = (
        pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo_saida: ind})
        .groupby("CO_MUNICIPIO", as_index=False)[rotulo_saida]
        .mean()
    )
    return out

def _anos_disponiveis(df: pd.DataFrame, ano_min=2005, ano_max=2023) -> list[int]:
    """Detecta automaticamente os anos que existem como VL_INDICADOR_REND_YYYY dentro do range dado."""
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m:
            a = int(m.group(1))
            if ano_min <= a <= ano_max:
                anos.append(a)
    return sorted(set(anos))

def _long_por_municipio_ano(df: pd.DataFrame, etapa_rotulo: str) -> pd.DataFrame:
    """Converte uma planilha (iniciais/finais/médio) para formato longo: colunas: CO_MUNICIPIO, ANO, <etapa_rotulo>."""
    df = df.copy()
    if "CO_MUNICIPIO" not in df.columns: raise KeyError("Planilha não possui CO_MUNICIPIO.")
    df["CO_MUNICIPIO"] = (df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7))
    anos = _anos_disponiveis(df, 2005, 2023)
    if not anos: raise KeyError("Nenhuma coluna VL_INDICADOR_REND_YYYY encontrada no intervalo 2005–2023.")
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    num = df[["CO_MUNICIPIO"] + cols].copy()
    for c in cols: num[c] = pd.to_numeric(num[c], errors="coerce")

    long_df = num.melt(id_vars="CO_MUNICIPIO", value_vars=cols, var_name="COL", value_name=etapa_rotulo)
    long_df["ANO"] = long_df["COL"].str.extract(r"(\d{4})").astype(int)
    long_df = long_df.drop(columns=["COL"])
    
    long_grp = (long_df.groupby(["CO_MUNICIPIO", "ANO"], as_index=False)[etapa_rotulo].mean())
    return long_grp

def ensure_key_urgentes(urgentes: pd.DataFrame) -> pd.DataFrame:
    """Garante a chave de casamento (MUNICIPIO_CHAVE) na base urgentes."""
    u = urgentes.copy()
    if "MUNICIPIO_NOME_ALP" in u.columns:
        base_nome = u["MUNICIPIO_NOME_ALP"].where(u["MUNICIPIO_NOME_ALP"].notna(), u.get("NO_MUNICIPIO"))
    else:
        base_nome = u.get("NO_MUNICIPIO")
    u["MUNICIPIO_CHAVE"] = base_nome.apply(chave_municipio)
    return u

def _minmax(s: pd.Series) -> pd.Series:
    """Normaliza para 0-1, tratando NaNs e casos de min=max."""
    s = pd.to_numeric(s, errors="coerce")
    s_clean = s.dropna()
    if s_clean.empty or s_clean.max() == s_clean.min():
        return pd.Series(0.5, index=s.index)
    return (s - s_clean.min()) / (s_clean.max() - s_clean.min())

def _to_num(x: pd.Series) -> pd.Series:
    """Coerção robusta para numérico."""
    return pd.to_numeric(
        x.astype(str)
         .str.replace("%","",regex=False)
         .str.replace(",","",regex=False) 
         .str.replace(" ","",regex=False),
        errors="coerce"
    )

# =========================================================
# 2) Funções de Carregamento e Processamento (Cache)
# =========================================================

# --- Leitura da DTB (IBGE) ---
@st.cache_data
def carrega_dtb(path: str) -> pd.DataFrame:
    """Lê DTB/IBGE e devolve DataFrame com colunas-chave já limpas e prontas."""
    UF_SIGLAS = {"ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA",
                 "CEARÁ":"CE","DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOIÁS":"GO",
                 "MARANHÃO":"MA","MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG",
                 "PARÁ":"PA","PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI",
                 "RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS",
                 "RONDÔNIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SÃO PAULO":"SP",
                 "SERGIPE":"SE","TOCANTINS":"TO"}
    try:
        # Usando a leitura padrão para .xlsx
        raw = pd.read_excel(path, skiprows=6) 
    except FileNotFoundError:
        st.error(f"Arquivo DTB não encontrado: {path}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao ler DTB. Tente garantir que o arquivo é um XLSX válido: {e}")
        return pd.DataFrame()

    dtb = (raw.rename(columns={
                "UF": "UF_COD_NUM", "Nome_UF": "UF_NOME",
                "Código Município Completo": "MUNICIPIO_CODIGO",
                "Nome_Município": "MUNICIPIO_NOME"
            })[["UF_COD_NUM","UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
            .dropna(subset=["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]))

    dtb["UF_SIGLA"] = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"] = dtb["MUNICIPIO_CODIGO"].astype(str).str.zfill(7)
    dtb["MUNICIPIO_NOME"] = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"] = dtb["MUNICIPIO_NOME"].apply(chave_municipio)

    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

# --- Leitura do arquivo Alpargatas ---
@st.cache_data
def carrega_alpargatas(path: str) -> pd.DataFrame:
    """Lê todas as abas (2020–2025) do Alpargatas e extrai CIDADES/UF."""
    try:
        xls = pd.ExcelFile(path)
    except FileNotFoundError:
        st.error(f"Arquivo Alpargatas não encontrado: {path}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao ler Alpargatas: {e}")
        return pd.DataFrame()

    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        st.warning("Nenhuma aba 2020–2025 encontrada no arquivo Alpargatas.")
        return pd.DataFrame()

    frames = []
    for aba in abas:
        try:
            nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
            hdr = acha_linha_header_cidades_uf(nohdr)
            if hdr is None:
                continue 

            df = pd.read_excel(path, sheet_name=aba, header=hdr)

            cmap = {c: nrm(c) for c in df.columns}
            c_cid = next((orig for orig, norm in cmap.items() if norm == "CIDADES"), None)
            c_uf = next((orig for orig, norm in cmap.items() if norm == "UF"), None)
            if not c_cid or not c_uf:
                continue 

            tmp = (df[[c_cid, c_uf]].copy()
                    .rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"}))
            tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
            tmp["UF_SIGLA"] = tmp["UF_SIGLA"].astype(str).str.strip()
            tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
            tmp = tmp[tmp["MUNICIPIO_NOME_ALP"].str.len() > 0]

            tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
            tmp["FONTE_ABA"] = aba
            frames.append(tmp)
        except Exception as e:
            st.warning(f"Erro ao processar aba '{aba}': {e}")
            continue

    if not frames:
        st.error("Nenhuma aba válida foi processada.")
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])

# --- Cruzamento Alpargatas × IBGE (Build Codificados) ---
@st.cache_data
def build_codificados(dtb: pd.DataFrame, alpa: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Casa Alpargatas × IBGE e aplica correção manual de Campina Grande."""
    if dtb.empty or alpa.empty: return pd.DataFrame(), pd.DataFrame()

    codificados = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE"))

    # Ajuste CAMPINA GRANDE (PB)
    mask = (
        codificados["MUNICIPIO_NOME_ALP"].astype(str).str.contains("CAMPINA GRANDE", case=False, na=False, regex=False)
        & (codificados["UF_SIGLA"] == "PB")
        & (codificados["MUNICIPIO_CODIGO"].isna())
    )
    codificados.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"
    codificados = codificados.drop(columns=["MUNICIPIO_NOME_IBGE"], errors="ignore")

    nao_encontrados = (codificados[codificados["MUNICIPIO_CODIGO"].isna()]
                         .drop_duplicates(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
                         .sort_values(["UF_SIGLA","MUNICIPIO_NOME_ALP"]))

    return codificados, nao_encontrados

# --- Carregamento e fusão de dados de Aprovação (IDEB/INEP) ---
@st.cache_data
def build_taxas_aprovacao(codificados: pd.DataFrame, ini_path: str, fin_path: str, em_path: str) -> pd.DataFrame:
    """Lê dados de aprovação, calcula médias e funde com a base 'codificados'."""
    if codificados.empty: return pd.DataFrame()
    
    # Carregar arquivos INEP
    try:
        df_iniciais = pd.read_excel(ini_path, header= 9)
        df_finais = pd.read_excel(fin_path, header = 9)
        df_em = pd.read_excel(em_path, header = 9)
    except FileNotFoundError:
        st.error("Arquivos IDEB/INEP não encontrados. Verifique os caminhos.")
        return pd.DataFrame()

    # Calcular as médias
    ini = media_por_municipio(df_iniciais, "TAXA_APROVACAO_INICIAIS_P")
    fin = media_por_municipio(df_finais, "TAXA_APROVACAO_FINAIS_P")
    med = media_por_municipio(df_em, "TAXA_APROVACAO_MEDIO_P")

    # Colunas em percentual
    ini["TAXA_APROVACAO_INICIAIS_%"] = ini["TAXA_APROVACAO_INICIAIS_P"] * 100
    fin["TAXA_APROVACAO_FINAIS_%"] = fin["TAXA_APROVACAO_FINAIS_P"] * 100
    med["TAXA_APROVACAO_MEDIO_%"] = med["TAXA_APROVACAO_MEDIO_P"] * 100

    res = codificados.copy()

    # Padroniza código
    res["MUNICIPIO_CODIGO"] = (
        res["MUNICIPIO_CODIGO"]
        .astype(str)
        .str.extract(r"(\d{7})", expand=False)
        .str.zfill(7)
    )

    # Merge com as três tabelas
    res = (
        res.merge(ini, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
        .merge(fin, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_fin"))
        .merge(med, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_med"))
    )

    # Remove colunas CO_MUNICIPIO repetidas
    for c in ["CO_MUNICIPIO", "CO_MUNICIPIO_fin", "CO_MUNICIPIO_med"]:
        if c in res.columns: res.drop(columns=c, inplace=True)

    res = res.rename(columns=lambda x: x.replace("_P", "") if x.endswith("_P") else x)
    cols_remover = ["TAXA_APROVACAO_INICIAIS", "TAXA_APROVACAO_FINAIS", "TAXA_APROVACAO_MEDIO"]
    res = res.drop(columns=cols_remover, errors="ignore")

    return res

# --- Carregamento e fusão de dados de Evasão ---
@st.cache_data
def build_evasao(taxas_aprovacao: pd.DataFrame, evasao_path: str) -> pd.DataFrame:
    """Lê dados de evasão, cruza com as taxas de aprovação, aplica Winsorização e calcula Urgência."""
    if taxas_aprovacao.empty: return pd.DataFrame()
    
    try:
        # Se for ODS, engine deve ser 'odf' (se a lib estiver instalada), caso contrário, o default é 'openpyxl' para XLSX
        if evasao_path.endswith('.ods'):
            df_evasao = pd.read_excel(evasao_path, header=8, engine='odf')
        else:
            df_evasao = pd.read_excel(evasao_path, header=8) 
    except FileNotFoundError:
        st.error(f"Arquivo de Evasão não encontrado: {evasao_path}")
        return taxas_aprovacao
    except Exception as e:
        st.error(f"Erro ao ler arquivo de Evasão ({evasao_path}). Verifique se é um XLSX válido, ou se o ODS exige a instalação do 'odfpy'. Erro: {e}")
        return taxas_aprovacao


    colunas_desejadas = [
        "CO_MUNICIPIO", "NO_MUNICIPIO", "NO_LOCALIZACAO", "NO_DEPENDENCIA",
        "1_CAT3_CATFUN", "1_CAT3_CATMED"
    ]
    df_filtrado = df_evasao[[c for c in colunas_desejadas if c in df_evasao.columns]]

    mapa_colunas = {"1_CAT3_CATFUN": "Fundamental - Total", "1_CAT3_CATMED": "Médio - Total"}
    df_filtrado = df_filtrado.rename(columns=mapa_colunas)

    for col in ["Fundamental - Total", "Médio - Total"]:
        if col in df_filtrado.columns:
            df_filtrado[col] = pd.to_numeric(
                df_filtrado[col].astype(str).str.replace(",", "."), errors="coerce"
            )

    res_ok = taxas_aprovacao.copy().dropna(subset=["MUNICIPIO_CODIGO"])
    df_filtrado_ok = df_filtrado.dropna(subset=["CO_MUNICIPIO"])

    res_ok["MUNICIPIO_CODIGO"] = pd.to_numeric(res_ok["MUNICIPIO_CODIGO"], errors="coerce").astype("Int64")
    df_filtrado_ok["CO_MUNICIPIO"] = pd.to_numeric(df_filtrado_ok["CO_MUNICIPIO"], errors="coerce").astype("Int64")

    df_merge = pd.merge(
        res_ok, df_filtrado_ok,
        left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="inner"
    )

    resultado = df_merge.rename(
        columns={"Fundamental - Total": "Evasão - Fundamental", "Médio - Total": "Evasão -Médio"}
    ).copy()
    
    num_cols = ["Evasão - Fundamental", "Evasão -Médio", "TAXA_APROVACAO_INICIAIS", "TAXA_APROVACAO_FINAIS"]
    num_cols = [c for c in num_cols if c in resultado.columns]
    
    for col in num_cols:
        resultado[col] = _to_num(resultado[col]) # Usando _to_num para coerção robusta

    # Winsorização (Cap) e Cálculo de Urgência
    winsor_df = resultado.copy()
    if not winsor_df[num_cols].empty:
        Q1 = winsor_df[num_cols].quantile(0.25, numeric_only=True)
        Q3 = winsor_df[num_cols].quantile(0.75, numeric_only=True)
        IQR = Q3 - Q1
        low = Q1 - 1.5 * IQR
        high = Q3 + 1.5 * IQR

        for col in num_cols:
            if col in winsor_df.columns:
                winsor_df[col] = winsor_df[col].clip(lower=low.get(col, -np.inf), upper=high.get(col, np.inf))
    
    # Cálculo de Reprovação e Urgência
    winsor_df["Reprovacao_Iniciais"] = (1 - winsor_df["TAXA_APROVACAO_INICIAIS"]) * 100
    winsor_df["Reprovacao_Finais"] = (1 - winsor_df["TAXA_APROVACAO_FINAIS"]) * 100

    winsor_df["Urgencia"] = (
        winsor_df["Evasão - Fundamental"] +
        winsor_df["Evasão -Médio"] +
        winsor_df["Reprovacao_Iniciais"] +
        winsor_df["Reprovacao_Finais"]
    )

    colunas_essenciais = [
        "MUNICIPIO_CODIGO", "UF_SIGLA", "MUNICIPIO_NOME_ALP", "NO_MUNICIPIO", "NO_LOCALIZACAO", "NO_DEPENDENCIA",
        "Evasão - Fundamental", "Evasão -Médio", "TAXA_APROVACAO_INICIAIS", "TAXA_APROVACAO_FINAIS",
        "Reprovacao_Iniciais", "Reprovacao_Finais", "Urgencia"
    ]
    urgentes = winsor_df.sort_values("Urgencia", ascending=False).head(20).copy()
    urgentes = urgentes[[c for c in colunas_essenciais if c in urgentes.columns]]
    
    return urgentes

# --- Construção da Evolução Histórica (Tabela Longa) ---
@st.cache_data
def build_evolucao_filtrada(df_iniciais: pd.DataFrame, df_finais: pd.DataFrame, df_em: pd.DataFrame, dtb_lookup: pd.DataFrame, urgentes: pd.DataFrame) -> pd.DataFrame:
    """Calcula a evolução histórica das taxas de aprovação (long format) e preenche nulos."""
    if urgentes.empty: return pd.DataFrame()

    # Leitura dos arquivos ODS/XLSX (necessário carregar aqui para evitar conflito de cache)
    # df_iniciais, df_finais, df_em já estão sendo carregados e passados via _load_inep_data
    
    # 1. Long format para cada etapa
    evo_ini = _long_por_municipio_ano(df_iniciais, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_finais, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_em, "APROVACAO_MEDIO")

    # 2. Merge por município + ano
    evolucao = (evo_ini
                .merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))

    # Média simples
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
        ["APROVACAO_INICIAIS", "APROVACAO_FINAIS", "APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)
    
    # Versões em porcentagem
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]:
        evolucao[c + "_%"] = (evolucao[c] * 100).round(2)

    # 3. Anexar UF e nome oficial
    evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")

    # 4. Filtrar apenas municípios presentes em URGENTES
    urgentes = ensure_key_urgentes(urgentes)
    evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)

    evolucao_filtrada = evolucao.merge(
        urgentes[["UF_SIGLA","MUNICIPIO_CHAVE"]].drop_duplicates(),
        on=["UF_SIGLA","MUNICIPIO_CHAVE"],
        how="inner"
    ).sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True)

    # 5. Preencher NaN pela mediana dos outros anos (por município)
    cols_num = [
        "APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL",
        "APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%","APROVACAO_MEDIA_GERAL_%"
    ]
    
    def preencher_por_mediana(df, grupo="MUNICIPIO_CHAVE", cols=cols_num):
        df = df.copy()
        for col in cols:
            if col in df.columns:
                df[col] = df.groupby(grupo)[col].transform(lambda x: x.fillna(x.median(skipna=True)))
        return df

    evolucao_filtrada = preencher_por_mediana(evolucao_filtrada)
    return evolucao_filtrada.drop(columns=["MUNICIPIO_CHAVE"], errors="ignore")


# --- Montagem da tabela estática de risco (df_static) ---
@st.cache_data
def build_df_static(evolucao_filtrada: pd.DataFrame, urgentes: pd.DataFrame) -> pd.DataFrame:
    """Cria a tabela estática com médias de aprovação e score de risco."""
    if evolucao_filtrada.empty or urgentes.empty: return pd.DataFrame()

    evo = evolucao_filtrada.copy()
    evo["MUNICIPIO_NOME"] = evo["MUNICIPIO_NOME"].astype(str).str.strip()
    
    # Média do período (robusto)
    df_static = (
        evo.groupby(["MUNICIPIO_NOME"], as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
        .mean(numeric_only=True)
    )

    # Prepara evasão
    urg = urgentes.rename(columns={"Evasão - Fundamental": "EVASAO_FUNDAMENTAL"})
    # Usa NO_MUNICIPIO (nome do município no IBGE) para garantir que o merge funciona
    nome_col_urg = next((c for c in ["NO_MUNICIPIO", "MUNICIPIO_NOME_ALP"] if c in urg.columns), None)
    if nome_col_urg:
        urg["MUNICIPIO_NOME"] = urg[nome_col_urg].astype(str).str.strip()
        urg = urg.groupby("MUNICIPIO_NOME", as_index=False)["EVASAO_FUNDAMENTAL"].mean(numeric_only=True)
    
    # Merge evasão
    df_static = df_static.merge(urg[["MUNICIPIO_NOME","EVASAO_FUNDAMENTAL"]], on="MUNICIPIO_NOME", how="left")

    # Coerção final -> numérico
    for c in ["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]:
        if c in df_static.columns: df_static[c] = _to_num(df_static[c])

    # Métricas derivadas e Score de Risco
    df_static["GAP_APROV_%"] = df_static["APROVACAO_INICIAIS_%"] - df_static["APROVACAO_FINAIS_%"]
    
    # Normalização
    aprov_finais_norm = 1 - _minmax(df_static["APROVACAO_FINAIS_%"].fillna(df_static["APROVACAO_FINAIS_%"].median()))
    evasao_norm = _minmax(df_static["EVASAO_FUNDAMENTAL"].fillna(df_static["EVASAO_FUNDAMENTAL"].median()))
    gap_norm = _minmax(df_static["GAP_APROV_%"].fillna(0))

    # Score (pesos: 50% aprov. finais, 40% evasão, 10% gap)
    df_static["SCORE_RISCO"] = 0.5 * aprov_finais_norm + 0.4 * evasao_norm + 0.1 * gap_norm
    
    return df_static

# =========================================================
# 3) Funções de Geração de Gráficos (Streamlit)
# =========================================================

def graf_tendencia_geral(evo: pd.DataFrame):
    t = evo.dropna(subset=["ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]).copy()
    m = t.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean()
    melted = m.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
    fig = px.line(melted, x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                  title="Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)")
    fig.update_layout(yaxis_tickformat=".1f", yaxis_range=[60, 100])
    return fig

def graf_ranking_risco(df_static: pd.DataFrame, top_n=20):
    t = df_static.dropna(subset=["SCORE_RISCO"]).copy()
    t = t.sort_values("SCORE_RISCO", ascending=False).head(top_n)
    fig = px.bar(
        t, x="SCORE_RISCO", y="MUNICIPIO_NOME", orientation="h",
        hover_data=["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL","GAP_APROV_%"],
        title=f"Top {top_n} — Ranking de Risco",
        labels={"MUNICIPIO_NOME":"Município","SCORE_RISCO":"Score de Risco (0–1)"}
    )
    fig.update_yaxes(categoryorder="total ascending")
    return fig

def graf_quadrantes_risco(df_static: pd.DataFrame, usar_tamanho_por_risco=True):
    t = df_static.dropna(subset=["APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]).copy()
    if t.empty: return None

    cut_x = t["APROVACAO_FINAIS_%"].median()
    cut_y = t["EVASAO_FUNDAMENTAL"].median()

    conds = [
        (t["APROVACAO_FINAIS_%"] < cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] < cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
    ]
    labels = ["Crítico","Atenção","Apoio pedagógico","OK"]
    t["Quadrante"] = np.select(conds, labels)
    t["LABEL"] = t["MUNICIPIO_NOME"].str.title().str.slice(0, 18)

    size_arg = "SCORE_RISCO" if usar_tamanho_por_risco and "SCORE_RISCO" in t.columns else None

    fig = px.scatter(
        t, x="APROVACAO_FINAIS_%", y="EVASAO_FUNDAMENTAL",
        color="Quadrante", size=size_arg, size_max=26,
        hover_data=["MUNICIPIO_NOME","APROVACAO_INICIAIS_%","GAP_APROV_%","SCORE_RISCO"],
        text="LABEL",
        title="Quadrantes — Aprovação (Anos Finais) × Evasão (Fundamental)",
        labels={"APROVACAO_FINAIS_%":"Aprovação Finais (%)","EVASAO_FUNDAMENTAL":"Evasão Fundamental (%)"},
    )
    fig.add_vline(x=cut_x, line_width=2, line_dash="dash", annotation_text=f"Mediana Aprov: {cut_x:.1f}%")
    fig.add_hline(y=cut_y, line_width=2, line_dash="dash", annotation_text=f"Mediana Evasão: {cut_y:.1f}%", annotation_position="bottom right")

    fig.update_traces(textposition="top center", marker=dict(opacity=0.8, line=dict(width=1, color="white")))
    return fig

# =========================================================
# 4) Execução Principal (DataFlow)
# =========================================================

# Função auxiliar para garantir a leitura dos arquivos IDEB/INEP no cache
@st.cache_data
def _load_inep_data():
    try:
        df_iniciais = pd.read_excel(ODS_INICIAIS, header=9)
        df_finais = pd.read_excel(ODS_FINAIS, header=9)
        df_em = pd.read_excel(ODS_EM, header=9)
        return df_iniciais, df_finais, df_em
    except FileNotFoundError:
        st.error("Arquivos IDEB/INEP não encontrados.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


st.info("Script Python iniciou a execução e o carregamento dos dados.")

try:
    # --- 4.1 Carregamento e Codificação Inicial ---
    with st.spinner("Carregando e codificando bases (DTB/Alpargatas)..."):
        # AQUI O ERRO OCORRIA! ARQ_ALP agora está definido no topo
        dtb = carrega_dtb(ARQ_DTB)
        alpa = carrega_alpargatas(ARQ_ALP)
        codificados, _ = build_codificados(dtb, alpa)

    # --- 4.2 Taxas de Aprovação, Evasão, Urgência e Evolução ---
    if not codificados.empty:
        df_iniciais, df_finais, df_em = _load_inep_data()
        dtb_lookup = dtb[["MUNICIPIO_CODIGO", "UF_SIGLA", "MUNICIPIO_NOME"]].rename(columns={"MUNICIPIO_CODIGO": "CO_MUNICIPIO"}).copy()
        
        with st.spinner("Calculando taxas de aprovação..."):
            taxas_aprovacao = build_taxas_aprovacao(codificados, ODS_INICIAIS, ODS_FINAIS, ODS_EM)

        with st.spinner("Calculando evasão e grau de urgência..."):
            urgentes = build_evasao(taxas_aprovacao, CAMINHO_EVASAO)

        with st.spinner("Preparando a série histórica (evolução)..."):
            evolucao_filtrada = build_evolucao_filtrada(df_iniciais, df_finais, df_em, dtb_lookup, urgentes)

        with st.spinner("Calculando a tabela estática de risco (df_static)..."):
            df_static_ready = build_df_static(evolucao_filtrada, urgentes)
    else:
        st.error("Falha ao carregar as bases principais. Verifique os logs.")
        df_static_ready = pd.DataFrame()
        evolucao_filtrada = pd.DataFrame()
        urgentes = pd.DataFrame()

except Exception as e:
    st.error(f"Ocorreu um erro fatal durante o processamento de dados: {e}")
    st.warning("Verifique se todos os arquivos estão na pasta 'dados/' e se o arquivo XLSX da DTB e Evasão não está corrompido.")
    df_static_ready = pd.DataFrame()
    evolucao_filtrada = pd.DataFrame()
    urgentes = pd.DataFrame()


st.success("Carregamento de dados concluído.")
# =========================================================
# 5) Interface do Streamlit
# =========================================================

if df_static_ready.empty:
    st.info("Aguardando o carregamento dos dados para exibir o painel.")
else:
    # 5.1 KPIs
    df = df_static_ready
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Municípios no recorte", len(df["MUNICIPIO_NOME"].unique()))
    with c2: st.metric("Aprovação — Finais (média)", f"{df['APROVACAO_FINAIS_%'].mean():.1f}%")
    with c3: st.metric("Evasão — Fundamental (média)", f"{df['EVASAO_FUNDAMENTAL'].mean():.1f}%")
    with c4: st.metric("Score de risco (média)", f"{df['SCORE_RISCO'].mean():.2f}")
    st.divider()

    # 5.2 Abas
    tab_overview, tab_risco, tab_evolucao, tab_tables = st.tabs(["Visão Geral", "Análise de Risco", "Evolução Histórica", "Tabelas (RAW)"])

    with tab_overview:
        st.subheader("Introdução e Metodologia")
        st.markdown("""
        Este painel visa **mapear os municípios com maior urgência educacional** e avaliar os desafios nos locais de atuação.
        O **Score de Risco** combina baixa taxa de aprovação (Anos Finais), alta taxa de evasão (Fundamental) e o GAP de aprovação (Iniciais - Finais).
        """)

    with tab_risco:
        st.subheader("Ranking e Quadrantes de Risco")
        
        st.plotly_chart(graf_quadrantes_risco(df_static_ready), use_container_width=True)
        st.info("Os pontos são os municípios do recorte. O tamanho do círculo indica o Score de Risco.")

        st.plotly_chart(graf_ranking_risco(df_static_ready), use_container_width=True)
        
        st.markdown("---")
        st.subheader("Top 10 Municípios por Urgência (Métrica Original)")
        urg_top10 = urgentes.head(10).reset_index(drop=True)
        st.dataframe(urg_top10, use_container_width=True)

    with tab_evolucao:
        st.subheader("Análise de Tendência e Evolução")
        
        st.plotly_chart(graf_tendencia_geral(evolucao_filtrada), use_container_width=True)
        
        st.markdown("---")
        st.subheader("Evolução Individual por Município")
        municipio_selecionado = st.selectbox(
            "Selecione o Município:",
            options=evolucao_filtrada["MUNICIPIO_NOME"].unique()
        )
        
        if municipio_selecionado:
            t = evolucao_filtrada[evolucao_filtrada["MUNICIPIO_NOME"] == municipio_selecionado].copy()
            t = t.dropna(subset=["ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"])
            
            if not t.empty:
                m = t.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean()
                melted = m.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
                fig = px.line(melted, x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                              title=f"{municipio_selecionado} — Evolução de Aprovação (Iniciais vs Finais)")
                fig.update_layout(yaxis_tickformat=".1f", yaxis_range=[60, 100])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"Dados insuficientes para mostrar a evolução de '{municipio_selecionado}'.")

    with tab_tables:
        st.subheader("Tabelas de Dados Brutos (Recorte)")
        
        st.markdown("**df_static (Score de Risco e Médias Estáticas)**")
        st.dataframe(df_static_ready.sort_values("SCORE_RISCO", ascending=False), use_container_width=True)
        
        st.markdown("**evolucao_filtrada (Série Histórica Longa)**")
        st.dataframe(evolucao_filtrada, use_container_width=True)
        
        st.markdown("**urgentes (Top 20 por Urgência)**")
        st.dataframe(urgentes, use_container_width=True)
