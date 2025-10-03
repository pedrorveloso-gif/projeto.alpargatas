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
ARQ_EVASAO = "dados/evasao.
       

# ======================================================================================
# ARQUIVO: app.py
# CONTEÚDO: Módulos de Análise + Streamlit (CLOUD-READY)
# ======================================================================================
import streamlit as st
import pandas as pd
import numpy as np
import re, unicodedata
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from pathlib import Path
import warnings

# Ignora warnings do Pandas (como SettingWithCopyWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=pd.core.common.SettingWithCopyWarning)

UF_SIGLAS = {
    "ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA","CEARÁ":"CE",
    "DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOIÁS":"GO","MARANHÃO":"MA",
    "MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG","PARÁ":"PA",
    "PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI","RIO DE JANEIRO":"RJ",
    "RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS","RONDÔNIA":"RO","RORAIMA":"RR",
    "SANTA CATARINA":"SC","SÃO PAULO":"SP","SERGIPE":"SE","TOCANTINS":"TO"
}
COLUNAS_EVASAO_MAP = {
    "NO_REGIAO": "NO_REGIAO", "NO_UF": "NO_UF", "CO_MUNICIPIO": "CO_MUNICIPIO",
    "NO_MUNICIPIO": "NO_MUNICIPIO", "NO_LOCALIZACAO": "NO_LOCALIZACAO",
    "NO_DEPENDENCIA": "NO_DEPENDENCIA", "1_CAT3_CATFUN": "Evasão - Fundamental",
    "1_CAT3_CATMED": "Evasão - Médio",
}
# =========================================================

# #########################################################
# 1. FUNÇÕES DE PROCESSAMENTO E CARREGAMENTO DE DADOS (Caixinha de Ferramentas)
# #########################################################
def nrm(txt: object) -> str:
    """Normaliza: remove acentos, vira CAIXA-ALTA e tira espaços. NaN -> ''."""
    if pd.isna(txt): return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    """Chave 'suave' para casamentos."""
    n = nrm(nome).replace("–", "-").replace("—", "-")
    n = n.split(" - ")[0].strip()
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf): n = n[: -len(suf)].strip()
    return n

def padroniza_codigo_ibge(series: pd.Series) -> pd.Series:
    """Extrai 7 dígitos e preenche com zeros à esquerda."""
    return series.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def to_num(s: pd.Series, replace_comma=True) -> pd.Series:
    """Converte para numérico, tratando strings de formato comum."""
    s_clean = s.astype(str).str.replace("%", "", regex=False)
    if replace_comma: s_clean = s_clean.str.replace(",", ".", regex=False)
    s_clean = s_clean.str.replace("−", "-", regex=False)
    return pd.to_numeric(s_clean, errors="coerce")

def _minmax(s: pd.Series) -> pd.Series:
    """Normaliza entre 0 e 1. Se max=min, retorna 0.5."""
    s = s.astype(float)
    if s.dropna().empty or s.max() == s.min():
        return pd.Series(0.5, index=s.index)
    return (s - s.min()) / (s.max() - s.min())

def carrega_dtb(path: Path) -> pd.DataFrame:
    """Lê DTB/IBGE e limpa colunas-chave."""
    # Usando 'odf' engine para .ods
    dtb_raw = pd.read_excel(path, engine="odf", skiprows=6)
    dtb = (dtb_raw.rename(columns={"UF": "UF_COD_NUM", "Nome_UF": "UF_NOME",
                            "Código Município Completo": "MUNICIPIO_CODIGO",
                            "Nome_Município": "MUNICIPIO_NOME"})
           [["UF_COD_NUM","UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
           .dropna(subset=["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]))
    dtb["UF_SIGLA"] = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"] = padroniza_codigo_ibge(dtb["MUNICIPIO_CODIGO"])
    dtb["MUNICIPIO_CHAVE"] = dtb["MUNICIPIO_NOME"].apply(chave_municipio)
    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

def media_por_municipio(df: pd.DataFrame, rotulo_saida: str) -> pd.DataFrame:
    """Calcula a MÉDIA do indicador 2023 por município."""
    df["CO_MUNICIPIO"] = padroniza_codigo_ibge(df["CO_MUNICIPIO"])
    ind = to_num(df.get("VL_INDICADOR_REND_2023")) # .get para segurança
    return pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo_saida: ind}).groupby("CO_MUNICIPIO", as_index=False)[rotulo_saida].mean()

def _long_por_municipio_ano(df: pd.DataFrame, etapa_rotulo: str) -> pd.DataFrame:
    """Converte wide (anos como colunas) para long (ano como linha)."""
    df = df.copy()
    if "CO_MUNICIPIO" not in df.columns: return pd.DataFrame()
    df["CO_MUNICIPIO"] = padroniza_codigo_ibge(df["CO_MUNICIPIO"])
    
    anos_cols = [c for c in df.columns if c.startswith("VL_INDICADOR_REND_")]
    if not anos_cols: return pd.DataFrame()

    num = df[["CO_MUNICIPIO"] + anos_cols].copy()
    for c in anos_cols: num[c] = to_num(num[c])

    long_df = num.melt(id_vars="CO_MUNICIPIO", value_vars=anos_cols, var_name="COL", value_name=etapa_rotulo)
    long_df["ANO"] = long_df["COL"].str.extract(r"(\d{4})").astype("Int64")
    return long_df.groupby(["CO_MUNICIPIO", "ANO"], as_index=False)[etapa_rotulo].mean()


# #########################################################
# 2. FUNÇÃO PRINCIPAL DE PROCESSAMENTO (Cached Resource)
# #########################################################

@st.cache_resource
def run_data_processing():
    """
    Executa toda a lógica pesada de ETL e Geração de Urgência/Evolução.
    É executada uma única vez, a menos que os arquivos de dados mudem.
    """
    if not ARQ_ALP.exists() or not ARQ_DTB.exists():
         raise FileNotFoundError("Arquivos de dados não encontrados. Verifique a pasta 'data'.")

    # --- 1. Carregamento e Cruzamento Base ---
    dtb = carrega_dtb(ARQ_DTB)
    dtb_lookup = dtb[["MUNICIPIO_CODIGO", "UF_SIGLA", "MUNICIPIO_NOME"]].rename(columns={"MUNICIPIO_CODIGO": "CO_MUNICIPIO"})
    dtb_lookup["CO_MUNICIPIO"] = padroniza_codigo_ibge(dtb_lookup["CO_MUNICIPIO"])

    alpa = pd.DataFrame()
    try:
        # Simplifica o carregamento do Alpargatas (o código otimizado era muito longo)
        alp_raw = pd.read_excel(ARQ_ALP, sheet_name=None)
        abas = [a for a in alp_raw if any(str(ano) in a for ano in range(2020, 2026))]
        frames = []
        for aba in abas:
             # Heurística simples: tenta ler com o header da linha 5
            try:
                df = pd.read_excel(ARQ_ALP, sheet_name=aba, header=5)
                # Tenta padronizar colunas
                cmap = {nrm(c): c for c in df.columns}
                if "CIDADES" in cmap and "UF" in cmap:
                     tmp = df.rename(columns={cmap["CIDADES"]:"MUNICIPIO_NOME_ALP", cmap["UF"]:"UF_SIGLA"})
                     tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
                     frames.append(tmp[["MUNICIPIO_NOME_ALP", "UF_SIGLA", "MUNICIPIO_CHAVE"]])
            except Exception: pass
        if frames: alpa = pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])
        
    except Exception as e:
        st.error(f"Erro ao carregar Alpargatas: {e}. Verifique se o arquivo é válido.")
        return pd.DataFrame(), pd.DataFrame()


    codificados = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE"))
    
    # Ajuste manual (Campina Grande)
    codificados["MUNICIPIO_CODIGO"] = padroniza_codigo_ibge(codificados.get("MUNICIPIO_CODIGO"))
    mask_cg = (codificados["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & (codificados["UF_SIGLA"] == "PB") & (codificados["MUNICIPIO_CODIGO"].isna())
    codificados.loc[mask_cg, "MUNICIPIO_CODIGO"] = "2504009"
    
    # --- 2. Aprovação 2023 ---
    df_iniciais = pd.read_excel(ARQ_INICIAIS, header=9)
    df_finais = pd.read_excel(ARQ_FINAIS, header=9)
    df_em = pd.read_excel(ARQ_EM, header=9)

    ini = media_por_municipio(df_iniciais, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_finais, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_em, "TAXA_APROVACAO_MEDIO")

    res = codificados.copy()
    res["MUNICIPIO_CODIGO"] = padroniza_codigo_ibge(res.get("MUNICIPIO_CODIGO"))
    res = (res.merge(ini, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
            .merge(fin, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_fin"))
            .merge(med, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_med"))
            .drop(columns=[c for c in ["CO_MUNICIPIO", "CO_MUNICIPIO_fin", "CO_MUNICIPIO_med", "MUNICIPIO_NOME_IBGE"] if c in res.columns], errors="ignore")
    )
    
    # --- 3. Evasão e Urgência ---
    df_evasao_raw = pd.read_excel(ARQ_EVASAO, header=8)
    df_filtrado = df_evasao_raw.rename(columns=COLUNAS_EVASAO_MAP)
    for col in ["Evasão - Fundamental", "Evasão - Médio"]:
        if col in df_filtrado.columns: df_filtrado[col] = to_num(df_filtrado[col], replace_comma=True)

    res["MUNICIPIO_CODIGO"] = to_num(res["MUNICIPIO_CODIGO"]).astype("Int64")
    df_filtrado["CO_MUNICIPIO"] = to_num(df_filtrado.get("CO_MUNICIPIO")).astype("Int64")
    df_merge = pd.merge(res.dropna(subset=["MUNICIPIO_CODIGO"]), df_filtrado.dropna(subset=["CO_MUNICIPIO"]), left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="inner")
    
    resultado_num = df_merge.copy()
    num_cols = ["Evasão - Fundamental", "Evasão - Médio", "TAXA_APROVACAO_INICIAIS", "TAXA_APROVACAO_FINAIS"]
    for col in num_cols: resultado_num[col] = to_num(resultado_num[col], replace_comma=False)

    # Winsorização
    Q1, Q3 = resultado_num[num_cols].quantile(0.25, numeric_only=True), resultado_num[num_cols].quantile(0.75, numeric_only=True)
    IQR = Q3 - Q1
    low, high = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
    winsor_df = resultado_num.copy()
    for col in num_cols: winsor_df[col] = winsor_df[col].clip(lower=low[col], upper=high[col])

    winsor_df["Reprovacao_Iniciais"] = (1 - winsor_df["TAXA_APROVACAO_INICIAIS"]) * 100
    winsor_df["Reprovacao_Finais"] = (1 - winsor_df["TAXA_APROVACAO_FINAIS"]) * 100
    winsor_df["Urgencia"] = winsor_df["Evasão - Fundamental"] + winsor_df["Evasão - Médio"] + winsor_df["Reprovacao_Iniciais"] + winsor_df["Reprovacao_Finais"]
    urgentes = winsor_df.sort_values("Urgencia", ascending=False)
    
    # --- 4. Evolução Temporal ---
    evo_ini = _long_por_municipio_ano(df_iniciais, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_finais, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_em, "APROVACAO_MEDIO")
    evolucao = evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer").merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer")
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[[c for c in evolucao.columns if c.startswith("APROVACAO_")]].mean(axis=1, skipna=True)
    
    for c in evolucao.columns.intersection(["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]):
        evolucao[c + "_%"] = (evolucao[c] * 100).round(2)
        
    evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")
    evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)
    
    # Filtra: só os municípios que entraram no Urgentes
    urgentes["MUNICIPIO_CHAVE"] = (urgentes.get("MUNICIPIO_NOME_ALP").where(urgentes.get("MUNICIPIO_NOME_ALP").notna(), urgentes.get("NO_MUNICIPIO"))).apply(chave_municipio)
    evolucao_filtrada = evolucao.merge(urgentes[["UF_SIGLA","MUNICIPIO_CHAVE"]].drop_duplicates(), on=["UF_SIGLA","MUNICIPIO_CHAVE"], how="inner")
    
    # Preenche NaNs com mediana histórica (para gráficos)
    cols_num_evo = [c for c in evolucao_filtrada.columns if c.startswith("APROVACAO_") and c.endswith("_%")]
    for col in cols_num_evo: 
        evolucao_filtrada[col] = evolucao_filtrada.groupby("MUNICIPIO_CHAVE")[col].transform(lambda x: x.fillna(x.median(skipna=True)))

    return evolucao_filtrada, urgentes


# #########################################################
# 3. VARIÁVEIS E FUNÇÕES DE LAYOUT (Cached Data)
# #########################################################
# 1. Executa o processamento (uma vez)
try:
    with st.spinner('Carregando e processando dados...'):
        evolucao_filtrada, urgentes = run_data_processing()
        evo_safe = evolucao_filtrada
        urg_safe = urgentes
except FileNotFoundError as e:
    st.error(f"Erro Crítico: {e}. Verifique se a pasta 'data' e os arquivos estão corretos no repositório.")
    evo_safe = pd.DataFrame()
    urg_safe = pd.DataFrame()
except Exception as e:
    st.error(f"Erro desconhecido durante o processamento de dados: {e}")
    evo_safe = pd.DataFrame()
    urg_safe = pd.DataFrame()


# 2. Constrói o DF estático para cálculo do Score de Risco
@st.cache_data(show_spinner=False)
def build_static_df(evo: pd.DataFrame, urg: pd.DataFrame) -> pd.DataFrame:
    """Calcula o DF estático de métricas médias e Score de Risco."""
    if evo.empty or urg.empty: return pd.DataFrame()
    
    # Pré-limpeza e padronização de nomes
    t = evo.copy().rename(columns={"MUNICIPIO_NOME_ALP": "MUNICIPIO_NOME", "NO_MUNICIPIO": "MUNICIPIO_NOME"})
    if "MUNICIPIO_NOME" not in t.columns: t["MUNICIPIO_NOME"] = t.get("MUNICIPIO_NOME_IBGE", "Desconhecido")
    t["MUNICIPIO_NOME"] = t["MUNICIPIO_NOME"].astype(str).str.strip()

    # Média por município
    base_static = t.groupby("MUNICIPIO_NOME", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean(numeric_only=True)
    base_static["CHAVE"] = base_static["MUNICIPIO_NOME"].apply(lambda x: re.sub(r"[^a-z0-9]+", "_", unicodedata.normalize("NFKD", str(x)).encode("ASCII","ignore").decode("ASCII").lower()).strip("_"))
    
    # Anexar Evasão
    urg_clean = urg.copy()
    col_nome_urg = next((c for c in ["MUNICIPIO_NOME","MUNICIPIO_NOME_ALP","NO_MUNICIPIO"] if c in urg_clean.columns), "NO_MUNICIPIO")
    urg_clean = urg_clean.rename(columns={col_nome_urg:"MUNICIPIO_NOME"})
    urg_clean["MUNICIPIO_NOME"] = urg_clean["MUNICIPIO_NOME"].astype(str).str.strip()
    urg_clean["CHAVE"] = urg_clean["MUNICIPIO_NOME"].apply(lambda x: re.sub(r"[^a-z0-9]+", "_", unicodedata.normalize("NFKD", str(x)).encode("ASCII","ignore").decode("ASCII").lower()).strip("_"))
    
    df = base_static.merge(urg_clean[["CHAVE","Evasão - Fundamental"]].groupby("CHAVE").mean(numeric_only=True).reset_index(), on="CHAVE", how="left").drop(columns=["CHAVE"])
    df.rename(columns={"Evasão - Fundamental": "EVASAO_FUNDAMENTAL"}, inplace=True)

    # Cálculo do Score de Risco
    df["EVASAO_FUNDAMENTAL"] = to_num(df.get("EVASAO_FUNDAMENTAL"))
    df["GAP_APROV_%"] = df.get("APROVACAO_INICIAIS_%", 0) - df.get("APROVACAO_FINAIS_%", 0)
    
    aprov_finais_norm = 1 - _minmax(df["APROVACAO_FINAIS_%"])
    evasao_norm = _minmax(df["EVASAO_FUNDAMENTAL"])
    gap_norm = _minmax(df["GAP_APROV_%"].fillna(0))
    
    df["SCORE_RISCO"] = 0.5 * aprov_finais_norm + 0.4 * evasao_norm + 0.1 * gap_norm
    return df

df_static_ready = build_static_df(evo_safe, urg_safe)


# 3. Funções de Plotagem (Chamadas no Layout)
def graf_ranking_risco(base: pd.DataFrame, top_n=15) -> px.bar:
    """Ranking de Score de Risco (Plotly)."""
    t = base.dropna(subset=["SCORE_RISCO"]).sort_values("SCORE_RISCO", ascending=False).head(top_n)
    if t.empty: return go.Figure().update_layout(title="Dados insuficientes para Ranking de Risco.")
    fig = px.bar(t, x="SCORE_RISCO", y="MUNICIPIO_NOME", orientation="h",
                 hover_data=["APROVACAO_INICIAIS_%", "APROVACAO_FINAIS_%", "EVASAO_FUNDAMENTAL", "GAP_APROV_%"],
                 title=f"Top {top_n} — Ranking de Risco (Score 0-1)",
                 labels={"MUNICIPIO_NOME": "Município", "SCORE_RISCO": "Score de Risco"})
    return fig.update_yaxes(categoryorder="total ascending")

def graf_tendencia_municipio(municipio_nome: str, evo: pd.DataFrame) -> px.line:
    """Evolução de Aprovação para um município específico (Plotly)."""
    t = evo[evo["MUNICIPIO_NOME"].astype(str).str.strip() == str(municipio_nome).strip()].copy()
    t = t.dropna(subset=["ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"])
    if t.empty: return None

    m = t.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean(numeric_only=True)
    melted = m.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
    
    fig = px.line(melted, x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                  title=f"**{municipio_nome}** — Evolução de Aprovação (Iniciais vs Finais)")
    return fig.update_layout(yaxis_tickformat=".1f")


# #########################################################
# 4. LAYOUT STREAMLIT
# #########################################################
st.set_page_config(page_title="IA • Aprovação, Evasão e Urgência", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel de Urgência Educacional")

# --- Indicador de Status ---
if df_static_ready.empty:
    st.error("Não foi possível carregar ou processar os dados. Verifique a pasta `/data` e os logs de erro.")
else:
    # ----------------- KPIs -----------------
    c1,c2,c3,c4 = st.columns(4)
    with c1: st.metric("Municípios no recorte", len(df_static_ready["MUNICIPIO_NOME"].unique()))
    with c2: st.metric("Aprovação — Finais (média)", f"{df_static_ready['APROVACAO_FINAIS_%'].mean():.1f}%")
    with c3: st.metric("Evasão — Fundamental (média)", f"{df_static_ready['EVASAO_FUNDAMENTAL'].mean():.1f}%")
    with c4: st.metric("Score de risco (média)", f"{df_static_ready['SCORE_RISCO'].mean():.2f}")
    st.divider()

    # ----------------- Abas -----------------
    tab_overview, tab_grafs, tab_detalhe, tab_diag = st.tabs(["Visão Geral","Análise Gráfica","Detalhe Município", "Tabelas/Diagnóstico"])

    # ---- Visão Geral ----
    with tab_overview:
        st.subheader("📍 Ranking de Urgência e Priorização")
        colA, colB = st.columns([1, 2])
        
        with colA:
            st.markdown("**Top 10 Cidades com Maior Score de Risco**")
            st.dataframe(df_static_ready[["MUNICIPIO_NOME","SCORE_RISCO","EVASAO_FUNDAMENTAL","APROVACAO_FINAIS_%"]]
                         .sort_values("SCORE_RISCO", ascending=False).head(10).round(2),
                         use_container_width=True, hide_index=True)

        with colB:
            st.markdown("**Ranking de Risco (Score 0-1)**")
            st.plotly_chart(graf_ranking_risco(df_static_ready, top_n=15), use_container_width=True)

        st.subheader("📚 Metodologia e Desafios")
        st.markdown("""
        Este painel utiliza uma **métrica de Urgência** focada em:
        1.  **Baixa Taxa de Aprovação** nos Anos Finais (peso 50%).
        2.  **Alta Taxa de Evasão** no Ensino Fundamental (peso 40%).
        3.  **Maior GAP** entre a aprovação dos Anos Iniciais e Finais (gargalo, peso 10%).

        O índice **Score de Risco** (0-1) consolida esses fatores para priorizar ações estratégicas.
        """)

    # ---- Análise Gráfica ----
    with tab_grafs:
        st.subheader("📈 Tendência Geral e Gargalos")

        # Gráfico de Tendência Geral (Evolução Iniciais vs Finais)
        tmp = evo_safe.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean(numeric_only=True)
        fig_geral = px.line(tmp.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
                            x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                            title="Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)")
        st.plotly_chart(fig_geral, use_container_width=True)

        # Gráfico de diferença (GAP)
        gap = evo_safe.groupby("ANO")[["APROVACAO_INICIAIS", "APROVACAO_FINAIS", "APROVACAO_MEDIO"]].mean() * 100
        gap["queda_iniciais_finais"] = gap["APROVACAO_INICIAIS"] - gap["APROVACAO_FINAIS"]
        
        plt.figure(figsize=(10,5))
        gap[["queda_iniciais_finais"]].plot(ax=plt.gca(), marker="o")
        plt.title("Gargalo de Aprovação: Queda Iniciais para Finais (em p.p.)")
        plt.ylabel("Diferença percentual (p.p.)")
        plt.axhline(0, color="black", linestyle="--")
        st.pyplot(plt.gcf(), use_container_width=True) # Usa plt.gcf() para pegar a figura atual
        plt.close() # Fecha a figura para economizar memória

    # ---- Detalhe Município ----
    with tab_detalhe:
        st.subheader("Detalhe da Evolução Individual")
        municipios_validos = sorted(df_static_ready["MUNICIPIO_NOME"].dropna().unique())
        
        municipio_selecionado = st.selectbox(
            "Selecione um município para ver a evolução de Aprovação (Iniciais vs Finais):",
            municipios_validos,
            index=min(1, len(municipios_validos) - 1) if municipios_validos else None # Tenta o segundo ou o primeiro
        )
        if municipio_selecionado:
            fig_detalhe = graf_tendencia_municipio(municipio_selecionado, evo_safe)
            if fig_detalhe:
                st.plotly_chart(fig_detalhe, use_container_width=True)
            else:
                st.info(f"Dados de evolução insuficientes para plotar {municipio_selecionado}.")

    # ---- Tabelas/Diagnóstico ----
    with tab_diag:
        st.subheader("Tabelas Consolidadas")
        
        col_t1, col_t2 = st.columns(2)
        with col_t1:
            st.markdown("**df_static (Métricas Médias)**")
            st.dataframe(df_static_ready.sort_values("SCORE_RISCO", ascending=False).head(50).round(2), use_container_width=True)
            st.download_button("Baixar df_static.csv", df_static_ready.to_csv(index=False).encode("utf-8"), file_name="df_static.csv", use_container_width=True)
        
        with col_t2:
            st.markdown("**evolucao_filtrada (Série Temporal)**")
            prefer = ["UF_SIGLA","MUNICIPIO_NOME","ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIA_GERAL_%"]
            cols = [c for c in prefer if c in evo_safe.columns]
            st.dataframe(evo_safe[cols].head(50), use_container_width=True)
            st.download_button("Baixar evolucao.csv", evo_safe.to_csv(index=False).encode("utf-8"), file_name="evolucao.csv", use_container_width=True)
            
        st.subheader("Diagnóstico de Dados")
        st.info("O processamento de dados só é executado uma vez no Streamlit Cloud.")
        st.dataframe(pd.DataFrame({
            "Variável": ["df_static_ready", "evolucao_filtrada", "urgentes"],
            "Shape": [df_static_ready.shape, evo_safe.shape, urg_safe.shape],
            "Tamanho": [f"{df_static_ready.memory_usage(deep=True).sum() / 1024:.1f} KB", f"{evo_safe.memory_usage(deep=True).sum() / 1024:.1f} KB", f"{urg_safe.memory_usage(deep=True).sum() / 1024:.1f} KB"]
        }).set_index("Variável"))
