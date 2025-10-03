# gpt.py
# Painel Instituto Alpargatas — Municípios + Aprovação + Evasão + Urgência
# Foco em CIDADES/UF, sem partes de doações/sustentabilidade.

import pandas as pd
import unicodedata, re, warnings
import streamlit as st
import plotly.express as px

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet._reader")

# ============================
# 0) CAMINHOS (ajuste se precisar)
# ============================
ARQ_ALP      = "dados/Dados_alpa.xlsx"
ARQ_DTB      = "dados/dtb_municipios.ods"
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_EM       = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"

# ============================
# 1) Utilitários
# ============================
def nrm(txt: object) -> str:
    if pd.isna(txt): return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII","ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    n = nrm(nome).replace("–","-").replace("—","-")
    if " - " in n: n = n.split(" - ")[0]
    for suf in (" MIXING CENTER"," DISTRITO"," DISTRITO INDUSTRIAL"):
        if n.endswith(suf): n = n[: -len(suf)].strip()
    return n

def to7(s: pd.Series) -> pd.Series:
    return s.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace("%","",regex=False).str.replace(",",".",regex=False),
        errors="coerce"
    )

def acha_linha_header_cidades_uf(df_no_header: pd.DataFrame) -> int | None:
    """
    Procura a linha onde aparecem "CIDADES" e "UF" (após normalização) em qualquer célula.
    Retorna o índice da linha para ser usado como header no read_excel.
    """
    for i, row in df_no_header.iterrows():
        vals = [nrm(x) for x in row.tolist()]
        if "CIDADES" in vals and "UF" in vals:
            return i
    return None

# ============================
# 2) DTB / IBGE
# ============================
def carrega_dtb(path: str) -> pd.DataFrame:
    UF_SIGLAS = {
        "ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA",
        "CEARÁ":"CE","DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOIÁS":"GO",
        "MARANHÃO":"MA","MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG",
        "PARÁ":"PA","PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI",
        "RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS",
        "RONDÔNIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SÃO PAULO":"SP","SERGIPE":"SE","TOCANTINS":"TO"
    }
    raw = pd.read_excel(path, engine="odf", skiprows=6)

    # Mapas flexíveis de nomes
    norm_cols = {c: nrm(c) for c in raw.columns}
    def col_like(*opcoes):
        for orig, norm in norm_cols.items():
            if norm in opcoes: return orig
        return None

    c_ufnome = col_like("NOME_UF")
    c_cod    = col_like("CODIGO MUNICIPIO COMPLETO")
    c_nome   = col_like("NOME_MUNICIPIO")
    if not all([c_ufnome, c_cod, c_nome]):
        st.error("DTB: não encontrei colunas essenciais (Nome_UF, Código Município Completo, Nome_Município).")
        return pd.DataFrame(columns=["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"])

    dtb = raw.rename(columns={
        c_ufnome: "UF_NOME",
        c_cod:    "MUNICIPIO_CODIGO",
        c_nome:   "MUNICIPIO_NOME"
    })[["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]].dropna()

    dtb["UF_SIGLA"]         = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"] = to7(dtb["MUNICIPIO_CODIGO"])
    dtb["MUNICIPIO_NOME"]   = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]  = dtb["MUNICIPIO_NOME"].apply(chave_municipio)
    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

# ============================
# 3) Alpargatas — extrai CIDADES/UF (2020–2025)
# ============================
def carrega_alpargatas(path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        st.error("Dados_alpa.xlsx: não encontrei abas 2020–2025.")
        return pd.DataFrame(columns=["MUNICIPIO_NOME_ALP","UF_SIGLA","MUNICIPIO_CHAVE","FONTE_ABA"])

    frames = []
    for aba in abas:
        # 1) Tenta achar a linha do header varrendo as primeiras 400 linhas
        nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
        hdr = acha_linha_header_cidades_uf(nohdr)

        if hdr is not None:
            df = pd.read_excel(path, sheet_name=aba, header=hdr)
        else:
            # 2) fallback: lê normal e tenta achar colunas pelo nome
            df = pd.read_excel(path, sheet_name=aba)
            # se a primeira linha tem os títulos misturados nos dados, pula
            # (vamos apenas tentar localizar colunas por nome normalizado)
        
        # Descobre as colunas "CIDADES" e "UF" por nome normalizado
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm=="CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm=="UF"), None)

        if not c_cid or not c_uf:
            st.warning(f"[{aba}] Colunas CIDADES/UF não encontradas. Pulando…")
            continue

        tmp = (df[[c_cid, c_uf]].copy()
                 .rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"}))
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
        tmp["UF_SIGLA"]           = tmp["UF_SIGLA"].astype(str).str.strip()
        tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
        tmp = tmp[tmp["MUNICIPIO_NOME_ALP"].str.len()>0]
        tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        tmp["FONTE_ABA"] = aba
        frames.append(tmp)

    if not frames:
        st.error("Nenhuma aba com CIDADES/UF encontrada em Dados_alpa.xlsx.")
        return pd.DataFrame(columns=["MUNICIPIO_NOME_ALP","UF_SIGLA","MUNICIPIO_CHAVE","FONTE_ABA"])

    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])

# ============================
# 4) Aprovação (INEP)
# ============================
def media_por_municipio(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "CO_MUNICIPIO": to7(df["CO_MUNICIPIO"]),
        rotulo: pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce"),
    })
    return out.groupby("CO_MUNICIPIO", as_index=False)[rotulo].mean()

# ============================
# 5) Evasão (INEP)
# ============================
def carrega_evasao(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, header=8, engine="odf")
    df["CO_MUNICIPIO"] = to7(df["CO_MUNICIPIO"])
    cols_desejadas = [
        "NO_REGIAO","NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA",
        "1_CAT3_CATFUN","1_CAT3_CATFUN_AI","1_CAT3_CATFUN_AF",
        "1_CAT3_CATMED","1_CAT3_CATMED_01","1_CAT3_CATMED_02","1_CAT3_CATMED_03"
    ]
    cols = [c for c in cols_desejadas if c in df.columns]
    df = df[cols].copy()
    mapa = {
        "1_CAT3_CATFUN": "EVASAO_FUNDAMENTAL",
        "1_CAT3_CATFUN_AI": "EVASAO_FUN_AI",
        "1_CAT3_CATFUN_AF": "EVASAO_FUN_AF",
        "1_CAT3_CATMED": "EVASAO_MEDIO",
        "1_CAT3_CATMED_01": "EVASAO_MED_1",
        "1_CAT3_CATMED_02": "EVASAO_MED_2",
        "1_CAT3_CATMED_03": "EVASAO_MED_3",
    }
    df = df.rename(columns={k:v for k,v in mapa.items() if k in df.columns})
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","EVASAO_FUN_AI","EVASAO_FUN_AF","EVASAO_MED_1","EVASAO_MED_2","EVASAO_MED_3"]:
        if c in df.columns:
            df[c] = _num(df[c])
    return df

# ============================
# 6) Pipeline (cacheado)
# ============================
@st.cache_data(show_spinner=True)
def build_data():
    # Alpargatas (cidades alvo)
    alpa = carrega_alpargatas(ARQ_ALP)

    # Se não conseguiu ler nada, aborta cedo
    if alpa.empty:
        return pd.DataFrame(), pd.DataFrame()

    # DTB (lookup p/ código IBGE e nome oficial)
    dtb  = carrega_dtb(ARQ_DTB)

    base = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left")

    # Hotfix Campina Grande (se necessário)
    mask = (base["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & \
           (base["UF_SIGLA"]=="PB") & (base["MUNICIPIO_CODIGO"].isna())
    base.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"

    # Garante coluna para futuros merges (evita KeyError: 'CO_MUNICIPIO')
    base["CO_MUNICIPIO"] = to7(base["MUNICIPIO_CODIGO"])

    # Aprovação (médias 2023)
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
    eva  = carrega_evasao(ARQ_EVASAO)
    base = base.merge(eva, on="CO_MUNICIPIO", how="left")

    # Indicadores de reprovação e urgência
    base["Reprovacao_Iniciais"] = (1 - pd.to_numeric(base["TAXA_APROVACAO_INICIAIS"], errors="coerce")) * 100
    base["Reprovacao_Finais"]   = (1 - pd.to_numeric(base["TAXA_APROVACAO_FINAIS"],  errors="coerce")) * 100
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]:
        base[c] = _num(base[c])

    # Winsorização simples (cap em IQR)
    num_cols = ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]
    if base[num_cols].notna().sum().sum() > 0:
        q1 = base[num_cols].quantile(0.25, numeric_only=True)
        q3 = base[num_cols].quantile(0.75, numeric_only=True)
        iqr = q3 - q1
        low  = q1 - 1.5*iqr
        high = q3 + 1.5*iqr
        for c in num_cols:
            base[c] = base[c].clip(lower=low[c], upper=high[c])

    base["Urgencia"] = base[num_cols].sum(axis=1, skipna=True)

    # Top 20 por urgência
    urgentes = (base.sort_values("Urgencia", ascending=False)
                     .head(20)
                     .copy())

    return base, urgentes

# ============================
# 7) UI — Streamlit
# ============================
st.set_page_config(page_title="IA • Aprovação/Evasão", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel Municípios")

with st.spinner("Processando dados…"):
    base, urgentes = build_data()

# Se algo deu muito errado (arquivos/formato), mostra diagnóstico e para
if base.empty:
    st.error("Não consegui montar a base. Confira se as abas 2020–2025 de **Dados_alpa.xlsx** têm colunas **CIDADES** e **UF**, e se **dtb_municipios.ods** está no formato IBGE (DTB).")
    st.stop()

# Sidebar — filtro por UF
ufs = sorted([u for u in base["UF_SIGLA"].dropna().unique() if isinstance(u, str)])
sel_ufs = st.sidebar.multiselect("Filtrar por UF", options=ufs, default=ufs or None)
if not sel_ufs:
    sel_ufs = ufs
base_f = base[base["UF_SIGLA"].isin(sel_ufs)] if sel_ufs else base.copy()
urg_f  = urgentes[urgentes["UF_SIGLA"].isin(sel_ufs)] if sel_ufs else urgentes.copy()

# KPIs
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios (base)", f"{base_f['MUNICIPIO_CHAVE'].nunique()}")
with c2: st.metric("Aprovação — Finais (média)", f"{(pd.to_numeric(base_f['TAXA_APROVACAO_FINAIS'], errors='coerce').mean()*100):.1f}%")
with c3: st.metric("Evasão — Fundamental (média)", f"{base_f['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4: st.metric("Urgência — média", f"{base_f['Urgencia'].mean():.1f}")

tab1, tab2, tab3 = st.tabs(["🔎 Tabelas","📈 Visões rápidas","⚙️ Diagnóstico"])

with tab1:
    st.subheader("Top 20 por Urgência (filtrado)")
    cols_urg = [c for c in [
        "UF_SIGLA","MUNICIPIO_NOME_ALP","MUNICIPIO_NOME","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
        "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"
    ] if c in urg_f.columns]
    st.dataframe(urg_f[cols_urg], use_container_width=True)

    st.subheader("Base consolidada (amostra filtrada)")
    st.dataframe(base_f.head(50), use_container_width=True)

with tab2:
    st.subheader("Distribuição — Urgência (filtrado)")
    st.plotly_chart(px.histogram(base_f, x="Urgencia"), use_container_width=True)

    st.subheader("Aprovação — Iniciais vs Finais (pontos)")
    tmp = base_f.copy()
    tmp["INI_%"] = pd.to_numeric(tmp["TAXA_APROVACAO_INICIAIS"], errors="coerce")*100
    tmp["FIN_%"] = pd.to_numeric(tmp["TAXA_APROVACAO_FINAIS"],  errors="coerce")*100
    st.plotly_chart(
        px.scatter(tmp, x="INI_%", y="FIN_%", hover_name="MUNICIPIO_NOME_ALP", color="UF_SIGLA"),
        use_container_width=True
    )

with tab3:
    st.write("**Shapes**")
    st.write("base:", base.shape, "| urgentes:", urgentes.shape)
    st.write("Tipos (base):")
    st.code(str(base.dtypes))

    st.write("**Colunas disponíveis em `Dados_alpa.xlsx` (debug rápido):**")
    try:
        import pandas as _pd
        _xls = _pd.ExcelFile(ARQ_ALP)
        st.write({aba: list(_pd.read_excel(ARQ_ALP, sheet_name=aba, nrows=3).columns) for aba in _xls.sheet_names})
    except Exception as e:
        st.write(f"Não foi possível inspecionar colunas do Alpargatas: {e}")
