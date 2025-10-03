# gpt.py
# App Streamlit único (sem SAIDA_DIR).
# Requer: pandas, numpy, plotly, streamlit, openpyxl, odfpy

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet._reader")

import re
import unicodedata
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================
# 0) CAMINHOS (relativos ao repo)
# ============================
ARQ_ALP      = "dados/Dados_alpa.xlsx"
ARQ_DTB      = "dados/dtb_municipios.ods"
ARQ_INICIAIS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS   = "dados/anos_finais.xlsx"
ARQ_EM       = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"

# ============================
# Utilitários de normalização
# ============================
def nrm(x: object) -> str:
    if pd.isna(x): return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def _nrm_header(x: object) -> str:
    s = nrm(x)
    s = re.sub(r"\s+", " ", s)
    return s

def chave_municipio(nome: str) -> str:
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n:
        n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    return n

def to7(s: pd.Series) -> pd.Series:
    return s.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str)
         .str.replace("%", "", regex=False)
         .str.replace(",", ".", regex=False),
        errors="coerce"
    )

def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return (s - s.min())/(s.max()-s.min()) if s.max()!=s.min() else pd.Series(0.5, index=s.index)

# ============================
# 1) DTB/IBGE — robusto
# ============================
def carrega_dtb(path: str) -> pd.DataFrame:
    """
    Procura automaticamente a aba e a linha do cabeçalho corretas no .ods da DTB.
    Aceita Nome_UF (nome por extenso) ou UF (sigla).
    """
    xls = pd.ExcelFile(path, engine="odf")
    header_row, sheet_ok = None, None

    wants_cod = {
        "CODIGO MUNICIPIO COMPLETO","CÓDIGO MUNICIPIO COMPLETO",
        "CODIGO DO MUNICIPIO","CODIGO MUNICIPIO","COD MUNICIPIO","CÓDIGO DO MUNICÍPIO"
    }
    wants_nome_mun = {"NOME MUNICIPIO","NOME DO MUNICIPIO","NOME_MUNICIPIO","MUNICIPIO","MUNICÍPIO"}
    wants_nome_uf = {"NOME UF","NOME_UF","UF NOME","UF - NOME","NOME DA UF"}
    wants_uf_sigla = {"UF","SIGLA UF","SIGLA"}

    def _has_any(cands:set[str], rowvals:list[str]) -> bool:
        r = set(rowvals)
        return any(c in r for c in cands)

    # achar header
    for sh in xls.sheet_names:
        peek = pd.read_excel(path, engine="odf", sheet_name=sh, header=None, nrows=150)
        for i, row in peek.iterrows():
            vals = [_nrm_header(x) for x in row.tolist()]
            if _has_any(wants_cod, vals) and _has_any(wants_nome_mun, vals) and (_has_any(wants_nome_uf, vals) or _has_any(wants_uf_sigla, vals)):
                header_row, sheet_ok = i, sh
                break
        if sheet_ok:
            break

    if sheet_ok is None:
        raise KeyError(f"DTB: não encontrei um cabeçalho válido. Abas: {xls.sheet_names}")

    raw = pd.read_excel(path, engine="odf", sheet_name=sheet_ok, header=header_row)
    norm_map = {_nrm_header(c): c for c in raw.columns}

    def pick(cands:set[str]) -> str | None:
        for cand in cands:
            key = _nrm_header(cand)
            if key in norm_map:
                return norm_map[key]
            # começa com
            pref = [norm_map[k] for k in norm_map if k.startswith(key)]
            if pref:
                return pref[0]
        return None

    c_cod = pick(wants_cod)
    c_nmu = pick(wants_nome_mun)
    c_nuf = pick(wants_nome_uf)
    c_uf  = pick(wants_uf_sigla)

    if c_cod is None or c_nmu is None:
        raise KeyError(f"DTB: faltam colunas essenciais (código/nome). Colunas: {list(raw.columns)}")

    dtb = raw[[c_cod, c_nmu] + [c for c in [c_nuf, c_uf] if c is not None]].copy()
    dtb.columns = ["MUNICIPIO_CODIGO", "MUNICIPIO_NOME"] + (["UF_NOME"] if c_nuf else []) + (["UF"] if c_uf else [])
    dtb = dtb.dropna(subset=["MUNICIPIO_CODIGO", "MUNICIPIO_NOME"])

    dtb["MUNICIPIO_CODIGO"] = to7(dtb["MUNICIPIO_CODIGO"])
    dtb["MUNICIPIO_NOME"]   = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]  = dtb["MUNICIPIO_NOME"].apply(chave_municipio)

    UF_SIGLAS = {
        "ACRE":"AC","ALAGOAS":"AL","AMAPA":"AP","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA",
        "CEARA":"CE","CEARÁ":"CE","DISTRITO FEDERAL":"DF","ESPIRITO SANTO":"ES","ESPÍRITO SANTO":"ES",
        "GOIAS":"GO","GOIÁS":"GO","MARANHAO":"MA","MARANHÃO":"MA","MATO GROSSO":"MT",
        "MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG","PARA":"PA","PARÁ":"PA","PARAIBA":"PB",
        "PARAÍBA":"PB","PARANA":"PR","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUI":"PI","PIAUÍ":"PI",
        "RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS",
        "RONDONIA":"RO","RONDÔNIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SAO PAULO":"SP","SÃO PAULO":"SP",
        "SERGIPE":"SE","TOCANTINS":"TO"
    }

    if "UF_NOME" in dtb.columns:
        dtb["UF_SIGLA"] = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    elif "UF" in dtb.columns:
        dtb["UF_SIGLA"] = dtb["UF"].astype(str).str.upper().str.strip()
    else:
        dtb["UF_SIGLA"] = pd.NA

    if dtb["UF_SIGLA"].isna().all():
        raise ValueError("DTB: não consegui derivar UF_SIGLA (nem Nome_UF nem UF).")

    return dtb[["UF_SIGLA", "MUNICIPIO_CODIGO", "MUNICIPIO_NOME", "MUNICIPIO_CHAVE"]]

# ============================
# 2) Alpargatas — robusto
# ============================
def acha_header_municipio_uf(df_no_header: pd.DataFrame) -> int | None:
    MUN_TOKENS = {"CIDADE","CIDADES","MUNICIPIO","MUNICÍPIO","MUNICIPIOS","MUNICÍPIOS"}
    UF_TOKENS  = {"UF","SIGLA UF","SIGLA","ESTADO"}
    # 1ª passada: tokens diretos
    for i, row in df_no_header.iterrows():
        vals = [_nrm_header(x) for x in row.tolist()]
        has_mun = any(v in MUN_TOKENS or v.startswith(("CIDA","MUNICIP")) for v in vals)
        has_uf  = any(v in UF_TOKENS  or v.startswith("UF") for v in vals)
        if has_mun and has_uf:
            return i
    # fallback
    for i, row in df_no_header.iterrows():
        vals = [_nrm_header(x) for x in row.tolist()]
        if any(v == "UF" for v in vals) and any(v.startswith(("MUNIC","CIDA")) for v in vals):
            return i
    return None

def carrega_alpargatas(path: str) -> pd.DataFrame:
    """
    Lê o arquivo Dados_alpa.xlsx, procurando abas de 2020–2025,
    e coleta a lista de municípios (sem UF explícito).
    Assume que todos pertencem ao estado 'PB'.
    """
    xls = pd.ExcelFile(path, engine="openpyxl")
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        raise RuntimeError("Nenhuma aba 2020–2025 encontrada no arquivo Alpargatas.")
    
    frames = []
    for aba in abas:
        # força header na linha 6 (onde está 'CIDADES')
        df = pd.read_excel(path, sheet_name=aba, header=5, engine="openpyxl")
        
        # checa se a coluna de cidades existe
        c_cid = next((c for c in df.columns if "CIDADE" in str(c).upper()), None)
        if not c_cid:
            st.warning(f"Aba '{aba}': coluna 'CIDADES' não encontrada. Pulando…")
            continue
        
        tmp = df[[c_cid]].copy().rename(columns={c_cid: "MUNICIPIO_NOME_ALP"})
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
        tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP"])
        tmp = tmp[tmp["MUNICIPIO_NOME_ALP"].str.len() > 0]
        
        # adiciona UF padrão (PB, já que o arquivo não traz)
        tmp["UF_SIGLA"] = "PB"
        
        tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        tmp["FONTE_ABA"] = aba
        frames.append(tmp)
    
    if not frames:
        raise RuntimeError("Nenhuma aba válida foi processada no arquivo Alpargatas.")
    
    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE", "UF_SIGLA"])

# ============================
# 3) Acesso aos arquivos de aprovação (auto header)
# ============================
def carrega_aprovacao(path: str) -> pd.DataFrame:
    """
    Detecta automaticamente a linha do cabeçalho (onde aparece CO_MUNICIPIO)
    e devolve o DataFrame.
    """
    preview = pd.read_excel(path, header=None, nrows=30, engine="openpyxl")
    header_row = None
    for i, row in preview.iterrows():
        if any(str(x).strip().upper() == "CO_MUNICIPIO" for x in row):
            header_row = i; break
    if header_row is None:
        raise RuntimeError(f"Não encontrei 'CO_MUNICIPIO' em {path}")
    return pd.read_excel(path, header=header_row, engine="openpyxl")

def media_por_municipio(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "CO_MUNICIPIO": to7(df["CO_MUNICIPIO"]),
        rotulo: pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce"),
    })
    return out.groupby("CO_MUNICIPIO", as_index=False)[rotulo].mean()

def _anos_disponiveis(df: pd.DataFrame, a0=2005, a1=2023):
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m:
            ano = int(m.group(1))
            if a0 <= ano <= a1: anos.append(ano)
    return sorted(set(anos))

def _long_por_municipio_ano(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    t = df.copy()
    t["CO_MUNICIPIO"] = to7(t["CO_MUNICIPIO"])
    anos = _anos_disponiveis(t, 2005, 2023)
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    for c in cols: t[c] = pd.to_numeric(t[c], errors="coerce")
    long_ = t[["CO_MUNICIPIO"] + cols].melt("CO_MUNICIPIO", value_name=rotulo)
    long_["ANO"] = long_["variable"].str.extract(r"(\d{4})").astype(int)
    long_.drop(columns="variable", inplace=True)
    return long_.groupby(["CO_MUNICIPIO","ANO"], as_index=False)[rotulo].mean()

# ============================
# 4) Evasão — lê, filtra e renomeia CATFUN/CATMED
# ============================
def carrega_evasao_filtrado(path: str) -> pd.DataFrame:
    preview = pd.read_excel(path, header=None, nrows=30, engine="odf")
    header_row = None
    for i, row in preview.iterrows():
        if any(str(x).strip().upper() == "CO_MUNICIPIO" for x in row):
            header_row = i; break
    if header_row is None:
        raise RuntimeError(f"Não encontrei cabeçalho com 'CO_MUNICIPIO' em {path}")

    df = pd.read_excel(path, header=header_row, engine="odf")

    colunas_desejadas = [
        "NO_REGIAO","NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA",
        "1_CAT3_CATFUN","1_CAT3_CATFUN_AI","1_CAT3_CATFUN_AF",
        "1_CAT3_CATFUN_01","1_CAT3_CATFUN_02","1_CAT3_CATFUN_03","1_CAT3_CATFUN_04",
        "1_CAT3_CATFUN_05","1_CAT3_CATFUN_06","1_CAT3_CATFUN_07","1_CAT3_CATFUN_08","1_CAT3_CATFUN_09",
        "1_CAT3_CATMED","1_CAT3_CATMED_01","1_CAT3_CATMED_02","1_CAT3_CATMED_03",
    ]
    existentes = [c for c in colunas_desejadas if c in df.columns]
    df_filtrado = df[existentes].copy()

    mapa_colunas = {
        "1_CAT3_CATFUN": "Fundamental - Total",
        "1_CAT3_CATFUN_AI": "Fundamental - Anos Iniciais",
        "1_CAT3_CATFUN_AF": "Fundamental - Anos Finais",
        "1_CAT3_CATFUN_01": "Fundamental - 1º Ano",
        "1_CAT3_CATFUN_02": "Fundamental - 2º Ano",
        "1_CAT3_CATFUN_03": "Fundamental - 3º Ano",
        "1_CAT3_CATFUN_04": "Fundamental - 4º Ano",
        "1_CAT3_CATFUN_05": "Fundamental - 5º Ano",
        "1_CAT3_CATFUN_06": "Fundamental - 6º Ano",
        "1_CAT3_CATFUN_07": "Fundamental - 7º Ano",
        "1_CAT3_CATFUN_08": "Fundamental - 8º Ano",
        "1_CAT3_CATFUN_09": "Fundamental - 9º Ano",
        "1_CAT3_CATMED": "Médio - Total",
        "1_CAT3_CATMED_01": "Médio - 1ª série",
        "1_CAT3_CATMED_02": "Médio - 2ª série",
        "1_CAT3_CATMED_03": "Médio - 3ª série",
    }
    df_filtrado = df_filtrado.rename(columns={k:v for k,v in mapa_colunas.items() if k in df_filtrado.columns})

    if "CO_MUNICIPIO" in df_filtrado.columns:
        df_filtrado["CO_MUNICIPIO"] = to7(df_filtrado["CO_MUNICIPIO"])

    for col in ["Fundamental - Total", "Médio - Total"]:
        if col in df_filtrado.columns:
            df_filtrado[col] = _num(df_filtrado[col])

    if "Fundamental - Total" in df_filtrado.columns:
        df_filtrado["EVASAO_FUNDAMENTAL"] = df_filtrado["Fundamental - Total"]
    if "Médio - Total" in df_filtrado.columns:
        df_filtrado["EVASAO_MEDIO"] = df_filtrado["Médio - Total"]

    return df_filtrado

# ============================
# 5) Cruzamento básico
# ============================
def cruzar(dtb: pd.DataFrame, alpa: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    codificados = alpa.merge(
        dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE")
    )
    nao_encontrados = (codificados[codificados["MUNICIPIO_CODIGO"].isna()]
                       .drop_duplicates(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
                       .sort_values(["UF_SIGLA","MUNICIPIO_NOME_ALP"]))
    return codificados, nao_encontrados

# ============================
# 6) Pipeline principal (cacheado)
# ============================
@st.cache_data(show_spinner=True)
def build_data():
    # DTB + Alpargatas
    dtb = carrega_dtb(ARQ_DTB)
    alpa = carrega_alpargatas(ARQ_ALP)
    codificados, _ = cruzar(dtb, alpa)

    # hotfix Campina Grande (se faltar)
    mask = (codificados["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & \
           (codificados["UF_SIGLA"]=="PB") & (codificados["MUNICIPIO_CODIGO"].isna())
    codificados.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"

    # Aprovação (INEP) — header auto
    df_ini = carrega_aprovacao(ARQ_INICIAIS)
    df_fin = carrega_aprovacao(ARQ_FINAIS)
    df_med = carrega_aprovacao(ARQ_EM)

    ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    res = codificados.copy()
    res["MUNICIPIO_CODIGO"] = to7(res["MUNICIPIO_CODIGO"])
    res = (res
           .merge(ini, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
           .merge(fin, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("","_fin"))
           .merge(med, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("","_med")))
    for c in ["CO_MUNICIPIO","CO_MUNICIPIO_fin","CO_MUNICIPIO_med"]:
        if c in res.columns: res.drop(columns=c, inplace=True)

    # Evasão — CATFUN/CATMED já tratados
    eva = carrega_evasao_filtrado(ARQ_EVASAO)

    # Merge + urgência
    res2 = res.merge(eva, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
    res2["Reprovacao_Iniciais"] = (1 - pd.to_numeric(res2["TAXA_APROVACAO_INICIAIS"], errors="coerce")) * 100
    res2["Reprovacao_Finais"]   = (1 - pd.to_numeric(res2["TAXA_APROVACAO_FINAIS"],  errors="coerce")) * 100
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]:
        res2[c] = _num(res2[c])
    res2["Urgencia"] = res2[["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1, skipna=True)

    urgentes = res2.sort_values("Urgencia", ascending=False).head(20)

    # Evolução 2005–2023 (para os urgentes)
    evo_ini = _long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_med, "APROVACAO_MEDIO")
    evolucao = (evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                       .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
        ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)

    # Anexar UF + nome e filtrar pelos urgentes
    dtb_lookup = dtb[["MUNICIPIO_CODIGO","UF_SIGLA","MUNICIPIO_NOME"]].rename(columns={"MUNICIPIO_CODIGO":"CO_MUNICIPIO"})
    dtb_lookup["CO_MUNICIPIO"] = to7(dtb_lookup["CO_MUNICIPIO"])
    evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")
    evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)

    urg_ck = urgentes.copy()
    base_nome = urg_ck["MUNICIPIO_NOME_ALP"] if "MUNICIPIO_NOME_ALP" in urg_ck.columns else urg_ck.get("NO_MUNICIPIO")
    urg_ck["MUNICIPIO_CHAVE"] = base_nome.apply(chave_municipio)

    evolucao_filtrada = evolucao.merge(
        urg_ck[["UF_SIGLA","MUNICIPIO_CHAVE"]].drop_duplicates(),
        on=["UF_SIGLA","MUNICIPIO_CHAVE"], how="inner"
    ).sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True)

    return res2, urgentes, evolucao_filtrada

# ============================
# 7) UI (Streamlit)
# ============================
st.set_page_config(page_title="IA • Aprovação/Evasão", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel (sem SAIDA_DIR)")

with st.spinner("Processando dados…"):
    base, urgentes, evolucao_filtrada = build_data()

# KPIs
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios (base)", f"{base['MUNICIPIO_CODIGO'].nunique()}")
with c2: st.metric("Aprovação — Finais (média)", f"{(pd.to_numeric(base['TAXA_APROVACAO_FINAIS'], errors='coerce').mean()*100):.1f}%")
with c3: st.metric("Evasão — Fundamental (média)", f"{base['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4: st.metric("Urgência — média", f"{base['Urgencia'].mean():.1f}")

tab1, tab2, tab3 = st.tabs(["🔎 Tabelas","📈 Gráficos","⚙️ Diagnóstico"])

with tab1:
    st.subheader("Urgentes (Top 20 por urgência)")
    cols_u = ["UF_SIGLA","MUNICIPIO_NOME_ALP","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
              "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"]
    cols_u = [c for c in cols_u if c in urgentes.columns]
    st.dataframe(urgentes[cols_u], use_container_width=True)

    st.subheader("Evolução (recorte dos urgentes)")
    show_cols = ["UF_SIGLA","MUNICIPIO_NOME","ANO",
                 "APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]
    show_cols = [c for c in show_cols if c in evolucao_filtrada.columns]
    st.dataframe(evolucao_filtrada[show_cols], use_container_width=True)

with tab2:
    st.subheader("Tendência geral — aprovação (média do recorte)")
    tmp = evolucao_filtrada.copy()
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        if c in tmp.columns:
            tmp[c] = pd.to_numeric(tmp[c], errors="coerce")*100
    if {"ANO","APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"}.issubset(tmp.columns):
        m = tmp.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean()
        fig = px.line(m.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
                      x="ANO", y="Aprovação (%)", color="Etapa", markers=True)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Gap — Iniciais − Finais (p.p.)")
    if {"ANO","APROVACAO_INICIAIS","APROVACAO_FINAIS"}.issubset(tmp.columns):
        gap = tmp.groupby("ANO")[["APROVACAO_INICIAIS","APROVACAO_FINAIS"]].mean()
        gap["GAP"] = gap["APROVACAO_INICIAIS"] - gap["APROVACAO_FINAIS"]
        st.plotly_chart(px.line(gap.reset_index(), x="ANO", y="GAP", markers=True), use_container_width=True)

with tab3:
    st.write("**Shapes**")
    st.write("base:", base.shape, "| urgentes:", urgentes.shape, "| evolucao_filtrada:", evolucao_filtrada.shape)
    st.write("Tipos (base):")
    st.code(str(base.dtypes))
