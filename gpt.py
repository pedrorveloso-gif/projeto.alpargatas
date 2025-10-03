# gpt.py
# Painel Instituto Alpargatas — Municípios + Aprovação + Evasão + Urgência
# Agora com filtro por UF (sidebar) e downloads on-demand.

import pandas as pd
import numpy as np
import unicodedata, re, warnings
import streamlit as st
import plotly.express as px

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet._reader")

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
# 1) Utilitários curtos
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

def acha_linha_header_cidades_uf(df_no_header: pd.DataFrame) -> int | None:
    for i, row in df_no_header.iterrows():
        vals = [nrm(x) for x in row.tolist()]
        if "CIDADES" in vals and "UF" in vals:
            return i
    return None

def to7(s: pd.Series) -> pd.Series:
    return s.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace("%","",regex=False).str.replace(",",".",regex=False),
        errors="coerce"
    )

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
    ren = {
        "UF":"UF_COD_NUM",
        "Nome_UF":"UF_NOME",
        "Código Município Completo":"MUNICIPIO_CODIGO",
        "Nome_Município":"MUNICIPIO_NOME"
    }
    if not set(ren.keys()).issubset(raw.columns):
        norm = {c: nrm(c) for c in raw.columns}
        inv  = {v:k for k,v in norm.items()}
        ren = {
            inv.get("UF","UF"): "UF_COD_NUM",
            inv.get("NOME_UF","Nome_UF"): "UF_NOME",
            inv.get("CODIGO MUNICIPIO COMPLETO","Código Município Completo"): "MUNICIPIO_CODIGO",
            inv.get("NOME_MUNICIPIO","Nome_Município"): "MUNICIPIO_NOME",
        }
    dtb = (raw.rename(columns=ren)[["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
            .dropna(subset=["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]))
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
    if not abas: raise RuntimeError("Nenhuma aba 2020–2025 encontrada em Dados_alpa.xlsx.")
    frames = []
    for aba in abas:
        nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
        hdr = acha_linha_header_cidades_uf(nohdr)
        if hdr is None:
            st.warning(f"[{aba}] Header CIDADES/UF não encontrado. Pulando…")
            continue
        df = pd.read_excel(path, sheet_name=aba, header=hdr)
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm=="CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm=="UF"), None)
        if not c_cid or not c_uf:
            st.warning(f"[{aba}] Colunas CIDADES/UF não encontradas. Pulando…")
            continue
        tmp = (df[[c_cid,c_uf]].copy()
                 .rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"}))
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
        tmp["UF_SIGLA"]           = tmp["UF_SIGLA"].astype(str).str.strip()
        tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
        tmp = tmp[tmp["MUNICIPIO_NOME_ALP"].str.len()>0]
        tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        tmp["FONTE_ABA"] = aba
        frames.append(tmp)
    if not frames: raise RuntimeError("Nenhuma aba válida com CIDADES/UF processada.")
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
# 5) Evasão (INEP)
# ============================
def carrega_evasao(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, header=8, engine="odf")
    df["CO_MUNICIPIO"] = to7(df["CO_MUNICIPIO"])
    cols_desejadas = [
        "NO_REGIAO","NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA",
        "1_CAT3_CATFUN","1_CAT3_CATFUN_AI","1_CAT3_CATFUN_AF",
        "1_CAT3_CATFUN_01","1_CAT3_CATFUN_02","1_CAT3_CATFUN_03","1_CAT3_CATFUN_04","1_CAT3_CATFUN_05",
        "1_CAT3_CATFUN_06","1_CAT3_CATFUN_07","1_CAT3_CATFUN_08","1_CAT3_CATFUN_09",
        "1_CAT3_CATMED","1_CAT3_CATMED_01","1_CAT3_CATMED_02","1_CAT3_CATMED_03"
    ]
    cols = [c for c in cols_desejadas if c in df.columns]
    df = df[cols].copy()
    mapa = {
        "1_CAT3_CATFUN": "EVASAO_FUNDAMENTAL",
        "1_CAT3_CATMED": "EVASAO_MEDIO",
        "1_CAT3_CATFUN_AI": "EVASAO_FUN_AI",
        "1_CAT3_CATFUN_AF": "EVASAO_FUN_AF",
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
    alpa = carrega_alpargatas(ARQ_ALP)
    dtb  = carrega_dtb(ARQ_DTB)
    base = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left")
    mask = (base["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & \
           (base["UF_SIGLA"]=="PB") & (base["MUNICIPIO_CODIGO"].isna())
    base.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"

    df_ini = pd.read_excel(ARQ_INICIAIS, header=9)
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9)
    df_med = pd.read_excel(ARQ_EM,       header=9)

    ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    base["CO_MUNICIPIO"] = to7(base["MUNICIPIO_CODIGO"])
    base = (base.merge(ini, on="CO_MUNICIPIO", how="left")
                .merge(fin, on="CO_MUNICIPIO", how="left")
                .merge(med, on="CO_MUNICIPIO", how="left"))

    eva  = carrega_evasao(ARQ_EVASAO)
    base = base.merge(eva, on="CO_MUNICIPIO", how="left")

    base["Reprovacao_Iniciais"] = (1 - pd.to_numeric(base["TAXA_APROVACAO_INICIAIS"], errors="coerce")) * 100
    base["Reprovacao_Finais"]   = (1 - pd.to_numeric(base["TAXA_APROVACAO_FINAIS"],  errors="coerce")) * 100
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]:
        base[c] = _num(base[c])

    num_cols = ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]
    q1 = base[num_cols].quantile(0.25, numeric_only=True)
    q3 = base[num_cols].quantile(0.75, numeric_only=True)
    iqr = q3 - q1
    low  = q1 - 1.5*iqr
    high = q3 + 1.5*iqr
    for c in num_cols:
        base[c] = base[c].clip(lower=low[c], upper=high[c])
    base["Urgencia"] = base[num_cols].sum(axis=1, skipna=True)

    # Evolução (2005–2023)
    evo_ini = _long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_med, "APROVACAO_MEDIO")
    evolucao = (evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                       .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
        ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]
    ].mean(axis=1, skipna=True)

    dtb_lookup = dtb[["MUNICIPIO_CODIGO","UF_SIGLA","MUNICIPIO_NOME"]].rename(columns={"MUNICIPIO_CODIGO":"CO_MUNICIPIO"})
    dtb_lookup["CO_MUNICIPIO"] = to7(dtb_lookup["CO_MUNICIPIO"])
    evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")
    evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)

    return base, evolucao

# ============================
# 7) UI — Streamlit (com filtro por UF)
# ============================
st.set_page_config(page_title="IA • Aprovação/Evasão", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel de Municípios (sem SAIDA_DIR)")

with st.spinner("Processando dados…"):
    base_full, evolucao_full = build_data()

# -------- Sidebar: filtro por UF --------
ufs = sorted([u for u in base_full["UF_SIGLA"].dropna().unique() if isinstance(u, str)])
sel_ufs = st.sidebar.multiselect("Filtrar por UF", options=ufs, default=ufs)
if len(sel_ufs) == 0:
    st.sidebar.info("Selecione ao menos uma UF.")
    sel_ufs = ufs

# aplica filtro
base = base_full[base_full["UF_SIGLA"].isin(sel_ufs)].copy()
evolucao_filtrada = evolucao_full[evolucao_full["UF_SIGLA"].isin(sel_ufs)].copy()

# recomputa Top 20 após filtro
urgentes = base.sort_values("Urgencia", ascending=False).head(20).copy()

# KPIs (filtrados)
c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios (base)", f"{base['MUNICIPIO_CHAVE'].nunique()}")
with c2: st.metric("Aprovação — Finais (média)", f"{(pd.to_numeric(base['TAXA_APROVACAO_FINAIS'], errors='coerce').mean()*100):.1f}%")
with c3: st.metric("Evasão — Fundamental (média)", f"{base['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4: st.metric("Urgência — média", f"{base['Urgencia'].mean():.1f}")

tab1, tab2, tab3, tab4 = st.tabs(["🔎 Tabelas","📈 Tendências","🏷️ Downloads","⚙️ Diagnóstico"])

with tab1:
    st.subheader("Urgentes (Top 20 por urgência)")
    st.dataframe(urgentes[[
        "UF_SIGLA","MUNICIPIO_NOME_ALP","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
        "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"
    ]], use_container_width=True)

    st.subheader("Base consolidada (amostra)")
    st.dataframe(base.head(50), use_container_width=True)

with tab2:
    st.subheader("Tendência geral — aprovação (média do recorte de urgentes)")
    tmp = evolucao_filtrada.copy()
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        tmp[c] = pd.to_numeric(tmp[c], errors="coerce")*100
    m = tmp.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean()
    st.plotly_chart(
        px.line(m.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
                x="ANO", y="Aprovação (%)", color="Etapa", markers=True),
        use_container_width=True
    )

    st.subheader("Gap — Iniciais − Finais (p.p.)")
    gap = (tmp.groupby("ANO")[["APROVACAO_INICIAIS","APROVACAO_FINAIS"]].mean())
    gap["GAP"] = gap["APROVACAO_INICIAIS"] - gap["APROVACAO_FINAIS"]
    st.plotly_chart(px.line(gap.reset_index(), x="ANO", y="GAP", markers=True), use_container_width=True)

    st.subheader("Séries por município (selecione)")
    muni_opts = evolucao_filtrada["MUNICIPIO_NOME"].dropna().unique()
    muni = st.selectbox("Município", sorted(muni_opts))
    rec = evolucao_filtrada[evolucao_filtrada["MUNICIPIO_NOME"]==muni].copy()
    for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]:
        rec[c] = pd.to_numeric(rec[c], errors="coerce")*100
    st.plotly_chart(
        px.line(rec.melt(id_vars=["ANO"], value_vars=["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"],
                         var_name="Etapa", value_name="Aprovação (%)"),
                x="ANO", y="Aprovação (%)", color="Etapa", markers=True),
        use_container_width=True
    )

with tab3:
    st.write("Baixe os CSVs (respeitam o filtro de UF):")
    cA, cB, cC = st.columns(3)
    with cA:
        st.download_button("⬇️ Base consolidada (filtrada)", base.to_csv(index=False).encode("utf-8"),
                           file_name="base_consolidada_filtrada.csv")
    with cB:
        st.download_button("⬇️ Urgentes Top20 (filtrado)", urgentes.to_csv(index=False).encode("utf-8"),
                           file_name="urgentes_top20_filtrado.csv")
    with cC:
        st.download_button("⬇️ Evolução (recorte filtrado)", evolucao_filtrada.to_csv(index=False).encode("utf-8"),
                           file_name="evolucao_recorte_filtrado.csv")

with tab4:
    st.write("**Shapes** (após filtro)")
    st.write("base:", base.shape, "| urgentes:", urgentes.shape, "| evolucao_filtrada:", evolucao_filtrada.shape)
    st.write("Tipos (base):")
    st.code(str(base.dtypes))
