# gpt.py — App Streamlit único (Instituto Alpargatas)
# Requer: streamlit, pandas, numpy, plotly, odfpy, openpyxl

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet._reader")

import pandas as pd
import numpy as np
import unicodedata, re, traceback
from pathlib import Path
import streamlit as st
import plotly.express as px

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
# 1) Helpers
# ============================
def nrm(txt: object) -> str:
    if pd.isna(txt): return ""
    s = unicodedata.normalize("NFKD", str(txt)).encode("ASCII","ignore").decode("ASCII")
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
        s.astype(str).str.replace("%","",regex=False).str.replace(",","." ,regex=False),
        errors="coerce"
    )

def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return (s - s.min())/(s.max()-s.min()) if s.max()!=s.min() else pd.Series(0.5, index=s.index)

def _nrm_header(s: object) -> str:
    if pd.isna(s): return ""
    t = unicodedata.normalize("NFKD", str(s)).encode("ASCII","ignore").decode("ASCII")
    return re.sub(r"\s+"," ", t).strip().upper()

# ============================
# 2) Leitura de bases
# ============================
def carrega_dtb(path: str) -> pd.DataFrame:
    """
    Lê o dtb_municipios.ods detectando automaticamente:
      • a planilha correta (ignora capas/abas sem dados)
      • a linha do cabeçalho correta
    Aceita tanto 'Nome_UF' (nome por extenso) quanto 'UF' (sigla) para a UF.
    """
    # 1) varredura de abas e linhas para encontrar um cabeçalho válido
    xls = pd.ExcelFile(path, engine="odf")
    header_row = None
    sheet_ok = None

    # sinônimos aceitos
    wants_nome_uf = {"NOME UF","NOME_UF","UF NOME","UF - NOME","NOME DA UF"}
    wants_uf_sigla = {"UF"}
    wants_cod = {
        "CODIGO MUNICIPIO COMPLETO","CÓDIGO MUNICIPIO COMPLETO",
        "CODIGO DO MUNICIPIO","CODIGO MUNICIPIO","COD MUNICIPIO"
    }
    wants_nome_mun = {"NOME MUNICIPIO","NOME DO MUNICIPIO","NOME_MUNICIPIO","MUNICIPIO","MUNICÍPIO"}

    def _has_any(vals:set[str], rowvals:list[str]) -> bool:
        rowset = set(rowvals)
        return any(v in rowset for v in vals)

    for sh in xls.sheet_names:
        # lê as primeiras ~120 linhas sem header para procurar o cabeçalho
        peek = pd.read_excel(path, engine="odf", sheet_name=sh, header=None, nrows=120)
        for i, row in peek.iterrows():
            vals = [_nrm_header(x) for x in row.tolist()]
            # precisa ter código + nome do município e (Nome_UF OU UF)
            if _has_any(wants_cod, vals) and _has_any(wants_nome_mun, vals) and (_has_any(wants_nome_uf, vals) or _has_any(wants_uf_sigla, vals)):
                header_row, sheet_ok = i, sh
                break
        if sheet_ok is not None:
            break

    if sheet_ok is None:
        raise KeyError(f"DTB: não encontrei um cabeçalho válido em nenhuma aba. Abas: {xls.sheet_names}")

    # 2) lê a planilha com o header detectado
    raw = pd.read_excel(path, engine="odf", sheet_name=sheet_ok, header=header_row)
    norm_map = {_nrm_header(c): c for c in raw.columns}

    # 3) localiza colunas reais por nomes normalizados
    def _pick(candidates:set[str]) -> str | None:
        for cand in candidates:
            key = _nrm_header(cand)
            if key in norm_map:
                return norm_map[key]
            # também aceita "começa com"
            matches = [norm_map[k] for k in norm_map if k.startswith(key)]
            if matches:
                return matches[0]
        return None

    c_uf_nome  = _pick(wants_nome_uf)          # pode vir None
    c_uf_sigla = _pick(wants_uf_sigla)         # pode vir None
    c_cod_mun  = _pick(wants_cod)
    c_nome_mun = _pick(wants_nome_mun)

    miss = [n for n,v in {"MUNICIPIO_CODIGO":c_cod_mun, "MUNICIPIO_NOME":c_nome_mun}.items() if v is None]
    if miss:
        raise KeyError(f"DTB: não achei colunas essenciais {miss} na aba '{sheet_ok}'. Colunas: {list(raw.columns)}")

    # 4) monta DataFrame padronizado
    cols = {c_cod_mun:"MUNICIPIO_CODIGO", c_nome_mun:"MUNICIPIO_NOME"}
    if c_uf_nome:  cols[c_uf_nome]  = "UF_NOME"
    if c_uf_sigla: cols[c_uf_sigla] = "UF"

    dtb = raw[list(cols.keys())].rename(columns=cols).copy()
    dtb = dtb.dropna(subset=["MUNICIPIO_CODIGO","MUNICIPIO_NOME"])

    # padronizações
    dtb["MUNICIPIO_CODIGO"] = dtb["MUNICIPIO_CODIGO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    dtb["MUNICIPIO_NOME"]   = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]  = dtb["MUNICIPIO_NOME"].apply(chave_municipio)

    # 5) UF_SIGLA: prioriza Nome_UF → mapa; senão, usa a própria coluna 'UF'
    UF_SIGLAS = {
        "ACRE":"AC","ALAGOAS":"AL","AMAPA":"AP","AMAZONAS":"AM","BAHIA":"BA","CEARA":"CE","DISTRITO FEDERAL":"DF",
        "ESPIRITO SANTO":"ES","GOIAS":"GO","MARANHAO":"MA","MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG",
        "PARA":"PA","PARAIBA":"PB","PARANA":"PR","PERNAMBUCO":"PE","PIAUI":"PI","RIO DE JANEIRO":"RJ",
        "RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS","RONDONIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC",
        "SAO PAULO":"SP","SERGIPE":"SE","TOCANTINS":"TO","AMAPÁ":"AP","CEARÁ":"CE","ESPÍRITO SANTO":"ES","GOIÁS":"GO",
        "MARANHÃO":"MA","PARÁ":"PA","PARAÍBA":"PB","PARANÁ":"PR"
    }

    if "UF_NOME" in dtb.columns:
        dtb["UF_SIGLA"] = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    elif "UF" in dtb.columns:
        dtb["UF_SIGLA"] = dtb["UF"].astype(str).str.upper().str.strip()
    else:
        dtb["UF_SIGLA"] = pd.NA

    if dtb["UF_SIGLA"].isna().all():
        raise ValueError(f"DTB: não consegui derivar UF_SIGLA (nem Nome_UF nem UF). Aba '{sheet_ok}'.")

    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]


def carrega_alpargatas(path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        raise RuntimeError("Nenhuma aba 2020–2025 encontrada em Dados_alpa.xlsx.")
    frames = []
    for aba in abas:
        nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400, engine="openpyxl")
        hdr = acha_linha_header_cidades_uf(nohdr)
        if hdr is None:
            st.warning(f"Aba '{aba}': cabeçalho com CIDADES/UF não encontrado. Pulando…")
            continue
        df = pd.read_excel(path, sheet_name=aba, header=hdr, engine="openpyxl")
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm=="CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm=="UF"), None)
        if not c_cid or not c_uf:
            st.warning(f"Aba '{aba}': colunas 'CIDADES'/'UF' ausentes. Pulando…")
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
    if not frames:
        raise RuntimeError("Nenhuma aba válida foi processada (CIDADES/UF não encontrado).")
    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])

def cruzar(dtb: pd.DataFrame, alpa: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    codificados = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE"))
    nao_encontrados = (codificados[codificados["MUNICIPIO_CODIGO"].isna()]
                       .drop_duplicates(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
                       .sort_values(["UF_SIGLA","MUNICIPIO_NOME_ALP"]))
    return codificados, nao_encontrados

# ============================
# 3) Evolução / Indicadores
# ============================
def media_por_municipio(df: pd.DataFrame, rotulo: str) -> pd.DataFrame:
    out = pd.DataFrame({
        "CO_MUNICIPIO": to7(df["CO_MUNICIPIO"]),
        rotulo: pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce"),
    })
    return out.groupby("CO_MUNICIPIO", as_index=False)[rotulo].mean()

def ensure_key_urgentes(urg: pd.DataFrame) -> pd.DataFrame:
    u = urg.copy()
    base = u["MUNICIPIO_NOME_ALP"] if "MUNICIPIO_NOME_ALP" in u.columns else u.get("NO_MUNICIPIO")
    u["MUNICIPIO_CHAVE"] = base.apply(chave_municipio)
    return u

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
    if not anos:
        raise KeyError("Planilha sem colunas VL_INDICADOR_REND_YYYY (2005–2023).")
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    for c in cols: t[c] = pd.to_numeric(t[c], errors="coerce")
    long_ = t[["CO_MUNICIPIO"] + cols].melt("CO_MUNICIPIO", value_name=rotulo)
    long_["ANO"] = long_["variable"].str.extract(r"(\d{4})").astype(int)
    long_.drop(columns="variable", inplace=True)
    return long_.groupby(["CO_MUNICIPIO","ANO"], as_index=False)[rotulo].mean()

# ============================
# 4) Sidebar: diagnóstico
# ============================
def _diag_arquivos():
    base = Path(__file__).parent
    dados = base / "dados"
    st.sidebar.subheader("🧪 Diagnóstico")
    st.sidebar.write("Diretório:", str(base))
    st.sidebar.write("Conteúdo de `dados/`:",
                     [p.name for p in sorted(dados.glob("*"))] if dados.exists() else "pasta não encontrada")
    for nome, rel in {"ARQ_ALP":ARQ_ALP,"ARQ_DTB":ARQ_DTB,"ARQ_INICIAIS":ARQ_INICIAIS,
                      "ARQ_FINAIS":ARQ_FINAIS,"ARQ_EM":ARQ_EM,"ARQ_EVASAO":ARQ_EVASAO}.items():
        st.sidebar.write(f"{nome} → {'OK' if (base/rel).exists() else 'FALTA'}")

# ============================
# 5) Pipeline
# ============================
@st.cache_data(show_spinner=True)
def build_data():
    dtb  = carrega_dtb(ARQ_DTB)
    alpa = carrega_alpargatas(ARQ_ALP)
    codificados, _nao = cruzar(dtb, alpa)

    mask = (codificados["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & \
           (codificados["UF_SIGLA"]=="PB") & (codificados["MUNICIPIO_CODIGO"].isna())
    codificados.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"

    df_ini = pd.read_excel(ARQ_INICIAIS, header=9, engine="openpyxl")
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9, engine="openpyxl")
    df_med = pd.read_excel(ARQ_EM,       header=9, engine="openpyxl")

    ini = media_por_municipio(df_ini, "TAXA_APROVACAO_INICIAIS")
    fin = media_por_municipio(df_fin, "TAXA_APROVACAO_FINAIS")
    med = media_por_municipio(df_med, "TAXA_APROVACAO_MEDIO")

    res = codificados.copy()
    res["MUNICIPIO_CODIGO"] = to7(res["MUNICIPIO_CODIGO"])
    res = (res.merge(ini, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
             .merge(fin, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("","_fin"))
             .merge(med, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("","_med")))
    for c in ["CO_MUNICIPIO","CO_MUNICIPIO_fin","CO_MUNICIPIO_med"]:
        if c in res.columns: res.drop(columns=c, inplace=True)

    # Evasão
    if str(ARQ_EVASAO).lower().endswith(".ods"):
        df_eva = pd.read_excel(ARQ_EVASAO, header=8, engine="odf")
    else:
        df_eva = pd.read_excel(ARQ_EVASAO, header=8)
    cols_pick = ["NO_REGIAO","NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA",
                 "1_CAT3_CATFUN","1_CAT3_CATMED"]
    cols_pick = [c for c in cols_pick if c in df_eva.columns]
    eva = (df_eva[cols_pick]
           .rename(columns={"1_CAT3_CATFUN":"EVASAO_FUNDAMENTAL","1_CAT3_CATMED":"EVASAO_MEDIO"})
           .copy())
    eva["CO_MUNICIPIO"] = to7(eva["CO_MUNICIPIO"])
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]:
        if c in eva.columns: eva[c] = _num(eva[c])

    res2 = res.merge(eva, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
    res2["Reprovacao_Iniciais"] = (1 - pd.to_numeric(res2["TAXA_APROVACAO_INICIAIS"], errors="coerce")) * 100
    res2["Reprovacao_Finais"]   = (1 - pd.to_numeric(res2["TAXA_APROVACAO_FINAIS"],  errors="coerce")) * 100
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]:
        res2[c] = _num(res2[c])
    res2["Urgencia"] = res2[["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1, skipna=True)

    urgentes = res2.sort_values("Urgencia", ascending=False).head(20)

    # Evolução 2005–2023
    evo_ini = _long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_med, "APROVACAO_MEDIO")
    evolucao = (evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                       .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean(axis=1, skipna=True)

    # Anexar UF + nome e filtrar pelos urgentes
    dtb_lookup = dtb[["MUNICIPIO_CODIGO","UF_SIGLA","MUNICIPIO_NOME"]].rename(columns={"MUNICIPIO_CODIGO":"CO_MUNICIPIO"})
    dtb_lookup["CO_MUNICIPIO"] = to7(dtb_lookup["CO_MUNICIPIO"])
    evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")
    evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)

    urg_ck = ensure_key_urgentes(urgentes)
    evolucao_filtrada = evolucao.merge(
        urg_ck[["UF_SIGLA","MUNICIPIO_CHAVE"]].drop_duplicates(),
        on=["UF_SIGLA","MUNICIPIO_CHAVE"], how="inner"
    ).sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True)

    return res2, urgentes, evolucao_filtrada

# ============================
# 6) UI
# ============================
st.set_page_config(page_title="IA • Aprovação/Evasão", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel")
_diag_arquivos()

with st.spinner("Processando dados…"):
    try:
        base, urgentes, evolucao_filtrada = build_data()
    except Exception:
        st.error("❌ Erro ao montar os dados. Stack trace abaixo:")
        st.code(traceback.format_exc())
        st.stop()

c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios (base)", f"{base['MUNICIPIO_CODIGO'].nunique()}")
with c2: st.metric("Aprovação — Finais (média)", f"{(pd.to_numeric(base['TAXA_APROVACAO_FINAIS'], errors='coerce').mean()*100):.1f}%")
with c3: st.metric("Evasão — Fundamental (média)", f"{base['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4: st.metric("Urgência — média", f"{base['Urgencia'].mean():.1f}")

tab1, tab2, tab3 = st.tabs(["🔎 Tabelas","📈 Gráficos","⚙️ Diagnóstico"])

with tab1:
    st.subheader("Urgentes (Top 20 por urgência)")
    st.dataframe(urgentes[[
        "UF_SIGLA","MUNICIPIO_NOME_ALP","EVASAO_FUNDAMENTAL","EVASAO_MEDIO",
        "Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"
    ]], use_container_width=True)

    st.subheader("Evolução (recorte dos urgentes)")
    show_cols = ["UF_SIGLA","MUNICIPIO_NOME","ANO","APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]
    st.dataframe(evolucao_filtrada[show_cols], use_container_width=True)

with tab2:
    st.subheader("Tendência geral — aprovação (média do recorte)")
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

with tab3:
    st.write("**Shapes**")
    st.write("base:", base.shape, "| urgentes:", urgentes.shape, "| evolucao_filtrada:", evolucao_filtrada.shape)
    st.write("Tipos (base):")
    st.code(str(base.dtypes))
