import pandas as pd
import unicodedata
from pathlib import Path

# ============================
# 0) CAMINHOS (use os seus)
# ============================
ARQ_ALP = "dados/Dados_alpa.xlsx"
ARQ_DTB = "dados/dtb_municipios.ods"

# =========================================================
# 1) Utilitários curtos
# =========================================================
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

# =========================================================
# 2) Ler & limpar DTB/IBGE
# =========================================================
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
    dtb = (raw.rename(columns={
            "UF":"UF_COD_NUM","Nome_UF":"UF_NOME",
            "Código Município Completo":"MUNICIPIO_CODIGO",
            "Nome_Município":"MUNICIPIO_NOME"
        })[["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
        .dropna())
    dtb["UF_SIGLA"]         = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"] = dtb["MUNICIPIO_CODIGO"].astype(str).str.zfill(7)
    dtb["MUNICIPIO_NOME"]   = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]  = dtb["MUNICIPIO_NOME"].apply(chave_municipio)
    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

# =========================================================
# 3) Ler abas do arquivo Alpargatas (2020–2025)
# =========================================================
def carrega_alpargatas(path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        raise RuntimeError("Nenhuma aba 2020–2025 encontrada no arquivo Alpargatas.")
    frames = []
    for aba in abas:
        nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
        hdr = acha_linha_header_cidades_uf(nohdr)
        if hdr is None:
            print(f"[AVISO] Header CIDADES/UF não encontrado na aba '{aba}'. Pulando…")
            continue
        df = pd.read_excel(path, sheet_name=aba, header=hdr)
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm=="CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm=="UF"), None)
        if not c_cid or not c_uf:
            print(f"[AVISO] Colunas 'CIDADES'/'UF' ausentes na aba '{aba}'. Pulando…")
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

# =========================================================
# 4) Cruzamento (sem salvar arquivos)
# =========================================================
def cruzar(dtb: pd.DataFrame, alpa: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    codificados = alpa.merge(
        dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE")
    )
    nao_encontrados = (codificados[codificados["MUNICIPIO_CODIGO"].isna()]
                       .drop_duplicates(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
                       .sort_values(["UF_SIGLA","MUNICIPIO_NOME_ALP"]))
    print("\nConcluído:")
    print(f" - Codificados:   {len(codificados):>6}")
    print(f" - Para revisar:  {len(nao_encontrados):>6}")
    return codificados, nao_encontrados

# =========================================================
# 5) Execução
# =========================================================
if __name__ == "__main__":
    print("Lendo DTB/IBGE…")
    dtb  = carrega_dtb(ARQ_DTB)

    print("Lendo abas do arquivo Alpargatas…")
    alpa = carrega_alpargatas(ARQ_ALP)

    print("Cruzando…")
    codificados, nao_encontrados = cruzar(dtb, alpa)

    print("\nAmostra codificados:")
    print(codificados.head(10).to_string(index=False))

# streamlit_app.py
# Requer: pandas, numpy, plotly, streamlit, odfpy (para .ods)
import pandas as pd
import numpy as np
import unicodedata, re
import plotly.express as px
import streamlit as st

# ===== IMPORTANTE =====
# Cole acima deste bloco as funções que já te enviei:
# - nrm, chave_municipio, acha_linha_header_cidades_uf
# - carrega_dtb, carrega_alpargatas, cruzar
# - e os ARQ_ALP / ARQ_DTB (sem SAIDA_DIR)

# ============================
# 0) CAMINHOS extras (use os seus)
# ============================
ARQ_INICIAS = "dados/anos_iniciais.xlsx"
ARQ_FINAIS = "dados/anos_finais.xlsx"
ARQ_EM = "dados/ensino_medio.xlsx"
ARQ_EVASAO   = "dados/evasao.ods"

# ============================
# 1) Funções utilitárias
# ============================
def to7(s: pd.Series) -> pd.Series:
    return s.astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

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
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    for c in cols: t[c] = pd.to_numeric(t[c], errors="coerce")
    long_ = t[["CO_MUNICIPIO"] + cols].melt("CO_MUNICIPIO", value_name=rotulo)
    long_["ANO"] = long_["variable"].str.extract(r"(\d{4})").astype(int)
    long_.drop(columns="variable", inplace=True)
    return long_.groupby(["CO_MUNICIPIO","ANO"], as_index=False)[rotulo].mean()

def _num(s):  # % e vírgulas
    return pd.to_numeric(
        s.astype(str).str.replace("%","",regex=False).str.replace(",","." ,regex=False),
        errors="coerce"
    )

def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return (s - s.min())/(s.max()-s.min()) if s.max()!=s.min() else pd.Series(0.5, index=s.index)

# ============================
# 2) Pipeline de dados
# ============================
@st.cache_data(show_spinner=False)
def build_data():
    # DTB + Alpargatas (match)
    dtb = carrega_dtb(ARQ_DTB)
    alpa = carrega_alpargatas(ARQ_ALP)
    codificados, _nao = cruzar(dtb, alpa)

    # Ajuste Campina Grande (se faltar)
    mask = (codificados["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & \
           (codificados["UF_SIGLA"]=="PB") & (codificados["MUNICIPIO_CODIGO"].isna())
    codificados.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"

    # Planilhas aprovação
    df_ini = pd.read_excel(ARQ_INICIAIS, header=9)
    df_fin = pd.read_excel(ARQ_FINAIS,   header=9)
    df_med = pd.read_excel(ARQ_EM,       header=9)

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

    # Evasão
    if ARQ_EVASAO.lower().endswith(".ods"):
        df_eva = pd.read_excel(ARQ_EVASAO, header=8, engine="odf")
    else:
        df_eva = pd.read_excel(ARQ_EVASAO, header=8)

    cols_pick = ["NO_REGIAO","NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA",
                 "1_CAT3_CATFUN","1_CAT3_CATMED"]
    cols_pick = [c for c in cols_pick if c in df_eva.columns]
    eva = df_eva[cols_pick].rename(columns={
        "1_CAT3_CATFUN":"EVASAO_FUNDAMENTAL",
        "1_CAT3_CATMED":"EVASAO_MEDIO"
    }).copy()
    eva["CO_MUNICIPIO"] = to7(eva["CO_MUNICIPIO"])
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]:
        if c in eva.columns: eva[c] = _num(eva[c])

    # Merge res × evasão
    res2 = res.merge(eva, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")

    # Urgência simples
    res2["Reprovacao_Iniciais"] = (1 - pd.to_numeric(res2["TAXA_APROVACAO_INICIAIS"], errors="coerce")) * 100
    res2["Reprovacao_Finais"]   = (1 - pd.to_numeric(res2["TAXA_APROVACAO_FINAIS"],  errors="coerce")) * 100
    for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]:
        res2[c] = _num(res2[c])
    res2["Urgencia"] = res2[["EVASAO_FUNDAMENTAL","EVASAO_MEDIO","Reprovacao_Iniciais","Reprovacao_Finais"]].sum(axis=1, skipna=True)

    # Tabela de urgentes (top 20)
    urgentes = (res2.copy()
                .sort_values("Urgencia", ascending=False)
                .head(20))

    # Evolução histórica (2005–2023)
    evo_ini = _long_por_municipio_ano(df_ini, "APROVACAO_INICIAIS")
    evo_fin = _long_por_municipio_ano(df_fin, "APROVACAO_FINAIS")
    evo_med = _long_por_municipio_ano(df_med, "APROVACAO_MEDIO")
    evolucao = (evo_ini.merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
                       .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))
    evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean(axis=1, skipna=True)

    # Anexar UF + nome
    dtb_lookup = dtb[["MUNICIPIO_CODIGO","UF_SIGLA","MUNICIPIO_NOME"]].rename(columns={"MUNICIPIO_CODIGO":"CO_MUNICIPIO"})
    dtb_lookup["CO_MUNICIPIO"] = to7(dtb_lookup["CO_MUNICIPIO"])
    evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")
    evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)

    # Filtrar só municípios presentes em urgentes
    urg_ck = ensure_key_urgentes(urgentes)
    evolucao_filtrada = evolucao.merge(
        urg_ck[["UF_SIGLA","MUNICIPIO_CHAVE"]].drop_duplicates(),
        on=["UF_SIGLA","MUNICIPIO_CHAVE"], how="inner"
    ).sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True)

    return res2, urgentes, evolucao_filtrada

# ============================
# 3) Painel Streamlit
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


