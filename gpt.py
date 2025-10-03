# -*- coding: utf-8 -*-
# app_alpargatas.py — script único (prep + painel) pronto para nuvem

import re, unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

# ============================
# 0) CAMINHOS CLOUD-READY
# ============================
BASE_DIR   = Path(__file__).resolve().parent
DADOS_DIR  = BASE_DIR / "dados"
SAIDA_DIR  = DADOS_DIR / "saida"
SAIDA_DIR.mkdir(parents=True, exist_ok=True)

ARQ_ALP        = DADOS_DIR / "Dados_alpa.xlsx"
ARQ_DTB        = DADOS_DIR / "dtb_municipios.ods"
ods_iniciais   = DADOS_DIR / "anos_iniciais.xlsx"
ods_finais     = DADOS_DIR / "anos_finais.xlsx"
ods_em         = DADOS_DIR / "ensino_medio.xlsx"
caminho_evasao = DADOS_DIR / "evasao.ods"

# leitor inteligente (xlsx/xls/ods)
def read_excel_smart(path: Path, **kwargs) -> pd.DataFrame:
    try:
        if path.suffix.lower() == ".ods":
            return pd.read_excel(path, engine="odf", **kwargs)
        return pd.read_excel(path, **kwargs)
    except Exception as e:
        msg = str(e).lower()
        if path.suffix.lower() == ".ods" and ("odf" in msg or "engine" in msg):
            raise RuntimeError(
                "Para ler .ods na nuvem, adicione 'odfpy' ao requirements.txt."
            ) from e
        raise

# =========================================================
# 1) Utilitários
# =========================================================
def nrm(x) -> str:
    if pd.isna(x): return ""
    s = unicodedata.normalize("NFKD", str(x)).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n: n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf): n = n[: -len(suf)].strip()
    return n

def acha_linha_header_cidades_uf(df_no_header: pd.DataFrame) -> int | None:
    for i, row in df_no_header.iterrows():
        vals = [nrm(x) for x in row.tolist()]
        if "CIDADES" in vals and "UF" in vals: return i
    return None

# =========================================================
# 2) DTB/IBGE
# =========================================================
_UF_SIGLAS = {
    "ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA","CEARÁ":"CE",
    "DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOÍAS":"GO","MARANHÃO":"MA",
    "MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG","PARÁ":"PA",
    "PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI","RIO DE JANEIRO":"RJ",
    "RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS","RONDÔNIA":"RO","RORAIMA":"RR",
    "SANTA CATARINA":"SC","SÃO PAULO":"SP","SERGIPE":"SE","TOCANTINS":"TO"
}

def carrega_dtb(path: Path) -> pd.DataFrame:
    raw = read_excel_smart(path, skiprows=6)
    dtb = (raw.rename(columns={
                "UF": "UF_COD_NUM",
                "Nome_UF": "UF_NOME",
                "Código Município Completo": "MUNICIPIO_CODIGO",
                "Nome_Município": "MUNICIPIO_NOME"
           })[["UF_COD_NUM","UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
           .dropna(subset=["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]))
    dtb["UF_SIGLA"]         = dtb["UF_NOME"].astype(str).str.upper().map(_UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"] = dtb["MUNICIPIO_CODIGO"].astype(str).str.zfill(7)
    dtb["MUNICIPIO_NOME"]   = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]  = dtb["MUNICIPIO_NOME"].apply(chave_municipio)
    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

# =========================================================
# 3) ALP 2020–2025
# =========================================================
def carrega_alpargatas(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas: raise RuntimeError("Nenhuma aba 2020–2025 encontrada no arquivo Alpargatas.")

    frames = []
    for aba in abas:
        nohdr = read_excel_smart(path, sheet_name=aba, header=None, nrows=400)
        hdr = acha_linha_header_cidades_uf(nohdr)
        if hdr is None:
            print(f"[AVISO] Não achei cabeçalho CIDADES/UF na aba '{aba}'. Pulando…")
            continue
        df = read_excel_smart(path, sheet_name=aba, header=hdr)
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm == "CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm == "UF"), None)
        if not c_cid or not c_uf:
            print(f"[AVISO] Colunas 'CIDADES'/'UF' não encontradas na aba '{aba}'.")
            continue
        tmp = (df[[c_cid, c_uf]].copy()
               .rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"}))
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
        tmp["UF_SIGLA"]           = tmp["UF_SIGLA"].astype(str).str.strip()
        tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
        tmp = tmp[tmp["MUNICIPIO_NOME_ALP"].str.len() > 0]
        tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        tmp["FONTE_ABA"] = aba
        frames.append(tmp)

    if not frames: raise RuntimeError("Nenhuma aba válida foi processada.")
    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])

# =========================================================
# 4) Cruzamento e saída
# =========================================================
def cruzar_e_salvar(dtb: pd.DataFrame, alpa: pd.DataFrame, saida_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    codificados = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE"))
    nao_encontrados = (codificados[codificados["MUNICIPIO_CODIGO"].isna()]
                       .drop_duplicates(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
                       .sort_values(["UF_SIGLA","MUNICIPIO_NOME_ALP"]))
    codificados.to_csv(saida_dir / "municipios_alpargatas_codificados.csv", index=False, encoding="utf-8")
    nao_encontrados.to_csv(saida_dir / "municipios_nao_encontrados_para_tratar.csv", index=False, encoding="utf-8")
    print(f"\nConcluído:\n - Codificados: {len(codificados):>6}\n - Para revisar: {len(nao_encontrados):>6}")
    return codificados, nao_encontrados

# =========================================================
# 5) Execução (gera variáveis globais)
# =========================================================
print("Lendo DTB/IBGE…"); dtb  = carrega_dtb(ARQ_DTB)
print("Lendo abas do arquivo Alpargatas…"); alpa = carrega_alpargatas(ARQ_ALP)
print("Cruzando e salvando…"); codificados, nao_encontrados = cruzar_e_salvar(dtb, alpa, SAIDA_DIR)

print("\nAmostra codificados:"); print(codificados.head(10).to_string(index=False))

# Ajuste manual Campina Grande (PB)
mask = (codificados["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) & \
       (codificados["UF_SIGLA"] == "PB") & (codificados["MUNICIPIO_CODIGO"].isna())
codificados.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"
codificados = codificados.drop(columns=["MUNICIPIO_NOME_IBGE"], errors="ignore")

# =========================================================
# 6) INEP 2023 — leituras
# =========================================================
df_iniciais = read_excel_smart(ods_iniciais, header=9)
df_finais   = read_excel_smart(ods_finais,   header=9)
df_em       = read_excel_smart(ods_em,       header=9)

def media_por_municipio(df: pd.DataFrame, rotulo_saida: str) -> pd.DataFrame:
    df = df.copy()
    df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    ind = pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce")
    return (pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo_saida: ind})
            .groupby("CO_MUNICIPIO", as_index=False)[rotulo_saida].mean())

ini = media_por_municipio(df_iniciais, "TAXA_APROVACAO_INICIAIS_P")
fin = media_por_municipio(df_finais,   "TAXA_APROVACAO_FINAIS_P")
med = media_por_municipio(df_em,       "TAXA_APROVACAO_MEDIO_P")

# versões em %
ini["TAXA_APROVACAO_INICIAIS_%"] = ini["TAXA_APROVACAO_INICIAIS_P"] * 100
fin["TAXA_APROVACAO_FINAIS_%"]   = fin["TAXA_APROVACAO_FINAIS_P"]   * 100
med["TAXA_APROVACAO_MEDIO_%"]    = med["TAXA_APROVACAO_MEDIO_P"]    * 100

# merge nas médias
res = codificados.copy()
res["MUNICIPIO_CODIGO"] = res["MUNICIPIO_CODIGO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
res = (res.merge(ini, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")
         .merge(fin, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_fin"))
         .merge(med, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_med")))
res.drop(columns=[c for c in ["CO_MUNICIPIO","CO_MUNICIPIO_fin","CO_MUNICIPIO_med"] if c in res.columns], inplace=True)

# arredondamentos
for c in ["TAXA_APROVACAO_INICIAIS_P","TAXA_APROVACAO_FINAIS_P","TAXA_APROVACAO_MEDIO_P"]:
    if c in res.columns: res[c] = res[c].round(4)
for c in ["TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%"]:
    if c in res.columns: res[c] = res[c].round(2)

# limpeza/cortes (iguais ao original)
res = (res.drop(columns=["TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS","TAXA_APROVACAO_MEDIO"], errors="ignore")
          .drop(index=5, errors="ignore"))
res = res.rename(columns=lambda x: x.replace("_P", "") if x.endswith("_P") else x)
res = res.iloc[:18]
# garantir dtype numérico antes de setar valores
res["TAXA_APROVACAO_INICIAIS_%"] = pd.to_numeric(res.get("TAXA_APROVACAO_INICIAIS_%"), errors="coerce")
res["TAXA_APROVACAO_INICIAIS"]   = pd.to_numeric(res.get("TAXA_APROVACAO_INICIAIS"),   errors="coerce")

res.at[1, "TAXA_APROVACAO_INICIAIS_%"] = 90.66
res.at[1, "TAXA_APROVACAO_INICIAIS"]   = 0.9066

res.loc[1, "TAXA_APROVACAO_INICIAIS_%"] = "90.66"
res.loc[1, "TAXA_APROVACAO_INICIAIS"]  = "0.9066"
for col in ["TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%"]:
    res[col] = pd.to_numeric(res[col], errors="coerce")

tabela_uf = (res.groupby("UF_SIGLA")[["TAXA_APROVACAO_INICIAIS_%","TAXA_APROVACAO_FINAIS_%","TAXA_APROVACAO_MEDIO_%"]]
               .mean().round(2).sort_values("TAXA_APROVACAO_INICIAIS_%", ascending=False))

# =========================================================
# 7) Evasão (INEP)
# =========================================================
df_evasao = read_excel_smart(caminho_evasao, header=8)
colunas_desejadas = [
    "NO_REGIAO","NO_UF","CO_MUNICIPIO","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA",
    "1_CAT3_CATFUN","1_CAT3_CATFUN_AI","1_CAT3_CATFUN_01","1_CAT3_CATFUN_02","1_CAT3_CATFUN_03",
    "1_CAT3_CATFUN_04","1_CAT3_CATFUN_05","1_CAT3_CATFUN_06","1_CAT3_CATFUN_07","1_CAT3_CATFUN_08",
    "1_CAT3_CATFUN_09","1_CAT3_CATMED","1_CAT3_CATMED_01","1_CAT3_CATMED_02","1_CAT3_CATMED_03"
]
df_filtrado = df_evasao[colunas_desejadas]
df_filtrado = df_filtrado.rename(columns={
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
})
for col in ["Fundamental - Total","Médio - Total"]:
    df_filtrado[col] = pd.to_numeric(df_filtrado[col].astype(str).str.replace(",", "."), errors="coerce")

# merges com códigos
res["MUNICIPIO_CODIGO"]     = pd.to_numeric(res["MUNICIPIO_CODIGO"], errors="coerce").astype("Int64")
df_filtrado["CO_MUNICIPIO"] = pd.to_numeric(df_filtrado["CO_MUNICIPIO"], errors="coerce").astype("Int64")
res_ok         = res.dropna(subset=["MUNICIPIO_CODIGO"]).copy()
df_filtrado_ok = df_filtrado.dropna(subset=["CO_MUNICIPIO"]).copy()
res_ok["COD_IBGE"]          = res_ok["MUNICIPIO_CODIGO"].astype("Int64").astype(str).str.zfill(7)
df_filtrado_ok["COD_IBGE"]  = df_filtrado_ok["CO_MUNICIPIO"].astype("Int64").astype(str).str.zfill(7)
df_merge = pd.merge(res_ok, df_filtrado_ok, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="inner")

cols_saida = [
    "COD_IBGE","UF_SIGLA","MUNICIPIO_NOME_ALP","NO_MUNICIPIO","NO_LOCALIZACAO","NO_DEPENDENCIA",
    "Fundamental - Total","Médio - Total","TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS"
]
cols_saida = [c for c in cols_saida if c in df_merge.columns]
resultado = df_merge[cols_saida].copy()
resultado.rename(columns={"Fundamental - Total":"Evasão - Fundamental","Médio - Total":"Evasão -Médio"}, inplace=True)

# Outliers (IQR) + winsor
num_cols = [c for c in ["Evasão - Fundamental","Evasão -Médio","TAXA_APROVACAO_INICIAIS","TAXA_APROVACAO_FINAIS"] if c in resultado.columns]
resultado_num = resultado.copy()
for col in num_cols:
    resultado_num[col] = (resultado_num[col].astype(str)
                          .str.replace(",", ".", regex=False)
                          .str.replace("%", "", regex=False)
                          .str.replace("\u2212", "-", regex=False))
    resultado_num[col] = pd.to_numeric(resultado_num[col], errors="coerce")
Q1, Q3 = resultado_num[num_cols].quantile(0.25), resultado_num[num_cols].quantile(0.75)
IQR = Q3 - Q1
low, high = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
mask_out = (resultado_num[num_cols] < low) | (resultado_num[num_cols] > high)
winsor_df = resultado_num.copy()
for col in num_cols: winsor_df[col] = winsor_df[col].clip(lower=low[col], upper=high[col])

winsor_df["Reprovacao_Iniciais"] = (1 - winsor_df["TAXA_APROVACAO_INICIAIS"]) * 100
winsor_df["Reprovacao_Finais"]   = (1 - winsor_df["TAXA_APROVACAO_FINAIS"])   * 100
winsor_df["Urgencia"] = (winsor_df["Evasão - Fundamental"] + winsor_df["Evasão -Médio"] +
                         winsor_df["Reprovacao_Iniciais"] + winsor_df["Reprovacao_Finais"])
urgentes = winsor_df.sort_values("Urgencia", ascending=False).head(20)

# seleção essencial
colunas_essenciais = [
    "MUNICIPIO_NOME_ALP","NO_LOCALIZACAO","NO_DEPENDENCIA",
    "Evasão - Fundamental","Evasão -Médio","TAXA_APROVACAO_INICIAIS",
    "TAXA_APROVACAO_FINAIS","Reprovacao_Iniciais","Reprovacao_Finais","Urgencia"
]
tabela_essencial = urgentes[colunas_essenciais].copy()

# =========================================================
# 8) Histórico 2005–2023 e evolução
# =========================================================
dtb_lookup = (carrega_dtb(ARQ_DTB)[["MUNICIPIO_CODIGO","UF_SIGLA","MUNICIPIO_NOME"]]
              .rename(columns={"MUNICIPIO_CODIGO":"CO_MUNICIPIO"}))
dtb_lookup["CO_MUNICIPIO"] = dtb_lookup["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)

def ensure_key_urgentes(df: pd.DataFrame) -> pd.DataFrame:
    u = df.copy()
    base = u["MUNICIPIO_NOME_ALP"] if "MUNICIPIO_NOME_ALP" in u.columns else u.get("NO_MUNICIPIO")
    u["MUNICIPIO_CHAVE"] = base.apply(chave_municipio)
    return u

def _anos_disponiveis(df: pd.DataFrame, ano_min=2005, ano_max=2023) -> list[int]:
    anos = []
    for c in df.columns:
        m = re.fullmatch(r"VL_INDICADOR_REND_(\d{4})", str(c))
        if m:
            a = int(m.group(1))
            if ano_min <= a <= ano_max: anos.append(a)
    return sorted(set(anos))

def _long_por_municipio_ano(df: pd.DataFrame, etapa_rotulo: str) -> pd.DataFrame:
    df = df.copy()
    if "CO_MUNICIPIO" not in df.columns: raise KeyError("Planilha não possui CO_MUNICIPIO.")
    df["CO_MUNICIPIO"] = df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    anos = _anos_disponiveis(df, 2005, 2023)
    if not anos: raise KeyError("Nenhuma VL_INDICADOR_REND_YYYY (2005–2023).")
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    num = df[["CO_MUNICIPIO"] + cols].copy()
    for c in cols: num[c] = pd.to_numeric(num[c], errors="coerce")
    long_df = num.melt(id_vars="CO_MUNICIPIO", value_vars=cols, var_name="COL", value_name=etapa_rotulo)
    long_df["ANO"] = long_df["COL"].str.extract(r"(\d{4})").astype(int)
    long_df.drop(columns=["COL"], inplace=True)
    return long_df.groupby(["CO_MUNICIPIO","ANO"], as_index=False)[etapa_rotulo].mean()

ini_hist = _long_por_municipio_ano(df_iniciais, "APROVACAO_INICIAIS")
fin_hist = _long_por_municipio_ano(df_finais,   "APROVACAO_FINAIS")
med_hist = _long_por_municipio_ano(df_em,       "APROVACAO_MEDIO")
evolucao = (ini_hist.merge(fin_hist, on=["CO_MUNICIPIO","ANO"], how="outer")
                    .merge(med_hist, on=["CO_MUNICIPIO","ANO"], how="outer"))
evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean(axis=1, skipna=True)
for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]:
    evolucao[c + "_%"] = (evolucao[c] * 100).round(2)
evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")
evolucao = evolucao[[ "UF_SIGLA","MUNICIPIO_NOME","CO_MUNICIPIO","ANO",
                      "APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL",
                      "APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%","APROVACAO_MEDIA_GERAL_%"]]
evolucao = evolucao.sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True)

urgentes = ensure_key_urgentes(urgentes)
evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)
evolucao_filtrada = (evolucao.merge(urgentes[["UF_SIGLA","MUNICIPIO_CHAVE"]].drop_duplicates(),
                                    on=["UF_SIGLA","MUNICIPIO_CHAVE"], how="inner")
                             .sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True))

for col in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL",
            "APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%","APROVACAO_MEDIA_GERAL_%"]:
    if col in evolucao_filtrada.columns:
        evolucao_filtrada[col] = evolucao_filtrada.groupby("MUNICIPIO_CHAVE")[col].transform(
            lambda x: x.fillna(x.median(skipna=True))
        )

# =========================================================
# 9) Gráficos de apoio (matplotlib)
# =========================================================
evolucao_filtrada["PERIODO"] = pd.cut(
    evolucao_filtrada["ANO"],
    bins=[2004,2007,2011,2015,2019,2023],
    labels=["2005–2007","2009–2011","2013–2015","2017–2019","2021–2023"]
)
grouped = (evolucao_filtrada.groupby(["MUNICIPIO_NOME","PERIODO"])
           [["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]]
           .mean().reset_index())
media_geral = (grouped.groupby("PERIODO")[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]]
               .mean()*100)
ax = media_geral.plot(marker="o", figsize=(9,5))
ax.set_title("Média de aprovação por etapa (2005–2023, em janelas de 2 anos)", fontsize=12, weight="bold")
ax.set_ylabel("%"); ax.set_ylim(0,100)

gap = evolucao_filtrada.groupby("ANO")[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean()*100
gap["queda_iniciais_finais"] = gap["APROVACAO_INICIAIS"] - gap["APROVACAO_FINAIS"]
gap["queda_finais_medio"]    = gap["APROVACAO_FINAIS"]   - gap["APROVACAO_MEDIO"]
gap[["queda_iniciais_finais","queda_finais_medio"]].plot(figsize=(10,5), marker="o")
plt.title("Diferença de aprovação entre etapas (2005–2023)")
plt.ylabel("Diferença percentual (p.p.)")
plt.axhline(0, color="black", linestyle="--")

# =========================================================
# 10) Funções de gráficos (Plotly)
# =========================================================
def _minmax(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    return pd.Series(0.5, index=s.index) if s.max() == s.min() else (s - s.min()) / (s.max() - s.min())

urg = urgentes.copy()
if {"NO_LOCALIZACAO","NO_DEPENDENCIA"}.issubset(urg.columns):
    sel = (urg["NO_LOCALIZACAO"].astype(str).str.upper() == "TOTAL") & \
          (urg["NO_DEPENDENCIA"].astype(str).str.upper() == "TOTAL")
    if sel.any(): urg = urg[sel].copy()

col_nome_urg = "NO_MUNICIPIO" if "NO_MUNICIPIO" in urg.columns else "MUNICIPIO_NOME_ALP"
urg = (urg.rename(columns={
        col_nome_urg: "MUNICIPIO_NOME",
        "Evasão - Fundamental": "EVASAO_FUNDAMENTAL",
        "Evasão -Médio": "EVASAO_MEDIO"
    })[["MUNICIPIO_NOME","EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]]
      .groupby("MUNICIPIO_NOME", as_index=False).mean(numeric_only=True))

evo = evolucao_filtrada.copy()
evo["MUNICIPIO_NOME"] = evo["MUNICIPIO_NOME"].astype(str).str.strip()
for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS"]:
    if c in evo.columns:
        mean_val = pd.to_numeric(evo[c], errors="coerce").mean()
        evo[c + "_%"] = (100 * pd.to_numeric(evo[c], errors="coerce")) if (pd.notna(mean_val) and mean_val <= 1.5) \
                         else pd.to_numeric(evo[c], errors="coerce")
    else:
        evo[c + "_%"] = np.nan

df_static = (evo.groupby(["MUNICIPIO_NOME"], as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
                .mean(numeric_only=True)
                .merge(urg, on="MUNICIPIO_NOME", how="left"))
df_static["GAP_APROV_%"] = df_static["APROVACAO_INICIAIS_%"] - df_static["APROVACAO_FINAIS_%"]
aprov_finais_norm = 1 - _minmax(df_static["APROVACAO_FINAIS_%"].fillna(df_static["APROVACAO_FINAIS_%"].median()))
evasao_norm       = _minmax(df_static["EVASAO_FUNDAMENTAL"].fillna(df_static["EVASAO_FUNDAMENTAL"].median()))
gap_norm          = _minmax(df_static["GAP_APROV_%"].fillna(0))
df_static["SCORE_RISCO"] = 0.5*aprov_finais_norm + 0.4*evasao_norm + 0.1*gap_norm

def graf_tendencia_geral(evo=evo):
    t = evo.dropna(subset=["ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]).copy()
    m = t.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean()
    melted = m.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
    fig = px.line(melted, x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                  title="Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)")
    fig.update_layout(yaxis_tickformat=".1f")
    return fig

def graf_ranking_risco(base=df_static, top_n=20):
    t = base.dropna(subset=["SCORE_RISCO"]).copy().sort_values("SCORE_RISCO", ascending=False).head(top_n)
    fig = px.bar(t, x="SCORE_RISCO", y="MUNICIPIO_NOME", orientation="h",
                 hover_data=["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL","GAP_APROV_%"],
                 title=f"Top {top_n} — Ranking de Risco (baixa aprov finais + alta evasão + gap)",
                 labels={"MUNICIPIO_NOME":"Município","SCORE_RISCO":"Score de Risco (0–1)"})
    fig.update_yaxes(categoryorder="total ascending")
    return fig

def graf_tendencia_municipio(municipio_nome, evo=evo):
    t = evo[evo["MUNICIPIO_NOME"].astype(str).str.strip() == str(municipio_nome).strip()].copy()
    t = t.dropna(subset=["ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"])
    if t.empty: raise ValueError(f"Município '{municipio_nome}' não encontrado.")
    m = t.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean()
    melted = m.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
    fig = px.line(melted, x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                  title=f"{municipio_nome} — Evolução de Aprovação (Iniciais vs Finais)")
    fig.update_layout(yaxis_tickformat=".1f")
    return fig

def graf_quadrantes(base=df_static, usar_tamanho_por_risco=True):
    t = base.dropna(subset=["APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]).copy()
    if t.empty: raise ValueError("Sem dados para plotar quadrantes.")
    cut_x, cut_y = t["APROVACAO_FINAIS_%"].median(), t["EVASAO_FUNDAMENTAL"].median()
    conds = [
        (t["APROVACAO_FINAIS_%"] < cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] < cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
    ]
    labels = ["Crítico (aprov baixa, evas alta)","Atenção (aprov alta, evas alta)",
              "Apoio pedagógico (aprov baixa, evasão baixa)","OK (aprov alta, evasão baixa)"]
    t["Quadrante"] = np.select(conds, labels)
    t["LABEL"] = t["MUNICIPIO_NOME"].str.title().str.slice(0, 18)
    pad_x = max(1.0, (t["APROVACAO_FINAIS_%"].max() - t["APROVACAO_FINAIS_%"].min())*0.06)
    pad_y = max(0.5, (t["EVASAO_FUNDAMENTAL"].max() - t["EVASAO_FUNDAMENTAL"].min())*0.08)
    xr = [t["APROVACAO_FINAIS_%"].min()-pad_x, t["APROVACAO_FINAIS_%"].max()+pad_x]
    yr = [t["EVASAO_FUNDAMENTAL"].min()-pad_y, t["EVASAO_FUNDAMENTAL"].max()+pad_y]
    size_arg = "SCORE_RISCO" if usar_tamanho_por_risco and "SCORE_RISCO" in t.columns else None
    fig = px.scatter(
        t, x="APROVACAO_FINAIS_%", y="EVASAO_FUNDAMENTAL",
        color="Quadrante", size=size_arg, size_max=26,
        hover_data=["MUNICIPIO_NOME","APROVACAO_INICIAIS_%","GAP_APROV_%","SCORE_RISCO"],
        text="LABEL",
        title="Quadrantes — Aprovação (Anos Finais) × Evasão (Fundamental)",
        labels={"APROVACAO_FINAIS_%":"Aprovação Finais (%)","EVASAO_FUNDAMENTAL":"Evasão Fundamental (%)"},
    )
    fig.update_traces(marker=dict(line=dict(width=1, color="white"), opacity=0.9),
                      textposition="top center", textfont=dict(size=11))
    fig.add_shape(type="rect", x0=xr[0], x1=cut_x, y0=cut_y, y1=yr[1], fillcolor="red",    opacity=0.06, line_width=0)
    fig.add_shape(type="rect", x0=cut_x, x1=xr[1], y0=cut_y, y1=yr[1], fillcolor="orange", opacity=0.06, line_width=0)
    fig.add_shape(type="rect", x0=xr[0], x1=cut_x, y0=yr[0], y1=cut_y, fillcolor="gray",   opacity=0.06, line_width=0)
    fig.add_shape(type="rect", x0=cut_x, x1=xr[1], y0=yr[0], y1=cut_y, fillcolor="green",  opacity=0.06, line_width=0)
    fig.add_vline(x=cut_x, line_width=3, line_dash="dash"); fig.add_hline(y=cut_y, line_width=3, line_dash="dash")
    return fig

# ===========================================
# PAINEL STREAMLIT (usa as variáveis globais)
# ===========================================
import streamlit as st

st.set_page_config(page_title="IA • Aprovação, Evasão e Urgência", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel (hotfix)")

def _slug(s: object) -> str:
    if pd.isna(s): return ""
    t = unicodedata.normalize("NFKD", str(s)).encode("ASCII","ignore").decode("ASCII")
    t = t.replace("–","-").replace("—","-").strip().lower()
    t = re.sub(r"[^a-z0-9]+","_", t)
    return re.sub(r"_+","_", t).strip("_")

def _to_num(x: pd.Series) -> pd.Series:
    return pd.to_numeric(
        x.astype(str).str.replace("%","",regex=False).str.replace(",","",regex=False).str.replace(" ","",regex=False),
        errors="coerce"
    )

def _minmax_streamlit(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.dropna().empty or s.max() == s.min(): return pd.Series(0.5, index=s.index)
    return (s - s.min())/(s.max()-s.min())

def _prepare_urgentes(urg: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(urg, pd.DataFrame) or urg.empty:
        return pd.DataFrame(columns=["MUNICIPIO_NOME","EVASAO_FUNDAMENTAL","CHAVE"])
    u = urg.copy()
    nome_col = next((c for c in ["MUNICIPIO_NOME","MUNICIPIO_NOME_ALP","NO_MUNICIPIO"] if c in u.columns), None)
    if not nome_col: return pd.DataFrame(columns=["MUNICIPIO_NOME","EVASAO_FUNDAMENTAL","CHAVE"])
    u = u.rename(columns={nome_col:"MUNICIPIO_NOME"})
    u["MUNICIPIO_NOME"] = u["MUNICIPIO_NOME"].astype(str).str.strip()
    col_evas = next((c for c in ["EVASAO_FUNDAMENTAL","Evasão - Fundamental","Fundamental - Total"] if c in u.columns), None)
    if col_evas is None:
        for c in u.columns:
            sc = _slug(c)
            if "evas" in sc and ("fund" in sc or "fundamental" in sc): col_evas = c; break
    u["EVASAO_FUNDAMENTAL"] = _to_num(u[col_evas]) if col_evas else np.nan
    o = (u.groupby("MUNICIPIO_NOME", as_index=False)["EVASAO_FUNDAMENTAL"].mean(numeric_only=True))
    o["CHAVE"] = o["MUNICIPIO_NOME"].map(_slug)
    return o

@st.cache_data(show_spinner=False)
def _build_static(evo: pd.DataFrame, urg: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(evo, pd.DataFrame) or evo.empty: return pd.DataFrame()
    t = evo.copy()
    if "MUNICIPIO_NOME" not in t.columns:
        for c in ["NO_MUNICIPIO","MUNICIPIO_NOME_ALP"]:
            if c in t.columns: t = t.rename(columns={c:"MUNICIPIO_NOME"}); break
    t["MUNICIPIO_NOME"] = t["MUNICIPIO_NOME"].astype(str).str.strip()
    for base in ["APROVACAO_INICIAIS","APROVACAO_FINAIS"]:
        pct = base + "_%"
        if pct not in t.columns:
            if base in t.columns:
                m = pd.to_numeric(t[base], errors="coerce").mean()
                t[pct] = 100*pd.to_numeric(t[base], errors="coerce") if pd.notna(m) and m<=1.5 else pd.to_numeric(t[base], errors="coerce")
            else:
                t[pct] = np.nan
        else:
            t[pct] = _to_num(t[pct])
    base_static = (t.groupby("MUNICIPIO_NOME", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean(numeric_only=True))
    base_static["CHAVE"] = base_static["MUNICIPIO_NOME"].map(_slug)
    urg2 = _prepare_urgentes(urg)
    df = base_static.merge(urg2[["CHAVE","EVASAO_FUNDAMENTAL"]], on="CHAVE", how="left").drop(columns=["CHAVE"])
    for c in ["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]:
        if c in df.columns: df[c] = _to_num(df[c])
    df["GAP_APROV_%"] = df["APROVACAO_INICIAIS_%"] - df["APROVACAO_FINAIS_%"]
    df["SCORE_RISCO"] = 0.5*(1 - _minmax_streamlit(df["APROVACAO_FINAIS_%"])) + \
                        0.4*_minmax_streamlit(df["EVASAO_FUNDAMENTAL"]) + \
                        0.1*_minmax_streamlit(df["GAP_APROV_%"].fillna(0))
    return df

evo_safe = globals().get("evolucao_filtrada", pd.DataFrame())
urg_safe = globals().get("urgentes", pd.DataFrame())
df_static_ready = _build_static(evo_safe, urg_safe)

c1,c2,c3,c4 = st.columns(4)
with c1: st.metric("Municípios no recorte", len(df_static_ready["MUNICIPIO_NOME"].unique()) if not df_static_ready.empty else "–")
with c2:
    v = df_static_ready["APROVACAO_FINAIS_%"].mean() if not df_static_ready.empty else np.nan
    st.metric("Aprovação — Finais (média)", f"{v:.1f}%" if pd.notna(v) else "–")
with c3:
    v = df_static_ready["EVASAO_FUNDAMENTAL"].mean() if ("EVASAO_FUNDAMENTAL" in df_static_ready.columns and not df_static_ready.empty) else np.nan
    st.metric("Evasão — Fundamental (média)", f"{v:.1f}%" if pd.notna(v) else "–")
with c4:
    v = df_static_ready["SCORE_RISCO"].mean() if ("SCORE_RISCO" in df_static_ready.columns and not df_static_ready.empty) else np.nan
    st.metric("Score de risco (média)", f"{v:.2f}" if pd.notna(v) else "–")

st.divider()
tab_overview, tab_grafs, tab_tables, tab_diag = st.tabs(["Visão geral","Gráficos","Tabelas","Diagnóstico"])

with tab_overview:
    st.subheader("📌 Introdução")
    st.markdown("""
    Este site apresenta os resultados da análise de dados cujo objetivo foi **mapear os municípios com maior urgência educacional**
    e avaliar como os projetos do **Instituto Alpargatas (2020-2024)** estão respondendo a esses desafios.
    A análise foi baseada em dados do Instituto Alpargatas, do **INEP (Censo Escolar)** e do **IDEB**, resultando em uma **métrica de urgência** para a priorização de ações.

    ### Metodologia de Análise
    Para alcançar o objetivo, a análise seguiu uma metodologia focada na criação de um **ranking de municípios críticos**.
    A abordagem principal foi o desenvolvimento de uma métrica de **"Grau de Urgência" educacional**, que permitiu classificar as cidades e direcionar os esforços de forma estratégica.
    A análise consolidou dados de desempenho escolar, **taxas de evasão** e **aprovação** para gerar um índice que reflete a necessidade de intervenção em cada localidade.
    """)

with tab_grafs:
    st.subheader("📊 Gráficos disponíveis")
    if "graf_tendencia_geral" in globals() and callable(graf_tendencia_geral) and not evo_safe.empty:
        st.plotly_chart(graf_tendencia_geral(evo=evo_safe), use_container_width=True)
    elif not evo_safe.empty and {"ANO","APROVACAO_INICIAIS","APROVACAO_FINAIS"}.issubset(evo_safe.columns):
        tmp = evo_safe.copy()
        for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS"]:
            m = pd.to_numeric(tmp[c], errors="coerce").mean()
            tmp[c + "_%"] = 100*pd.to_numeric(tmp[c], errors="coerce") if pd.notna(m) and m<=1.5 else pd.to_numeric(tmp[c], errors="coerce")
        m = tmp.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean(numeric_only=True)
        st.plotly_chart(px.line(m.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
                                x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                                title="Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)"),
                        use_container_width=True)

    # também renderiza os gráficos matplotlib
    figs = [plt.figure(n) for n in plt.get_fignums()]
    if figs:
        st.subheader("🖼️ Gráficos (imagem) gerados no código")
        for f in figs:
            st.pyplot(f, use_container_width=True)

with tab_tables:
    st.subheader("Bases consolidadas")
    if not df_static_ready.empty:
        st.markdown("**df_static (métricas por município)**")
        st.dataframe(df_static_ready.sort_values("SCORE_RISCO", ascending=False), use_container_width=True)
        st.download_button("Baixar df_static.csv", df_static_ready.to_csv(index=False).encode("utf-8"),
                           file_name="df_static.csv", use_container_width=True)
    else:
        st.info("df_static ainda não foi formado (ou está vazio).")

    if isinstance(evo_safe, pd.DataFrame) and not evo_safe.empty:
        prefer = ["UF_SIGLA","MUNICIPIO_NOME","ANO",
                  "APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%",
                  "APROVACAO_MEDIO_%","APROVACAO_MEDIA_GERAL_%"]
        cols = [c for c in prefer if c in evo_safe.columns]
        st.markdown("**evolucao_filtrada**")
        st.dataframe(evo_safe[cols] if cols else evo_safe.head(50), use_container_width=True)

    if isinstance(urg_safe, pd.DataFrame) and not urg_safe.empty:
        st.markdown("**urgentes (cru)**")
        st.dataframe(urg_safe, use_container_width=True)

with tab_diag:
    st.subheader("Diagnóstico")
    def _diag(df, nome):
        if isinstance(df, pd.DataFrame) and not df.empty:
            st.success(f"{nome} OK — shape {df.shape}")
            st.dataframe(df.head(10), use_container_width=True)
            st.caption(df.dtypes.astype(str))
        else:
            st.error(f"{nome} ausente ou vazio.")
    _diag(df_static_ready, "df_static_ready")
    _diag(evo_safe, "evolucao_filtrada")
    _diag(urg_safe, "urgentes")
