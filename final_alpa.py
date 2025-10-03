
import pandas as pd
import unicodedata
from pathlib import Path
import re
import matplotlib.pyplot as plt

# ============================
# 0) AJUSTE OS CAMINHOS AQUI
# ============================
ARQ_ALP = "dados/Dados_alpa.xlsx"
ARQ_DTB = "dados/dtb_municipios.ods"

# =========================================================
# 1) Utilitários curtos
# =========================================================
def nrm(txt: object) -> str:
    """Normaliza: remove acentos, vira CAIXA-ALTA e tira espaços. NaN -> ''."""
    if pd.isna(txt):
        return ""
    s = str(txt)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    """
    Chave 'suave' para casamentos:
    - caixa alta
    - remove pontuações leves
    - corta sufixos que atrapalham match (ex.: ' - ...', ' MIXING CENTER').
    """
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n:           # corta qualquer coisa depois de ' - '
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

# =========================================================
# 2) Ler & limpar DTB/IBGE
# =========================================================
def carrega_dtb(path: str) -> pd.DataFrame:
    """Lê DTB/IBGE e devolve DataFrame com colunas-chave já limpas e prontas."""
    # Mapa Nome_UF -> Sigla
    UF_SIGLAS = {
        "ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAZONAS":"AM","BAHIA":"BA",
        "CEARÁ":"CE","DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","GOIÁS":"GO",
        "MARANHÃO":"MA","MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG",
        "PARÁ":"PA","PARAÍBA":"PB","PARANÁ":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI",
        "RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN","RIO GRANDE DO SUL":"RS",
        "RONDÔNIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SÃO PAULO":"SP",
        "SERGIPE":"SE","TOCANTINS":"TO"
    }

    # A DTB costuma trazer linhas de título/cabeçalhos antes dos dados
    # → usamos skiprows=6 (ajuste se necessário).
    raw = pd.read_excel(path, engine="odf", skiprows=6)

    # Seleciona/renomeia o que interessa e padroniza
    dtb = (raw.rename(columns={
                "UF": "UF_COD_NUM",
                "Nome_UF": "UF_NOME",
                "Código Município Completo": "MUNICIPIO_CODIGO",
                "Nome_Município": "MUNICIPIO_NOME"
            })[["UF_COD_NUM","UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]]
           .dropna(subset=["UF_NOME","MUNICIPIO_CODIGO","MUNICIPIO_NOME"]))

    dtb["UF_SIGLA"]           = dtb["UF_NOME"].astype(str).str.upper().map(UF_SIGLAS)
    dtb["MUNICIPIO_CODIGO"]   = dtb["MUNICIPIO_CODIGO"].astype(str).str.zfill(7)
    dtb["MUNICIPIO_NOME"]     = dtb["MUNICIPIO_NOME"].astype(str).str.upper().str.strip()
    dtb["MUNICIPIO_CHAVE"]    = dtb["MUNICIPIO_NOME"].apply(chave_municipio)

    return dtb[["UF_SIGLA","MUNICIPIO_CODIGO","MUNICIPIO_NOME","MUNICIPIO_CHAVE"]]

# =========================================================
# 3) Ler abas do arquivo Alpargatas (2020–2025) e extrair cidade/UF
# =========================================================
def carrega_alpargatas(path: str) -> pd.DataFrame:
    """Lê todas as abas (2020–2025), detecta header e extrai CIDADES/UF em um único DataFrame."""
    xls = pd.ExcelFile(path)
    abas = [a for a in xls.sheet_names if any(str(ano) in a for ano in range(2020, 2026))]
    if not abas:
        raise RuntimeError("Nenhuma aba 2020–2025 encontrada no arquivo Alpargatas.")

    frames = []
    for aba in abas:
        # Lê as primeiras linhas sem header só para acharmos onde começa CIDADES/UF
        nohdr = pd.read_excel(path, sheet_name=aba, header=None, nrows=400)
        hdr   = acha_linha_header_cidades_uf(nohdr)
        if hdr is None:
            print(f"[AVISO] Não achei cabeçalho CIDADES/UF na aba '{aba}'. Pulando…")
            continue

        df = pd.read_excel(path, sheet_name=aba, header=hdr)

        # Descobre as colunas "Cidades" e "UF" em qualquer grafia
        cmap = {c: nrm(c) for c in df.columns}
        c_cid = next((orig for orig, norm in cmap.items() if norm == "CIDADES"), None)
        c_uf  = next((orig for orig, norm in cmap.items() if norm == "UF"), None)
        if not c_cid or not c_uf:
            print(f"[AVISO] Colunas 'CIDADES'/'UF' não encontradas após header na aba '{aba}'.")
            continue

        tmp = (df[[c_cid, c_uf]].copy()
                 .rename(columns={c_cid:"MUNICIPIO_NOME_ALP", c_uf:"UF_SIGLA"}))
        tmp["MUNICIPIO_NOME_ALP"] = tmp["MUNICIPIO_NOME_ALP"].astype(str).str.upper().str.strip()
        tmp["UF_SIGLA"]           = tmp["UF_SIGLA"].astype(str).str.strip()
        tmp = tmp.dropna(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
        tmp = tmp[tmp["MUNICIPIO_NOME_ALP"].str.len() > 0]

        tmp["MUNICIPIO_CHAVE"] = tmp["MUNICIPIO_NOME_ALP"].apply(chave_municipio)
        tmp["FONTE_ABA"]       = aba
        frames.append(tmp)

    if not frames:
        raise RuntimeError("Nenhuma aba válida foi processada (CIDADES/UF não encontrado).")

    # remove duplicados entre abas (mesma cidade/UF pode aparecer em mais de uma aba)
    return pd.concat(frames, ignore_index=True).drop_duplicates(["MUNICIPIO_CHAVE","UF_SIGLA"])

# =========================================================
# 4) Cruzamento e saída
# =========================================================
def cruzar_e_salvar(dtb: pd.DataFrame, alpa: pd.DataFrame, saida_dir: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Casa Alpargatas × IBGE usando (MUNICIPIO_CHAVE, UF_SIGLA).
    Se 'saida_dir' for informado, salva CSVs.
    Retorna (codificados, nao_encontrados).
    """
    codificados = alpa.merge(
        dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE")
    )

    nao_encontrados = (codificados[codificados["MUNICIPIO_CODIGO"].isna()]
                       .drop_duplicates(subset=["MUNICIPIO_NOME_ALP","UF_SIGLA"])
                       .sort_values(["UF_SIGLA","MUNICIPIO_NOME_ALP"]))

    if saida_dir:
        Path(saida_dir).mkdir(parents=True, exist_ok=True)
        codificados.to_csv(Path(saida_dir, "municipios_alpargatas_codificados.csv"), index=False, encoding="utf-8")
        nao_encontrados.to_csv(Path(saida_dir, "municipios_nao_encontrados_para_tratar.csv"), index=False, encoding="utf-8")

    return codificados, nao_encontrados

# =========================================================
# 5) Execução
# =========================================================
if __name__ == "__main__":
    print("Lendo DTB/IBGE…")
    dtb  = carrega_dtb(ARQ_DTB)

    print("Lendo abas do arquivo Alpargatas…")
    alpa = carrega_alpargatas(ARQ_ALP)

    print("Cruzando bases em memória…")
    codificados, nao_encontrados = cruzar_e_salvar(dtb, alpa)

    # >>> ajuste CAMPINA GRANDE
    mask = (
        (codificados["MUNICIPIO_NOME_ALP"].str.contains("CAMPINA GRANDE", case=False, na=False)) &
        (codificados["UF_SIGLA"] == "PB") &
        (codificados["MUNICIPIO_CODIGO"].isna())
    )
    codificados.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"
    codificados = codificados.drop(columns=["MUNICIPIO_NOME_IBGE"], errors="ignore")

ods_iniciais = "dados/anos_iniciais.xlsx"
ods_finais = "dados/anos_finais.xlsx"
ods_em = "dados/ensino_medio.xlsx"

df_iniciais = pd.read_excel(ods_iniciais, header= 9)
df_finais = pd.read_excel(ods_finais, header = 9)
df_em = pd.read_excel(ods_em, header = 9)

import numpy as np

# ============================================================
# 1) Função utilitária: calcula a MÉDIA do indicador (VL_INDICADOR_REND_2023)
#    por município em um DataFrame qualquer, e devolve
#    um DataFrame com duas colunas: CO_MUNICIPIO e <rótulo_saida>.
def media_por_municipio(df: pd.DataFrame, rotulo_saida: str) -> pd.DataFrame:
    # Faz uma cópia para não alterar o df original fora da função
    df = df.copy()

    # 1.1) Padroniza o código do município (IBGE) como string com 7 dígitos.
    #      - extrai apenas números (7 dígitos) caso venham misturados
    #      - preenche com zeros à esquerda se precisar (zfill)
    df["CO_MUNICIPIO"] = (
        df["CO_MUNICIPIO"]
        .astype(str)
        .str.extract(r"(\d{7})", expand=False)  # se tiver mais coisa na célula, pega só os 7 dígitos
        .str.zfill(7)
    )

    # 1.2) Converte a coluna do indicador para numérico.
    #      - errors='coerce' transforma valores inválidos ('-', strings etc.) em NaN
    ind = pd.to_numeric(df["VL_INDICADOR_REND_2023"], errors="coerce")

    # 1.3) Calcula a MÉDIA do indicador por município (ignora NaN automaticamente).
    #      - faz um DataFrame com as duas colunas: CO_MUNICIPIO e rotulo_saida
    #      - agrupa por CO_MUNICIPIO e calcula mean()
    out = (
        pd.DataFrame({"CO_MUNICIPIO": df["CO_MUNICIPIO"], rotulo_saida: ind})
        .groupby("CO_MUNICIPIO", as_index=False)[rotulo_saida]
        .mean()
    )

    # Devolve um DF com CO_MUNICIPIO e a média do indicador
    return out


# 2) Calcula as três TABELAS (uma para cada etapa):
#    - anos iniciais
#    - anos finais
#    - ensino médio
#    Cada uma com a coluna "CO_MUNICIPIO" + "TAXA_APROVACAO_<ETAPA>_P"

ini = media_por_municipio(df_iniciais, "TAXA_APROVACAO_INICIAIS_P")
fin = media_por_municipio(df_finais,   "TAXA_APROVACAO_FINAIS_P")
med = media_por_municipio(df_em,       "TAXA_APROVACAO_MEDIO_P")

# ============================================================
# Cria também colunas em percentual
# ============================================================
ini["TAXA_APROVACAO_INICIAIS_%"] = ini["TAXA_APROVACAO_INICIAIS_P"] * 100
fin["TAXA_APROVACAO_FINAIS_%"]   = fin["TAXA_APROVACAO_FINAIS_P"]   * 100
med["TAXA_APROVACAO_MEDIO_%"]    = med["TAXA_APROVACAO_MEDIO_P"]    * 100

# ============================================================
# 3) Faz o MERGE com a sua base "codificados" (municípios alvo):
#    - garante MUNICIPIO_CODIGO com 7 dígitos
#    - junta (left) as médias calculadas de cada etapa por código IBGE
#    - remove colunas duplicadas de CO_MUNICIPIO criadas pelos merges
# ============================================================
# --- GUARDA: garante que 'codificados' existe em memória ---
def _build_codificados():
    dtb  = carrega_dtb(ARQ_DTB)
    alpa = carrega_alpargatas(ARQ_ALP)
    cod, nao = cruzar_e_salvar(dtb, alpa)

    # ajuste CAMPINA GRANDE (PB)
    mask = (
        cod["MUNICIPIO_NOME_ALP"].astype(str).str.contains("CAMPINA GRANDE", case=False, na=False, regex=False)
        & (cod["UF_SIGLA"] == "PB")
        & (cod["MUNICIPIO_CODIGO"].isna())
    )
    cod.loc[mask, "MUNICIPIO_CODIGO"] = "2504009"
    cod = cod.drop(columns=["MUNICIPIO_NOME_IBGE"], errors="ignore")
    return cod

try:
    codificados  # verifica se já existe
except NameError:
    codificados = _build_codificados()

res = codificados.copy()

# 3.1) Padroniza o código do município na base principal
res["MUNICIPIO_CODIGO"] = (
    res["MUNICIPIO_CODIGO"]
    .astype(str)
    .str.extract(r"(\d{7})", expand=False)
    .str.zfill(7)
)

# 3.2) MERGE com as três tabelas calculadas (um left-join para manter todos os municípios da base)
#      Observação: usamos suffixes diferentes nos merges 2 e 3 para evitar conflitos de nomes
res = (
    res
    .merge(ini, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left")                                  # anos iniciais
    .merge(fin, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_fin"))           # anos finais
    .merge(med, left_on="MUNICIPIO_CODIGO", right_on="CO_MUNICIPIO", how="left", suffixes=("", "_med"))           # ensino médio
)

# 3.3) Remove colunas CO_MUNICIPIO repetidas (geradas pelos merges)
for c in ["CO_MUNICIPIO", "CO_MUNICIPIO_fin", "CO_MUNICIPIO_med"]:
    if c in res.columns:
        res.drop(columns=c, inplace=True)

# ============================================================
# 4) Arredonda para Visualização:
#    - *_P (proporção) com 4 casas
#    - *_% (percentual) com 2 casas
# ============================================================
for c in ["TAXA_APROVACAO_INICIAIS_P", "TAXA_APROVACAO_FINAIS_P", "TAXA_APROVACAO_MEDIO_P"]:
    if c in res.columns:
        res[c] = res[c].round(4)

for c in ["TAXA_APROVACAO_INICIAIS_%", "TAXA_APROVACAO_FINAIS_%", "TAXA_APROVACAO_MEDIO_%"]:
    if c in res.columns:
        res[c] = res[c].round(2)

# ============================================================
# 5) Prévia das colunas principais para conferir o resultado
# ============================================================
cols_show = [
    "MUNICIPIO_CODIGO", "UF_SIGLA", "MUNICIPIO_NOME_ALP",
    "TAXA_APROVACAO_INICIAIS_P", "TAXA_APROVACAO_FINAIS_P", "TAXA_APROVACAO_MEDIO_P",
    "TAXA_APROVACAO_INICIAIS_%", "TAXA_APROVACAO_FINAIS_%", "TAXA_APROVACAO_MEDIO_%"
]

# 1) Remover colunas que estão vazias ou duplicadas
cols_remover = ["TAXA_APROVACAO_INICIAIS", "TAXA_APROVACAO_FINAIS", "TAXA_APROVACAO_MEDIO"]
res = res.drop(columns=cols_remover, errors="ignore")
res = res.drop(index=5)  # remove a linha de índice 3
# 2) Renomear colunas removendo o sufixo "_P"
res = res.rename(columns=lambda x: x.replace("_P", "") if x.endswith("_P") else x)
# 3) Definir que a tabela acaba no útimo municipio do dataset do alpargatas
res = res.iloc[:18]

# Preenchimento dos valores nulos (Baía da Traição), apoós rápida checagem no dataset do inep 
# Coletamos a média dos últimos anos preenchidos a substituímos na nossa tabela
res.loc[1, "TAXA_APROVACAO_INICIAIS_%"] = "90.66"
res.loc[1, "TAXA_APROVACAO_INICIAIS"] = "0.9066"

# Garantir que as colunas sejam numéricas
for col in ["TAXA_APROVACAO_INICIAIS_%", "TAXA_APROVACAO_FINAIS_%", "TAXA_APROVACAO_MEDIO_%"]:
    res[col] = pd.to_numeric(res[col], errors="coerce")

# Resumo por estado
tabela_uf = (
    res.groupby("UF_SIGLA")[["TAXA_APROVACAO_INICIAIS_%", "TAXA_APROVACAO_FINAIS_%", "TAXA_APROVACAO_MEDIO_%"]]
    .mean()
    .round(2)
    .sort_values("TAXA_APROVACAO_INICIAIS_%", ascending=False)
)

caminho_evasao = "dados/evasao.ods"
df_evasao = pd.read_excel(caminho_evasao, header = 8)

# selecionar apenas as colunas que você quer
colunas_desejadas = [
    "NO_REGIAO",
    "NO_UF",
    "CO_MUNICIPIO",
    "NO_MUNICIPIO",
    "NO_LOCALIZACAO",
    "NO_DEPENDENCIA",
    "1_CAT3_CATFUN",
    "1_CAT3_CATFUN_AI",
    "1_CAT3_CATFUN_01",
    "1_CAT3_CATFUN_02",
    "1_CAT3_CATFUN_03",
    "1_CAT3_CATFUN_04",
    "1_CAT3_CATFUN_05",
    "1_CAT3_CATFUN_06",
    "1_CAT3_CATFUN_07",
    "1_CAT3_CATFUN_08",
    "1_CAT3_CATFUN_09",
    "1_CAT3_CATMED",
    "1_CAT3_CATMED_01",
    "1_CAT3_CATMED_02",
    "1_CAT3_CATMED_03",

]

df_filtrado = df_evasao[colunas_desejadas]

mapa_colunas = {
    # Fundamental
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

    # Médio
    "1_CAT3_CATMED": "Médio - Total",
    "1_CAT3_CATMED_01": "Médio - 1ª série",
    "1_CAT3_CATMED_02": "Médio - 2ª série",
    "1_CAT3_CATMED_03": "Médio - 3ª série",
}

df_filtrado = df_filtrado.rename(columns=mapa_colunas)


# garantir que as taxas são numéricas
for col in ["Fundamental - Total", "Médio - Total"]:
    df_filtrado[col] = pd.to_numeric(
        df_filtrado[col].astype(str).str.replace(",", "."), errors="coerce"
    )

# maior taxa no Fundamental
mais_alto_fund = df_filtrado.loc[df_filtrado["Fundamental - Total"].idxmax(),
                               ["NO_MUNICIPIO", "Fundamental - Total", "Médio - Total"]]

# maior taxa no Médio
mais_alto_med = df_filtrado.loc[df_filtrado["Médio - Total"].idxmax(),
                              ["NO_MUNICIPIO", "Fundamental - Total", "Médio - Total"]]
# ordenar por Fundamental (do maior para o menor)
ranking_fund = df_filtrado.sort_values("Fundamental - Total", ascending=False)[
    ["NO_MUNICIPIO", "Fundamental - Total", "Médio - Total"]
]

# ordenar por Médio (do maior para o menor)
ranking_med = df_filtrado.sort_values("Médio - Total", ascending=False)[
    ["NO_MUNICIPIO", "Fundamental - Total", "Médio - Total"]
]


# 1) Forçar numérico (se vier string ou float com .0)
res["MUNICIPIO_CODIGO"]     = pd.to_numeric(res["MUNICIPIO_CODIGO"], errors="coerce")
df_filtrado["CO_MUNICIPIO"] = pd.to_numeric(df_filtrado["CO_MUNICIPIO"], errors="coerce")

# 2) Usar inteiro que aceita nulos
res["MUNICIPIO_CODIGO"]     = res["MUNICIPIO_CODIGO"].astype("Int64")
df_filtrado["CO_MUNICIPIO"] = df_filtrado["CO_MUNICIPIO"].astype("Int64")

# 3) Eliminar linhas sem código
res_ok        = res.dropna(subset=["MUNICIPIO_CODIGO"]).copy()
df_filtrado_ok = df_filtrado.dropna(subset=["CO_MUNICIPIO"]).copy()

# (opcional) criar código IBGE como string de 7 dígitos
res_ok["COD_IBGE"]        = res_ok["MUNICIPIO_CODIGO"].astype("Int64").astype(str).str.zfill(7)
df_filtrado_ok["COD_IBGE"] = df_filtrado_ok["CO_MUNICIPIO"].astype("Int64").astype(str).str.zfill(7)

# mesmas etapas de merge que já fizemos antes...
df_merge = pd.merge(
    res_ok,
    df_filtrado_ok,
    left_on="MUNICIPIO_CODIGO",
    right_on="CO_MUNICIPIO",
    how="inner"
)

# agora seleciona as colunas principais + localização e dependência
cols_saida = [
    "COD_IBGE", 
    "UF_SIGLA", 
    "MUNICIPIO_NOME_ALP", 
    "NO_MUNICIPIO",
    "NO_LOCALIZACAO",          # vem do df_filtrado
    "NO_DEPENDENCIA",          # vem do df_filtrado
    "Fundamental - Total", 
    "Médio - Total",
    "TAXA_APROVACAO_INICIAIS", 
    "TAXA_APROVACAO_FINAIS"
]

# pega só as colunas que realmente existem no df
cols_saida = [c for c in cols_saida if c in df_merge.columns]
resultado = df_merge[cols_saida].copy()

# visualizar os 10 primeiros
resultado.rename(columns={"Fundamental - Total": "Evasão - Fundamental", "Médio - Total": "Evasão -Médio"}, inplace=True)

# Tratamento de outliers
num_cols = [
    "Evasão - Fundamental", 
    "Evasão -Médio", 
    "TAXA_APROVACAO_INICIAIS", 
    "TAXA_APROVACAO_FINAIS"
]

num_cols = [c for c in num_cols if c in resultado.columns]

# 2) coerção robusta para numérico
resultado_num = resultado.copy()
for col in num_cols:
    # troca vírgula por ponto e converte; valores não-numéricos viram NaN
    resultado_num[col] = (
        resultado_num[col]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("\u2212", "-", regex=False)  # sinal de menos unicode, se houver
    )
    resultado_num[col] = pd.to_numeric(resultado_num[col], errors="coerce")

# 3) IQR
Q1 = resultado_num[num_cols].quantile(0.25, numeric_only=True)
Q3 = resultado_num[num_cols].quantile(0.75, numeric_only=True)
IQR = Q3 - Q1
low  = Q1 - 1.5 * IQR
high = Q3 + 1.5 * IQR

mask_out = (resultado_num[num_cols] < low) | (resultado_num[num_cols] > high)
outliers_df = resultado_num[mask_out.any(axis=1)].copy()
sem_outliers_df = resultado_num[~mask_out.any(axis=1)].copy()

# 4) Winsorização (cap) — limita aos limites low/high por coluna
winsor_df = resultado_num.copy()
for col in num_cols:
    winsor_df[col] = winsor_df[col].clip(lower=low[col], upper=high[col])

winsor_df["Reprovacao_Iniciais"] = (1 - winsor_df["TAXA_APROVACAO_INICIAIS"]) * 100
winsor_df["Reprovacao_Finais"]   = (1 - winsor_df["TAXA_APROVACAO_FINAIS"]) * 100

winsor_df["Urgencia"] = (
    winsor_df["Evasão - Fundamental"] +
    winsor_df["Evasão -Médio"] +
    winsor_df["Reprovacao_Iniciais"] +
    winsor_df["Reprovacao_Finais"]
)

urgentes = winsor_df.sort_values("Urgencia", ascending=False).head(20)

# Escolha as colunas que você quer no app:
colunas_essenciais = [
    "MUNICIPIO_NOME_ALP",   # ou "NO_MUNICIPIO"
    "NO_LOCALIZACAO",       # opcional
    "NO_DEPENDENCIA",       # opcional
    "Evasão - Fundamental",
    "Evasão -Médio",
    "TAXA_APROVACAO_INICIAIS",
    "TAXA_APROVACAO_FINAIS",
    "Reprovacao_Iniciais",
    "Reprovacao_Finais",
    "Urgencia"
]

tabela_essencial = urgentes[colunas_essenciais].copy()  # use o seu DF final (ex.: winsor_df / urgentes)

# Garanta que números estão no formato numérico:
def to_num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False), errors="coerce")

for c in ["Evasão - Fundamental", "Evasão - Médio",
          "TAXA_APROVACAO_INICIAIS", "TAXA_APROVACAO_FINAIS",
          "Reprovacao_Iniciais", "Reprovacao_Finais", "Urgencia"]:
    if c in tabela_essencial.columns:
        tabela_essencial[c] = to_num(tabela_essencial[c])


# ============================================================
# Funções utilitárias
# ============================================================
def nrm(x):
    if pd.isna(x):
        return ""
    s = str(x)
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return s.upper().strip()

def chave_municipio(nome: str) -> str:
    n = nrm(nome).replace("–", "-").replace("—", "-")
    if " - " in n:
        n = n.split(" - ")[0]
    for suf in (" MIXING CENTER", " DISTRITO", " DISTRITO INDUSTRIAL"):
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    return n

def ensure_key_urgentes(urgentes: pd.DataFrame) -> pd.DataFrame:
    u = urgentes.copy()
    # prioridade: MUNICIPIO_NOME_ALP → se não tiver, usa NO_MUNICIPIO
    if "MUNICIPIO_NOME_ALP" in u.columns:
        base_nome = u["MUNICIPIO_NOME_ALP"].where(
            u["MUNICIPIO_NOME_ALP"].notna(),
            u.get("NO_MUNICIPIO")
        )
    else:
        base_nome = u.get("NO_MUNICIPIO")
    u["MUNICIPIO_CHAVE"] = base_nome.apply(chave_municipio)
    return u

# ============================================================
# 0) Look-up UF por código IBGE (usa sua DTB)
# ============================================================
dtb = carrega_dtb(ARQ_DTB)  # do seu sprint01.py
dtb_lookup = (
    dtb[["MUNICIPIO_CODIGO", "UF_SIGLA", "MUNICIPIO_NOME"]]
      .rename(columns={"MUNICIPIO_CODIGO": "CO_MUNICIPIO"})
)
dtb_lookup["CO_MUNICIPIO"] = (
    dtb_lookup["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
)

# ============================================================
# 1) média dos OUTROS anos (≠ 2023) por município (anexa UF via DTB)
# ============================================================
def media_outros_anos_nome_uf(df: pd.DataFrame, rotulo_saida: str) -> pd.DataFrame:
    anos = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2022]
    cols = [f"VL_INDICADOR_REND_{a}" for a in anos if f"VL_INDICADOR_REND_{a}" in df.columns]
    if not cols:
        raise KeyError("Nenhuma coluna VL_INDICADOR_REND_XXXX (anos ≠ 2023) encontrada.")

    tmp = df.copy()
    tmp["CO_MUNICIPIO"] = (
        tmp["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )

    # anexa UF e nome oficial via DTB
    tmp = tmp.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")

    # cria chave por NOME para casar com 'urgentes'
    nome_base = tmp["NO_MUNICIPIO"].where(tmp["NO_MUNICIPIO"].notna(), tmp["MUNICIPIO_NOME"])
    tmp["MUNICIPIO_CHAVE"] = nome_base.apply(chave_municipio)

    # calcula média histórica
    tmp_num = tmp[cols].apply(pd.to_numeric, errors="coerce")
    tmp[rotulo_saida] = tmp_num.mean(axis=1, skipna=True)

    out = (tmp.groupby(["UF_SIGLA", "MUNICIPIO_CHAVE"], as_index=False)[rotulo_saida]
             .mean())
    return out

# ============================================================
# 2) calcula históricos (iniciais/finais/médio) e média geral
# ============================================================
ini_hist = media_outros_anos_nome_uf(df_iniciais, "TAXA_APROVACAO_INICIAIS_HIST")
fin_hist = media_outros_anos_nome_uf(df_finais,   "TAXA_APROVACAO_FINAIS_HIST")
med_hist = media_outros_anos_nome_uf(df_em,       "TAXA_APROVACAO_MEDIO_HIST")

hist = (ini_hist.merge(fin_hist, on=["UF_SIGLA","MUNICIPIO_CHAVE"], how="outer")
               .merge(med_hist, on=["UF_SIGLA","MUNICIPIO_CHAVE"], how="outer"))

hist["TAXA_APROVACAO_MEDIA_HIST"] = hist[
    ["TAXA_APROVACAO_INICIAIS_HIST","TAXA_APROVACAO_FINAIS_HIST","TAXA_APROVACAO_MEDIO_HIST"]
].mean(axis=1)

for c in ["TAXA_APROVACAO_INICIAIS_HIST","TAXA_APROVACAO_FINAIS_HIST","TAXA_APROVACAO_MEDIO_HIST","TAXA_APROVACAO_MEDIA_HIST"]:
    if c in hist.columns: 
        hist[c] = hist[c].round(4)

hist["TAXA_APROVACAO_MEDIA_HIST_%"] = (hist["TAXA_APROVACAO_MEDIA_HIST"]*100).round(2)

# ============================================================
# 3) garante chave em 'urgentes' e faz o merge por UF+chave
# ============================================================
urgentes = ensure_key_urgentes(urgentes)
urgentes = urgentes.merge(
    hist[["UF_SIGLA","MUNICIPIO_CHAVE","TAXA_APROVACAO_MEDIA_HIST","TAXA_APROVACAO_MEDIA_HIST_%"]],
    on=["UF_SIGLA","MUNICIPIO_CHAVE"],
    how="left"
)

urgentes.drop(columns=["TAXA_APROVACAO_MEDIA_HIST", "MUNICIPIO_CHAVE"], errors="ignore", inplace=True)
urgentes = urgentes.rename(columns={"TAXA_APROVACAO_MEDIA_HIST_%": "MÉDIA_HISTÓRICA"})


# ===========================
# 0) Lookup: UF e nome oficial via DTB
# ===========================
dtb = carrega_dtb(ARQ_DTB)  # já existe no seu sprint01.py
dtb_lookup = (
    dtb[["MUNICIPIO_CODIGO", "UF_SIGLA", "MUNICIPIO_NOME"]]
      .rename(columns={"MUNICIPIO_CODIGO": "CO_MUNICIPIO"})
      .copy()
)
dtb_lookup["CO_MUNICIPIO"] = (
    dtb_lookup["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
)

# ===========================
# 1) Utilitários
# ===========================
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
    """
    Converte uma planilha (iniciais/finais/médio) para formato longo:
    colunas: CO_MUNICIPIO, ANO, <etapa_rotulo>
    Onde <etapa_rotulo> é a taxa (proporção 0–1) da etapa naquele ano.
    """
    df = df.copy()
    if "CO_MUNICIPIO" not in df.columns:
        raise KeyError("Planilha não possui CO_MUNICIPIO.")
    df["CO_MUNICIPIO"] = (
        df["CO_MUNICIPIO"].astype(str).str.extract(r"(\d{7})", expand=False).str.zfill(7)
    )

    anos = _anos_disponiveis(df, 2005, 2023)
    if not anos:
        raise KeyError("Nenhuma coluna VL_INDICADOR_REND_YYYY encontrada no intervalo 2005–2023.")

    cols = [f"VL_INDICADOR_REND_{a}" for a in anos]
    num = df[["CO_MUNICIPIO"] + cols].copy()
    for c in cols:
        num[c] = pd.to_numeric(num[c], errors="coerce")

    # wide -> long
    long_df = num.melt(id_vars="CO_MUNICIPIO", value_vars=cols,
                       var_name="COL", value_name=etapa_rotulo)
    long_df["ANO"] = long_df["COL"].str.extract(r"(\d{4})").astype(int)
    long_df = long_df.drop(columns=["COL"])

    # média por município-ano (caso existam múltiplas linhas por município)
    long_grp = (long_df
                .groupby(["CO_MUNICIPIO", "ANO"], as_index=False)[etapa_rotulo]
                .mean())
    return long_grp

# ===========================
# 2) Construção das três séries (iniciais/finais/médio)
# ===========================
evo_ini = _long_por_municipio_ano(df_iniciais, "APROVACAO_INICIAIS")
evo_fin = _long_por_municipio_ano(df_finais,   "APROVACAO_FINAIS")
evo_med = _long_por_municipio_ano(df_em,       "APROVACAO_MEDIO")

# ===========================
# 3) Merge por município + ano e média geral
# ===========================
evolucao = (evo_ini
            .merge(evo_fin, on=["CO_MUNICIPIO","ANO"], how="outer")
            .merge(evo_med, on=["CO_MUNICIPIO","ANO"], how="outer"))

# média simples entre as etapas disponíveis (ignora NaN)
evolucao["APROVACAO_MEDIA_GERAL"] = evolucao[
    ["APROVACAO_INICIAIS", "APROVACAO_FINAIS", "APROVACAO_MEDIO"]
].mean(axis=1, skipna=True)

# versões em porcentagem
for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL"]:
    evolucao[c + "_%"] = (evolucao[c] * 100).round(2)

# ===========================
# 4) Anexar UF e nome oficial (para facilitar gráficos/relatórios)
# ===========================
evolucao = evolucao.merge(dtb_lookup, on="CO_MUNICIPIO", how="left")

# ordenar e colunas finais
cols_out = [
    "UF_SIGLA","MUNICIPIO_NOME","CO_MUNICIPIO","ANO",
    "APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL",
    "APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%","APROVACAO_MEDIA_GERAL_%",
]
evolucao = evolucao[cols_out].sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True)

# ============================================================
# 5) Filtrar apenas municípios presentes em URGENTES
# ============================================================

# garante que urgentes tem MUNICIPIO_CHAVE e UF_SIGLA
urgentes = ensure_key_urgentes(urgentes)

# cria chave também na tabela evolucao
evolucao["MUNICIPIO_CHAVE"] = evolucao["MUNICIPIO_NOME"].apply(chave_municipio)

# filtra: mantém só os municípios que estão na tabela urgentes
evolucao_filtrada = evolucao.merge(
    urgentes[["UF_SIGLA","MUNICIPIO_CHAVE"]].drop_duplicates(),
    on=["UF_SIGLA","MUNICIPIO_CHAVE"],
    how="inner"
).sort_values(["UF_SIGLA","MUNICIPIO_NOME","ANO"]).reset_index(drop=True)

# ============================================================
# 6) Preencher valores NaN pela mediana dos outros anos (por município)
# ============================================================

# colunas numéricas que queremos corrigir
cols_num = [
    "APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO","APROVACAO_MEDIA_GERAL",
    "APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","APROVACAO_MEDIO_%","APROVACAO_MEDIA_GERAL_%"
]

# aplica mediana por município (MUNICIPIO_CHAVE)
def preencher_por_mediana(df, grupo="MUNICIPIO_CHAVE", cols=cols_num):
    df = df.copy()
    for col in cols:
        if col not in df.columns:
            continue
        df[col] = df.groupby(grupo)[col].transform(
            lambda x: x.fillna(x.median(skipna=True))
        )
    return df

# aplica na tabela evolucao_filtrada
evolucao_filtrada = preencher_por_mediana(evolucao_filtrada)

# O problema
top10 = (urgentes.groupby("NO_MUNICIPIO")["Urgencia"]
                 .mean()
                 .sort_values(ascending=False)
                 .head(10))

# ============================================================
# Gráficos
# ============================================================
# Criar janelas de 3 anos
evolucao_filtrada["PERIODO"] = pd.cut(
    evolucao_filtrada["ANO"],
    bins=[2004,2007,2011,2015,2019,2023],  # intervalos
    labels=["2005–2007","2009–2011","2013–2015","2017–2019","2021–2023"]
)

# Calcular médias por município e período
grouped = (evolucao_filtrada
           .groupby(["MUNICIPIO_NOME","PERIODO"])
           [["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]]
           .mean()
           .reset_index())

# Exemplo: média geral por período (todos municípios juntos)
media_geral = (grouped.groupby("PERIODO")[["APROVACAO_INICIAIS",
                                           "APROVACAO_FINAIS",
                                           "APROVACAO_MEDIO"]]
               .mean()*100)

ax = media_geral.plot(marker="o", figsize=(9,5))
ax.set_title("Média de aprovação por etapa (2005–2023, em janelas de 2 anos)", fontsize=12, weight="bold")
ax.set_ylabel("%")
ax.set_ylim(0,100)

#Diferença de aprovação em etapas
gap = evolucao_filtrada.groupby("ANO")[["APROVACAO_INICIAIS","APROVACAO_FINAIS","APROVACAO_MEDIO"]].mean()*100
gap["queda_iniciais_finais"] = gap["APROVACAO_INICIAIS"] - gap["APROVACAO_FINAIS"]
gap["queda_finais_medio"] = gap["APROVACAO_FINAIS"] - gap["APROVACAO_MEDIO"]

gap[["queda_iniciais_finais","queda_finais_medio"]].plot(figsize=(10,5), marker="o")
plt.title("Diferença de aprovação entre etapas (2005–2023)")
plt.ylabel("Diferença percentual (p.p.)")
plt.axhline(0, color="black", linestyle="--")


#Diferença de aprovação do ensino médio
import matplotlib.pyplot as plt

# calcula a média de cada etapa por ano
medias = evolucao_filtrada.groupby("ANO")[["APROVACAO_INICIAIS",
                                           "APROVACAO_FINAIS",
                                           "APROVACAO_MEDIO"]].mean()

# diferença em pontos percentuais entre Médio e Iniciais/Finais
diff = pd.DataFrame({
    "Medio - Iniciais": (medias["APROVACAO_MEDIO"] - medias["APROVACAO_INICIAIS"]) * 100,
    "Medio - Finais":   (medias["APROVACAO_MEDIO"] - medias["APROVACAO_FINAIS"]) * 100
}, index=medias.index)

# plota
plt.figure(figsize=(10,6))
for col in diff.columns:
    plt.plot(diff.index, diff[col], marker="o", label=col)

plt.axhline(0, color="black", linestyle="--", alpha=0.7)
plt.title("Diferença da aprovação do Ensino Médio em relação aos outros níveis (2005–2023)")
plt.xlabel("Ano")
plt.ylabel("Diferença em pontos percentuais (p.p.)")
plt.legend()
plt.tight_layout()

#UrgÊntes
uf_col = "NO_MUNICIPIO" if "UF_SIGLA" in urgentes.columns else "UF"
if uf_col in urgentes.columns:
    por_uf = (urgentes.groupby(uf_col, as_index=False)["Urgencia"].mean()
                      .sort_values("Urgencia", ascending=False))
    plt.figure(figsize=(7,4))
    bars = plt.barh(por_uf[uf_col], por_uf["Urgencia"], color="#c1121f")
    plt.gca().invert_yaxis()
    for b in bars:
        plt.text(b.get_width()+0.5, b.get_y()+b.get_height()/2, f"{b.get_width():.1f}",
                 va="center", fontsize=9)
    plt.xlabel("Grau de urgência"); plt.title("Urgência média por UF")
    plt.tight_layout()

#Aprovação finais x iniciais 
import pandas as pd
import numpy as np
import plotly.express as px
import re

# ================================
# Helpers
# ================================
def _slug(s: str) -> str:
    s = str(s).strip().lower()
    trans = str.maketrans({
        "ã":"a","â":"a","á":"a","à":"a",
        "é":"e","ê":"e","è":"e",
        "í":"i",
        "ó":"o","ô":"o","õ":"o",
        "ú":"u",
        "ç":"c"
    })
    s = s.translate(trans)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s

def _minmax(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    if s.max() == s.min():
        return pd.Series(0.5, index=s.index)
    return (s - s.min()) / (s.max() - s.min())

# ================================
# 1) Preparar URGENTES
#    - manter apenas Total/Total para evitar duplicatas
#    - padronizar nomes/colunas
# ================================
urg = urgentes.copy()

# Preferir Total/Total; se não houver, seguimos com o que tiver
if {"NO_LOCALIZACAO","NO_DEPENDENCIA"}.issubset(urg.columns):
    sel = (urg["NO_LOCALIZACAO"].astype(str).str.upper() == "TOTAL") & \
          (urg["NO_DEPENDENCIA"].astype(str).str.upper() == "TOTAL")
    if sel.any():
        urg = urg[sel].copy()

# renomear colunas-chave
col_nome_urg = "NO_MUNICIPIO" if "NO_MUNICIPIO" in urg.columns else "MUNICIPIO_NOME_ALP"
urg = urg.rename(columns={
    col_nome_urg: "MUNICIPIO_NOME",
    "Evasão - Fundamental": "EVASAO_FUNDAMENTAL",
    "Evasão - Médio": "EVASAO_MEDIO"
})

# keep só o necessário e reduzir por município (caso ainda tenha múltiplas linhas)
keep_urg = ["MUNICIPIO_NOME"]
for c in ["EVASAO_FUNDAMENTAL","EVASAO_MEDIO"]:
    if c in urg.columns: keep_urg.append(c)
urg = urg[keep_urg].copy()
urg["MUNICIPIO_NOME"] = urg["MUNICIPIO_NOME"].astype(str).str.strip()
urg = urg.groupby("MUNICIPIO_NOME", as_index=False).mean(numeric_only=True)

# ================================
# 2) Preparar DF_FILTRADO (aprovações por ano)
#    - converter 0–1 para % (0–100)
# ================================
evo = evolucao_filtrada.copy()
evo["MUNICIPIO_NOME"] = evo["MUNICIPIO_NOME"].astype(str).str.strip()

for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS"]:
    if c in evo.columns:
        # se já estiver em % (0-100), não muda; caso esteja 0–1, multiplicar por 100
        # heurística: se média <= 1.5, consideramos proporção
        mean_val = pd.to_numeric(evo[c], errors="coerce").mean()
        if pd.notna(mean_val) and mean_val <= 1.5:
            evo[c + "_%"] = 100 * pd.to_numeric(evo[c], errors="coerce")
        else:
            evo[c + "_%"] = pd.to_numeric(evo[c], errors="coerce")
    else:
        evo[c + "_%"] = np.nan

# ================================
# 3) Agregados "estáticos" por município
#    - use a MÉDIA do período (robusto) ou o ÚLTIMO ano (snapshot)
#    -> abaixo: média; comente a média e descomente o "last" se preferir
# ================================
# média do período
df_static = (
    evo.groupby(["MUNICIPIO_NOME"], as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
       .mean(numeric_only=True)
)

# --- alternativa: último ano disponível por município ---
# idx = evo.groupby("MUNICIPIO_NOME")["ANO"].idxmax()
# df_static = evo.loc[idx, ["MUNICIPIO_NOME","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].reset_index(drop=True)

# merge evasão
df_static = df_static.merge(urg, on="MUNICIPIO_NOME", how="left")

# métricas derivadas
df_static["GAP_APROV_%"] = df_static["APROVACAO_INICIAIS_%"] - df_static["APROVACAO_FINAIS_%"]

aprov_finais_norm = 1 - _minmax(df_static["APROVACAO_FINAIS_%"].fillna(df_static["APROVACAO_FINAIS_%"].median()))
evasao_norm       = _minmax(df_static["EVASAO_FUNDAMENTAL"].fillna(df_static["EVASAO_FUNDAMENTAL"].median()))
gap_norm          = _minmax(df_static["GAP_APROV_%"].fillna(0))

df_static["SCORE_RISCO"] = 0.5*aprov_finais_norm + 0.4*evasao_norm + 0.1*gap_norm

# ================================
# 4) Gráficos
# ================================
def graf_tendencia_geral(evo=evo):
    t = evo.dropna(subset=["ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]).copy()
    m = t.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean()
    melted = m.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
    fig = px.line(melted, x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                  title="Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)")
    fig.update_layout(yaxis_tickformat=".1f")
    return fig

def graf_gap_aprov(base=df_static, top_n=25):
    t = base.dropna(subset=["GAP_APROV_%"]).copy()
    t = t.sort_values("GAP_APROV_%", ascending=False).head(top_n)
    fig = px.bar(t, x="GAP_APROV_%", y="MUNICIPIO_NOME", orientation="h",
                 title=f"Gargalo (GAP) de Aprovação — Iniciais − Finais (Top {top_n})",
                 labels={"MUNICIPIO_NOME":"Município","GAP_APROV_%":"GAP (p.p.)"})
    fig.update_yaxes(categoryorder="total ascending")
    return fig

def graf_quadrantes(base=df_static):
    t = base.dropna(subset=["APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]).copy()
    cut_aprov = t["APROVACAO_FINAIS_%"].median()
    cut_evas  = t["EVASAO_FUNDAMENTAL"].median()

    conds = [
        (t["APROVACAO_FINAIS_%"] < cut_aprov) & (t["EVASAO_FUNDAMENTAL"] > cut_evas),
        (t["APROVACAO_FINAIS_%"] >= cut_aprov) & (t["EVASAO_FUNDAMENTAL"] > cut_evas),
        (t["APROVACAO_FINAIS_%"] < cut_aprov) & (t["EVASAO_FUNDAMENTAL"] <= cut_evas),
        (t["APROVACAO_FINAIS_%"] >= cut_aprov) & (t["EVASAO_FUNDAMENTAL"] <= cut_evas),
    ]
    labels = ["Crítico (aprov baixa, evas alta)","Atenção (aprov alta, evas alta)",
              "Apoio pedagógico (aprov baixa, evas baixa)","OK (aprov alta, evas baixa)"]
    t["Quadrante"] = np.select(conds, labels)

    fig = px.scatter(
        t, x="APROVACAO_FINAIS_%", y="EVASAO_FUNDAMENTAL", color="Quadrante",
        hover_data=["MUNICIPIO_NOME","APROVACAO_INICIAIS_%","GAP_APROV_%","SCORE_RISCO"],
        title="Quadrantes — Aprovação (Anos Finais) × Evasão (Fundamental)",
        labels={"APROVACAO_FINAIS_%":"Aprovação Finais (%)","EVASAO_FUNDAMENTAL":"Evasão Fundamental (%)"}
    )
    fig.add_vline(x=cut_aprov); fig.add_hline(y=cut_evas)
    return fig

def graf_ranking_risco(base=df_static, top_n=20):
    t = base.dropna(subset=["SCORE_RISCO"]).copy()
    t = t.sort_values("SCORE_RISCO", ascending=False).head(top_n)
    fig = px.bar(
        t, x="SCORE_RISCO", y="MUNICIPIO_NOME", orientation="h",
        hover_data=["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL","GAP_APROV_%"],
        title=f"Top {top_n} — Ranking de Risco (baixa aprov finais + alta evasão + gap)",
        labels={"MUNICIPIO_NOME":"Município","SCORE_RISCO":"Score de Risco (0–1)"}
    )
    fig.update_yaxes(categoryorder="total ascending")
    return fig

def graf_tendencia_municipio(municipio_nome, evo=evo):
    t = evo[evo["MUNICIPIO_NOME"].astype(str).str.strip() == str(municipio_nome).strip()].copy()
    t = t.dropna(subset=["ANO","APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"])
    if t.empty:
        raise ValueError(f"Município '{municipio_nome}' não encontrado em df_filtrado.")
    m = t.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean()
    melted = m.melt(id_vars="ANO", var_name="Etapa", value_name="Aprovação (%)")
    fig = px.line(melted, x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                  title=f"{municipio_nome} — Evolução de Aprovação (Iniciais vs Finais)")
    fig.update_layout(yaxis_tickformat=".1f")
    return fig

#Quadrantes
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

def graf_quadrantes(base=df_static, usar_tamanho_por_risco=True):
    t = base.dropna(subset=["APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]).copy()
    if t.empty:
        raise ValueError("Sem dados para plotar quadrantes.")

    # Cortes (medianas)
    cut_x = t["APROVACAO_FINAIS_%"].median()
    cut_y = t["EVASAO_FUNDAMENTAL"].median()

    # Rótulo de quadrante
    conds = [
        (t["APROVACAO_FINAIS_%"] < cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] > cut_y),
        (t["APROVACAO_FINAIS_%"] < cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
        (t["APROVACAO_FINAIS_%"] >= cut_x) & (t["EVASAO_FUNDAMENTAL"] <= cut_y),
    ]
    labels = ["Crítico (aprov baixa, evas alta)",
              "Atenção (aprov alta, evas alta)",
              "Apoio pedagógico (aprov baixa, evasão baixa)",
              "OK (aprov alta, evasão baixa)"]
    t["Quadrante"] = np.select(conds, labels)

    # Abrevia nome para rótulo curto no ponto
    t["LABEL"] = t["MUNICIPIO_NOME"].str.title().str.slice(0, 18)

    # Range com folga
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

    # Pontos maiores, rótulos acima
    fig.update_traces(marker=dict(line=dict(width=1, color="white"), opacity=0.9))
    fig.update_traces(textposition="top center", textfont=dict(size=11))

    # Sombras dos quadrantes
    fig.add_shape(type="rect", x0=xr[0], x1=cut_x, y0=cut_y, y1=yr[1], fillcolor="red", opacity=0.06, line_width=0)
    fig.add_shape(type="rect", x0=cut_x, x1=xr[1], y0=cut_y, y1=yr[1], fillcolor="orange", opacity=0.06, line_width=0)
    fig.add_shape(type="rect", x0=xr[0], x1=cut_x, y0=yr[0], y1=cut_y, fillcolor="gray", opacity=0.06, line_width=0)
    fig.add_shape(type="rect", x0=cut_x, x1=xr[1], y0=yr[0], y1=cut_y, fillcolor="green", opacity=0.06, line_width=0)

    # Linhas de corte mais visíveis
    fig.add_vline(x=cut_x, line_width=3, line_dash="dash")
    fig.add_hline(y=cut_y, line_width=3, line_dash="dash")

    # Anotações dos quadrantes
    fig.add_annotation(x=xr[0]+(cut_x-xr[0])*0.08, y=yr[1]-(yr[1]-cut_y)*0.60,
                       text="Crítico", showarrow=False, font=dict(size=12, color="red"))
    fig.add_annotation(x=cut_x+(xr[1]-cut_x)*0.85, y=yr[1]-(yr[1]-cut_y)*0.15,
                       text="Atenção", showarrow=False, font=dict(size=12, color="orange"))
    fig.add_annotation(x=xr[0]+(cut_x-xr[0])*0.15, y=cut_y-(cut_y-yr[0])*0.15,
                       text="Apoio pedagógico", showarrow=False, font=dict(size=12, color="gray"))
    fig.add_annotation(x=cut_x+(xr[1]-cut_x)*0.85, y=cut_y-(cut_y-yr[0])*0.15,
                       text="OK", showarrow=False, font=dict(size=12, color="green"))


    # Layout
    fig.update_layout(
        xaxis=dict(range=xr, tickformat=".1f"),
        yaxis=dict(range=yr, tickformat=".1f"),
        legend_title_text="Quadrante",
        margin=dict(l=10, r=10, t=60, b=10),
    )
    return fig


# ===========================================
# PAINEL STREAMLIT • IMAGENS (sem quadrantes)
# ===========================================
import streamlit as st
import pandas as pd
import numpy as np
import re, unicodedata
import plotly.express as px
import matplotlib.pyplot as plt

st.set_page_config(page_title="IA • Aprovação, Evasão e Urgência", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel (hotfix)")

# ----------------- Helpers -----------------
def _slug(s: object) -> str:
    if pd.isna(s): return ""
    t = unicodedata.normalize("NFKD", str(s)).encode("ASCII","ignore").decode("ASCII")
    t = t.replace("–","-").replace("—","-").strip().lower()
    t = re.sub(r"[^a-z0-9]+","_", t)
    return re.sub(r"_+","_", t).strip("_")

def _to_num(x: pd.Series) -> pd.Series:
    return pd.to_numeric(
        x.astype(str)
         .str.replace("%","",regex=False)
         .str.replace(",","",regex=False)  # aceita "74,3" ou "7,430"
         .str.replace(" ","",regex=False),
        errors="coerce"
    )

def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.dropna().empty or s.max() == s.min():
        return pd.Series(0.5, index=s.index)
    return (s - s.min())/(s.max()-s.min())

# ----------------- Evasão (de urgentes) -----------------
def _prepare_urgentes(urg: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(urg, pd.DataFrame) or urg.empty:
        return pd.DataFrame(columns=["MUNICIPIO_NOME","EVASAO_FUNDAMENTAL","CHAVE"])
    u = urg.copy()

    # nome
    nome_col = next((c for c in ["MUNICIPIO_NOME","MUNICIPIO_NOME_ALP","NO_MUNICIPIO"] if c in u.columns), None)
    if not nome_col:
        return pd.DataFrame(columns=["MUNICIPIO_NOME","EVASAO_FUNDAMENTAL","CHAVE"])
    u = u.rename(columns={nome_col:"MUNICIPIO_NOME"})
    u["MUNICIPIO_NOME"] = u["MUNICIPIO_NOME"].astype(str).str.strip()

    # possíveis colunas de evasão
    evas_map = ["EVASAO_FUNDAMENTAL","Evasão - Fundamental","Fundamental - Total"]
    col_evas = next((c for c in evas_map if c in u.columns), None)
    if col_evas is None:
        # varre por nomes parecidos
        for c in u.columns:
            if "evas" in _slug(c) and ("fund" in _slug(c) or "fundamental" in _slug(c)):
                col_evas = c
                break

    u["EVASAO_FUNDAMENTAL"] = _to_num(u[col_evas]) if col_evas else np.nan
    o = (u.groupby("MUNICIPIO_NOME", as_index=False)["EVASAO_FUNDAMENTAL"]
           .mean(numeric_only=True))
    o["CHAVE"] = o["MUNICIPIO_NOME"].map(_slug)
    return o

# ----------------- Static seguro -----------------
@st.cache_data(show_spinner=False)
def _build_static(evo: pd.DataFrame, urg: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(evo, pd.DataFrame) or evo.empty:
        return pd.DataFrame()

    t = evo.copy()
    if "MUNICIPIO_NOME" not in t.columns:
        # tenta variantes
        for c in ["NO_MUNICIPIO","MUNICIPIO_NOME_ALP"]:
            if c in t.columns:
                t = t.rename(columns={c:"MUNICIPIO_NOME"})
                break
    t["MUNICIPIO_NOME"] = t["MUNICIPIO_NOME"].astype(str).str.strip()

    # garantir % (0–100)
    for base in ["APROVACAO_INICIAIS","APROVACAO_FINAIS"]:
        if base + "_%" not in t.columns:
            if base in t.columns:
                m = pd.to_numeric(t[base], errors="coerce").mean()
                t[base + "_%"] = (100*pd.to_numeric(t[base], errors="coerce")
                                  if pd.notna(m) and m <= 1.5
                                  else pd.to_numeric(t[base], errors="coerce"))
            else:
                t[base + "_%"] = np.nan
        else:
            t[base + "_%"] = _to_num(t[base + "_%"])

    # média por município
    base_static = (t.groupby("MUNICIPIO_NOME", as_index=False)
                     [["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]]
                     .mean(numeric_only=True))

    base_static["CHAVE"] = base_static["MUNICIPIO_NOME"].map(_slug)

    # anexar evasão
    urg2 = _prepare_urgentes(urg)
    df = base_static.merge(urg2[["CHAVE","EVASAO_FUNDAMENTAL"]], on="CHAVE", how="left").drop(columns=["CHAVE"])

    # coerção final -> numérico
    for c in ["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]:
        if c in df.columns: df[c] = _to_num(df[c])

    # derivados
    df["GAP_APROV_%"] = df["APROVACAO_INICIAIS_%"] - df["APROVACAO_FINAIS_%"]
    df["SCORE_RISCO"] = 0.5*(1 - _minmax(df["APROVACAO_FINAIS_%"])) + \
                        0.4* _minmax(df["EVASAO_FUNDAMENTAL"]) + \
                        0.1* _minmax(df["GAP_APROV_%"].fillna(0))

    return df

# ----------------- Dados vindos do seu script -----------------
evo_safe = globals().get("evolucao_filtrada", pd.DataFrame())
urg_safe = globals().get("urgentes", pd.DataFrame())
df_static_ready = _build_static(evo_safe, urg_safe)

# ----------------- KPIs -----------------
c1,c2,c3,c4 = st.columns(4)
with c1:
    st.metric("Municípios no recorte", len(df_static_ready["MUNICIPIO_NOME"].unique()) if not df_static_ready.empty else "–")
with c2:
    st.metric("Aprovação — Finais (média)",
              f"{df_static_ready['APROVACAO_FINAIS_%'].mean():.1f}%"
              if not df_static_ready.empty else "–")
with c3:
    st.metric("Evasão — Fundamental (média)",
              f"{df_static_ready['EVASAO_FUNDAMENTAL'].mean():.1f}%"
              if ("EVASAO_FUNDAMENTAL" in df_static_ready.columns and not df_static_ready.empty) else "–")
with c4:
    st.metric("Score de risco (média)",
              f"{df_static_ready['SCORE_RISCO'].mean():.2f}"
              if ("SCORE_RISCO" in df_static_ready.columns and not df_static_ready.empty) else "–")
st.divider()

# ----------------- Abas -----------------
tab_overview, tab_grafs, tab_tables, tab_diag = st.tabs(["Visão geral","Gráficos","Tabelas","Diagnóstico"])

# ---- Visão Geral ----
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

# ---- Gráficos ----
with tab_grafs:
    st.subheader("📊 Gráficos disponíveis")

    # Tendência geral (se a função já existir, usa; senão gera um fallback rápido)
    if "graf_tendencia_geral" in globals() and callable(graf_tendencia_geral) and not evo_safe.empty:
        st.plotly_chart(graf_tendencia_geral(evo=evo_safe), use_container_width=True)
    elif not evo_safe.empty and {"ANO","APROVACAO_INICIAIS","APROVACAO_FINAIS"}.issubset(evo_safe.columns):
        tmp = evo_safe.copy()
        for c in ["APROVACAO_INICIAIS","APROVACAO_FINAIS"]:
            m = pd.to_numeric(tmp[c], errors="coerce").mean()
            tmp[c + "_%"] = (100*pd.to_numeric(tmp[c], errors="coerce") if pd.notna(m) and m<=1.5
                              else pd.to_numeric(tmp[c], errors="coerce"))
        m = tmp.groupby("ANO", as_index=False)[["APROVACAO_INICIAIS_%","APROVACAO_FINAIS_%"]].mean(numeric_only=True)
        st.plotly_chart(px.line(m.melt("ANO", var_name="Etapa", value_name="Aprovação (%)"),
                                x="ANO", y="Aprovação (%)", color="Etapa", markers=True,
                                title="Tendência Geral — Aprovação Iniciais vs Finais (média do recorte)"),
                        use_container_width=True)

    # >>> Renderiza também TODOS os gráficos matplotlib já criados fora do app
    figs = [plt.figure(n) for n in plt.get_fignums()]
    if figs:
        st.subheader("🖼️ Gráficos (imagem) gerados no código")
        for f in figs:
            st.pyplot(f, use_container_width=True)

# ---- Tabelas ----
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
        st.markdown("**urgentes (cru)**"); st.dataframe(urg_safe, use_container_width=True)

# ---- Diagnóstico ----
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









