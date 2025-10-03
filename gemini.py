# app.py

import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import numpy as np

# Importa as funções de utilidade e cache
from utils import (
    ARQ_DTB, ARQ_ALP, ARQ_INICIAIS, ARQ_FINAIS, ARQ_EM, ARQ_EVASAO,
    carrega_dtb_cache, carrega_alpargatas_cache, carrega_dados_inep,
    processa_aprovacao, processa_evasao_e_ranking, build_static_data
)

st.set_page_config(page_title="IA • Aprovação, Evasão e Urgência", page_icon="📊", layout="wide")
st.title("📊 Instituto Alpargatas — Painel de Urgência Educacional")
st.markdown("---")

# ============================================================
# 1. CARREGAMENTO E PROCESSAMENTO (CACHEADO)
# As funções abaixo são executadas apenas uma vez, ou quando o arquivo muda.
# ============================================================

try:
    # A) Carregamento
    dtb = carrega_dtb_cache(ARQ_DTB)
    alpa = carrega_alpargatas_cache(ARQ_ALP)
    df_iniciais, df_finais, df_em, df_evasao = carrega_dados_inep(
        ARQ_INICIAIS, ARQ_FINAIS, ARQ_EM, ARQ_EVASAO
    )
    
    # B) Cruzamento inicial (parte da lógica do seu original)
    codificados = alpa.merge(dtb, on=["MUNICIPIO_CHAVE","UF_SIGLA"], how="left", suffixes=("_ALP","_IBGE"))
    
    # C) Processamento da Aprovação (taxas 2023)
    df_aprov = processa_aprovacao(df_iniciais, df_finais, df_em, codificados)

    # D) Processamento da Evasão, Urgência e Ranking
    df_urgentes = processa_evasao_e_ranking(df_aprov, df_evasao)

    # E) Tabela Estática Final (Score de Risco)
    df_final = build_static_data(df_urgentes, codificados)


except Exception as e:
    st.error(f"Erro Crítico no Carregamento/Processamento de Dados. Verifique os caminhos no `utils.py` e se os arquivos estão na pasta `dados/` do GitHub. Erro: {e}")
    st.stop()


# ============================================================
# 2. STREAMLIT UI E VISUALIZAÇÃO
# ============================================================

# --- KPIs ---
c1,c2,c3,c4 = st.columns(4)
with c1:
    st.metric("Municípios Prioritários", len(df_final["MUNICIPIO_NOME"].unique()))
with c2:
    st.metric("Aprovação Finais (média)", f"{df_final['APROVACAO_FINAIS_%'].mean():.1f}%")
with c3:
    st.metric("Evasão Fundamental (média)", f"{df_final['EVASAO_FUNDAMENTAL'].mean():.1f}%")
with c4:
    st.metric("Score de Risco (média)", f"{df_final['SCORE_RISCO'].mean():.2f}")
st.markdown("---")


# --- Abas ---
tab_ranking, tab_tabelas = st.tabs(["Ranking de Risco e Quadrantes","Tabelas e Diagnóstico"])

with tab_ranking:
    st.subheader("⚠️ Top 20: Ranking de Urgência Educacional")
    st.markdown("""
    O **Score de Risco** prioriza municípios com a **menor Aprovação nos Anos Finais** e a **maior Taxa de Evasão**.
    """)
    
    col_rank, col_quad = st.columns([1, 2])

    # Gráfico de Barras - Ranking de Risco
    with col_rank:
        st.markdown("##### Score de Risco (Top 20)")
        t = df_final.sort_values("SCORE_RISCO", ascending=False).head(20)
        fig_rank = px.bar(
            t, x="SCORE_RISCO", y="MUNICIPIO_NOME", orientation="h",
            hover_data=["APROVACAO_FINAIS_%", "EVASAO_FUNDAMENTAL", "GAP_APROV_%"],
            color="SCORE_RISCO", color_continuous_scale=px.colors.sequential.Reds,
            labels={"MUNICIPIO_NOME":"Município","SCORE_RISCO":"Score de Risco (0–1)"}
        )
        fig_rank.update_yaxes(categoryorder="total ascending")
        st.plotly_chart(fig_rank, use_container_width=True)

    # Gráfico de Quadrantes
    with col_quad:
        st.markdown("##### Quadrantes (Aprovação Finais x Evasão Fundamental)")
        
        # Lógica simplificada de Quadrantes (para não depender da função não-importada)
        t = df_final.dropna(subset=["APROVACAO_FINAIS_%","EVASAO_FUNDAMENTAL"]).copy()
        cut_aprov = t["APROVACAO_FINAIS_%"].median()
        cut_evas = t["EVASAO_FUNDAMENTAL"].median()

        conds = [
            (t["APROVACAO_FINAIS_%"] < cut_aprov) & (t["EVASAO_FUNDAMENTAL"] > cut_evas),
            (t["APROVACAO_FINAIS_%"] >= cut_aprov) & (t["EVASAO_FUNDAMENTAL"] > cut_evas),
            (t["APROVACAO_FINAIS_%"] < cut_aprov) & (t["EVASAO_FUNDAMENTAL"] <= cut_evas),
            (t["APROVACAO_FINAIS_%"] >= cut_aprov) & (t["EVASAO_FUNDAMENTAL"] <= cut_evas),
        ]
        labels = ["Crítico (Aprov baixa, Evas alta)", "Atenção (Aprov alta, Evas alta)",
                  "Apoio Pedagógico (Aprov baixa, Evas baixa)", "OK (Aprov alta, Evas baixa)"]
        t["Quadrante"] = np.select(conds, labels)
        
        fig_quad = px.scatter(
            t, x="APROVACAO_FINAIS_%", y="EVASAO_FUNDAMENTAL", color="Quadrante",
            size="SCORE_RISCO", size_max=20,
            hover_data=["MUNICIPIO_NOME","APROVACAO_INICIAIS_%","GAP_APROV_%"],
            title="Aprovação (Anos Finais) × Evasão (Fundamental)",
            labels={"APROVACAO_FINAIS_%":"Aprovação Finais (%)","EVASAO_FUNDAMENTAL":"Evasão Fundamental (%)"}
        )
        fig_quad.add_vline(x=cut_aprov, line_dash="dash"); fig_quad.add_hline(y=cut_evas, line_dash="dash")
        st.plotly_chart(fig_quad, use_container_width=True)


with tab_tabelas:
    st.subheader("Bases Consolidadas")
    
    st.markdown("##### 1. Ranking Final (`df_final`)")
    st.dataframe(df_final, use_container_width=True)
    
    st.markdown("##### 2. Base de Aprovação (`df_aprov`) — 2023")
    st.dataframe(df_aprov.head(20), use_container_width=True) # head para não sobrecarregar
    
    st.markdown("##### 3. Tabela de Urgência Pura (após Winsorização) (`df_urgentes`)")
    st.dataframe(df_urgentes, use_container_width=True)
