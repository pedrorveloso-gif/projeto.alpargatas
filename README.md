
# 📊 Instituto Alpargatas — Mapeamento da Urgência Educacional

Este repositório reúne a análise e desenvolvimento de um **projeto em grupo** da disciplina **Análise de dados** cujo objetivo foi **mapear a urgência educacional dos municípios apoiados pelo Instituto Alpargatas**, utilizando dados oficiais do **INEP/IDEB** (taxas de **aprovação** e **evasão**), e avaliar como o Instituto está atuando em cada localidade, além de propor caminhos para melhoria.

---

## 🎯 Objetivo do Projeto

* Identificar os municípios mais urgentes em termos de **indicadores educacionais**.
* Acompanhar a evolução do **IDEB**, **aprovação** e **evasão** entre os anos analisados.
* Cruzar esses dados com a **atuação do Instituto Alpargatas** (quantidade de escolas, projetos e beneficiados).
* Sugerir melhorias e políticas de permanência para fortalecer o impacto local.

---

## 🛠️ Como o Projeto Foi Feito

1. **Mineração e Tratamento dos Dados**

   * Utilizamos dados públicos do **INEP/IDEB**.
   * Criamos indicadores de **grau de urgência**, combinando taxas de aprovação e evasão escolar.
   * Analisamos os municípios em que o Instituto atua, destacando gargalos e oportunidades de melhoria.

2. **Exploração e Análise**

   * Todo o processo de **mineração, limpeza e análise dos dados** foi realizado em **Python**, dentro de um **Jupyter Notebook**, presente neste repositório.

3. **Visualização e Painel Interativo**

   * Foi desenvolvido um **painel em Streamlit**, onde é possível navegar pelos municípios e acompanhar:

     * Evolução das taxas educacionais ao longo do tempo.
     * Comparativo entre municípios.
     * Destaques para os casos mais urgentes.

🔗 O painel interativo pode ser acessado aqui:
👉 [Acessar o Painel no Streamlit](https://projetoalpargatas-pmtq27mvyfiiaticrvswcu.streamlit.app/)

---

## 📂 Estrutura do Repositório

* **`Análise_alpa.ipynb`** → Notebook Jupyter com toda a mineração, análise e modelagem dos dados.
* **`final_alpa.py`** → Código final otimizado para o **Streamlit**, responsável pelo painel interativo.
* **`/dados`** → Diretório contendo os dados educacionais utilizados na análise.

---

## 🚀 Tecnologias Utilizadas

* **Python** (pandas, numpy, plotly, streamlit, etc.)
* **Jupyter Notebook** para exploração de dados
* **Streamlit** para criação do painel interativo
* **GitHub** para versionamento e colaboração em grupo

---

## 👥 Autores

* Pedro — [GitHub](https://github.com/pedrorveloso-gif)
* João Filipe — [GitHub](https://github.com/Jotafdc)


