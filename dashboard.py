import streamlit as st
import pandas as pd
import streamlit.components.v1 as components
import os

# Configuração da página para ocupar a tela inteira
st.set_page_config(page_title="Resultados - Desafio Localiza", layout="wide")
st.title("🚀 Pipeline de Dados - Resultados")

# Criação das abas solicitadas
tab_qualidade, tab_saida = st.tabs(["📊 Qualidade de Dados (GX)", "📈 Saída do Teste"])

# Aba 1: Renderização do Great Expectations
with tab_qualidade:
    html_path = "gx/uncommitted/data_quality_report.html"
    
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        # O components.html injeta o código do GX de forma isolada e segura
        components.html(html_content, height=800, scrolling=True)
    else:
        st.warning("O relatório de qualidade ainda não foi gerado. Execute a DAG no Airflow primeiro.")

# Aba 2: Leitura dos arquivos Parquet gerados pelo PySpark
with tab_saida:
    st.markdown("### Tabela 1: Média de Risk Score por Região")
    try:
        df_tab1 = pd.read_parquet("data/output/tabela1_risk_score")
        st.dataframe(df_tab1, use_container_width=True, hide_index=True)
    except Exception:
        st.info("Tabela 1 não encontrada. Aguardando a execução do pipeline.")

    st.markdown("---")
    
    st.markdown("### Tabela 2: Top 3 Recebimentos Recentes (Sales)")
    try:
        df_tab2 = pd.read_parquet("data/output/tabela2_top3_sales")
        st.dataframe(df_tab2, use_container_width=True, hide_index=True)
    except Exception:
        st.info("Tabela 2 não encontrada. Aguardando a execução do pipeline.")