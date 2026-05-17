import streamlit as st
import pandas as pd
import streamlit.components.v1 as components
import os

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Data Pipeline - Localiza",
    page_icon="⬆️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. FUNÇÕES DE I/O EM CACHE (OTIMIZAÇÃO DE PERFORMANCE) ---
#@st.cache_data(show_spinner=False)
def carregar_dados_parquet(caminho_pasta):
    """
    Lê arquivos Parquet de um diretório otimizando a leitura via Pandas.
    O cache evita múltiplas leituras em disco durante a navegação (I/O optimization).
    """
    try:
        # Lê todos os arquivos Parquet dentro do diretório particionado
        df = pd.read_parquet(caminho_pasta)
        return df
    except FileNotFoundError:
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao carregar {caminho_pasta}: {e}")
        return None

@st.cache_data(show_spinner=False)
def carregar_relatorio_gx(caminho_html):
    """
    Carrega o arquivo HTML gerado pelo Great Expectations em memória.
    """
    try:
        with open(caminho_html, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return None

# --- 3. CABEÇALHO DO PAINEL ---
st.title("🚀 Resultados do Pipeline de Dados")
st.markdown("""
Este painel consome as tabelas geradas na etapa final do pipeline de orquestração. 
Os dados estão estruturados sob a **Arquitetura Medalhão**, garantindo isolamento entre higienização (Silver) e agregação de negócio (Gold).
""")

st.divider()

# --- 4. ROTEAMENTO DE CAMINHOS ---

PATH_GOLD_TABELA1 = "data/output/gold/tabela1_risk_score/"
PATH_GOLD_TABELA2 = "data/output/gold/tabela2_top3_sales/"
PATH_SILVER = "data/output/silver/"
PATH_GX_DOCS = "gx/uncommitted/data_quality_report.html" 

# --- 5. ESTRUTURA DE ABAS ---
print("Carregando dados para visualização... Isso pode levar alguns segundos dependendo do volume de dados e do cache." )
aba1, aba2, aba3 = st.tabs(["📊 Camada Gold (Negócio)", "🔎 Camada Silver (Tratada)", "🛡️ Data Quality (Raw)"])
df_tabela1 = carregar_dados_parquet(PATH_GOLD_TABELA1)
if df_tabela1 is None:
    st.warning("Tabela 1 da camada Gold não encontrada. O pipeline do Airflow já foi concluído?")
    print("Tabela 1 da camada Gold não encontrada. O pipeline do Airflow já foi concluído?")
else:
    with aba1:
        st.header("Camada Gold - Agregações Analíticas")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Tabela 1: Média de Risco por Região")
            st.markdown("Média de `risk_score` agrupada por `location_region`, ordenada decrescentemente.")
            df_tabela1 = carregar_dados_parquet(PATH_GOLD_TABELA1)
            
            if df_tabela1 is not None and not df_tabela1.empty:
                st.dataframe(df_tabela1, use_container_width=True, hide_index=True)
            else:
                st.warning("Tabela 1 não encontrada. O pipeline do Airflow já foi concluído?")
                print("Tabela 1 não encontrada. O pipeline do Airflow já foi concluído?")

        with col2:
            st.subheader("Tabela 2: Top 3 Vendas Recentes")
            st.markdown("Últimas 3 transações de 'sale' particionadas por data/hora para cada `receiving_address`.")
            df_tabela2 = carregar_dados_parquet(PATH_GOLD_TABELA2)
            
            if df_tabela2 is not None and not df_tabela2.empty:
                st.dataframe(df_tabela2, use_container_width=True, hide_index=True)
            else:
                st.warning("Tabela 2 não encontrada. O pipeline do Airflow já foi concluído?")

    with aba2:
        st.header("Camada Silver - Dados Higienizados")
        st.markdown("""
        Pré-visualização dos dados higienizados e tipados que passaram pelo contrato de qualidade. 
        *Exibindo as primeiras 100 linhas em cache para otimização de renderização.*
        """)
        
        df_silver = carregar_dados_parquet(PATH_SILVER)
        
        if df_silver is not None and not df_silver.empty:
            # Pega apenas os top 100 registros para não estourar a memória visual do navegador
            st.dataframe(df_silver.head(100), use_container_width=True, hide_index=True)
            st.caption(f"Volume total da tabela Silver disponível no Data Lake: {len(df_silver)} registros.")
        else:
            st.warning("Dados da camada Silver não encontrados. O pipeline do Airflow já foi concluído?")

    with aba3:
        st.header("Relatório Great Expectations - Camada Raw")
        st.markdown("Validação metadata-driven executada imediatamente após a ingestão (Data Quality).")
        
        html_content = carregar_relatorio_gx(PATH_GX_DOCS)
        
        if html_content:
            components.html(html_content, height=800, scrolling=True)
        else:
            st.info("""
            **Relatório não encontrado.** Isso ocorre porque o Airflow ainda não disparou a task de Data Quality ou o caminho do Data Docs no repositório foi alterado.
            """)