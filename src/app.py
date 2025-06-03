import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go # Para gráficos mais customizados, se precisar
from transformation import *
from queries_perfomed import *
from slots_consumed import *
from average_execution_time import *


# --- 1. Funções de Exemplo para Geração de Dados e Gráficos ---
# (Você substituirá estas por suas funções reais que carregam/processam dados e geram gráficos)

# --- 2. Estrutura da Página Streamlit ---

st.set_page_config(layout="wide", page_title="Dashboard de Observabilidade BigQuery")

st.title("📊 Dashboard de Observabilidade BigQuery")

st.markdown("""
Este dashboard apresenta métricas de observabilidade para os projetos BigQuery.
Você pode visualizar o tempo médio de execução e o consumo de slots ao longo do tempo.
""")

# --- 3. Carregamento e Processamento dos Dados ---
# Use st.cache_data para evitar recarregar e reprocessar dados a cada interação.
@st.cache_data
def get_data_and_process():
    # Carrega os dados brutos
    df_raw = run_all_transformation_functions()

    df_queries_performed_specific_columns = get_specific_columns(df_raw,'ProjectId','Clusterized_Date','Queries')
    df_queries_performed_grouped  = group_and_aggregate_data(df_queries_performed_specific_columns,'Queries','ProjectId','Clusterized_Date')
    
    
    df_slot_consumed_specific_columns = get_specific_columns(df_raw,'ProjectId','TotalSlotMin')
    df_slot_consumed_grouped  = group_and_aggregate_data(df_slot_consumed_specific_columns,'TotalSlotMin','ProjectId')

    df_execution_time_specific_columns = get_specific_columns(df_raw,'ProjectId','Clusterized_Date','execution_time_min')
    df_execution_time_grouped = group_and_aggregate_data(df_execution_time_specific_columns,'execution_time_min','Clusterized_Date','ProjectId',aggregation_method='mean')

    return df_queries_performed_grouped,df_slot_consumed_grouped,df_execution_time_grouped

# Carrega e processa os dados uma única vez
df_queries_performed_grouped,df_slot_consumed_grouped,df_execution_time_grouped = get_data_and_process()

col1,col2= st.columns([2,1])
# --- 4. Exibição dos Gráficos ---
with col1:
    st.header("📈 Consultas Realizadas")
    # Renderiza o gráfico usando a função Plotly e st.plotly_chart
    st.plotly_chart(plot_queries_perfomed(df_queries_performed_grouped), use_container_width=True)
with col2:
    st.header("📈 Slot Consumido")
    # Renderiza o gráfico usando a função Plotly e st.plotly_chart
    st.plotly_chart(plot_slots_consumed(df_slot_consumed_grouped), use_container_width=True)

st.header("📈 Tempo Médio de Execução")
# Renderiza o gráfico usando a função Plotly e st.plotly_chart
st.plotly_chart(plot_average_execution_time(df_execution_time_grouped), use_container_width=True)

st.markdown("---")
st.caption("Desenvolvido para observabilidade BigQuery.")