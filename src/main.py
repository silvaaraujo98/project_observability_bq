import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go # Para gráficos mais customizados, se precisar
from etl.etl_transformation import *
from viz.viz_queries_perfomed import *
from viz.viz_slots_consumed import *
from viz.viz_average_execution_time import *
from dash.dash_timefilter import display_filter
from datetime import timezone


# --- 1. Funções de Exemplo para Geração de Dados e Gráficos ---
# (Você substituirá estas por suas funções reais que carregam/processam dados e geram gráficos)

# --- 2. Estrutura da Página Streamlit ---

st.set_page_config(layout="wide", page_title="Dashboard de Observabilidade BigQuery")

st.title("📊 Dashboard de Observabilidade BigQuery")

st.markdown("""
Este dashboard apresenta métricas de observabilidade para os projetos BigQuery.
Você pode visualizar o tempo médio de execução e o consumo de slots nas últimas 24 horas.
""")

# --- 3. Carregamento e Processamento dos Dados ---
# Use st.cache_data para evitar recarregar e reprocessar dados a cada interação.
@st.cache_data
def get_data():
    df_raw = run_all_transformation_functions()
    return df_raw

def apply_project_filter(df_raw):
    projects_selected = st.multiselect(
        'Selecione os Projetos',
        options = df_raw['project_id'].unique(),
        default = list(df_raw['project_id'].unique()))
    if projects_selected:
        df_filtered = df_raw[df_raw['project_id'].isin(projects_selected)]
    else:
        df_filtered = df_raw
    return df_filtered

def apply_date_filter(initial_date,final_date,df_raw):
    aware_initial_date = initial_date.replace(tzinfo=timezone.utc)
    aware_final_date = final_date.replace(tzinfo=timezone.utc)
    df_filtered = df_raw[(df_raw['clusterized_date'] >= aware_initial_date) & (df_raw['clusterized_date'] <= aware_final_date)]
    return df_filtered

def create_measures(df):

    queries_perfomed = float(df['queries_perfomed'].sum())
    execution_time_min = float(df['execution_time_min'].sum())
    total_slot = float(df['total_slot'].sum())

    queries_perfomed_24h_ago = float(df['queries_perfomed_24h_ago'].sum())
    execution_time_min_24h_ago = float(df['execution_time_min_24h_ago'].sum())
    total_slot_24h_ago = float(df['total_slot_24h_ago'].sum())
    
    return queries_perfomed,execution_time_min,total_slot,queries_perfomed_24h_ago,execution_time_min_24h_ago,total_slot_24h_ago



df_raw = get_data()
initial_date,final_date = display_filter(df_raw['clusterized_date'].max())
df_filtered_date = apply_date_filter(initial_date,final_date,df_raw)
df_filtered_project = apply_project_filter(df_filtered_date)

queries_perfomed,\
execution_time_min,\
total_slot,\
queries_perfomed_24h_ago,\
execution_time_min_24h_ago,\
total_slot_24h_ago = create_measures(df_filtered_project)


col1, col2, col3 = st.columns(3)
delta_queries_perfomed = safely_division((queries_perfomed-queries_perfomed_24h_ago)*100,queries_perfomed_24h_ago)
delta_execution_time = safely_division((execution_time_min-execution_time_min_24h_ago)*100,execution_time_min_24h_ago)
delta_slot_consumed = safely_division((total_slot-total_slot_24h_ago)*100,total_slot_24h_ago)

with col1:
    st.metric("Queries executadas",format_br_number(queries_perfomed),delta = format_br_number(delta_queries_perfomed) + " %")

with col2:
    st.metric("Soma de Tempo médio de consultas",format_br_number(execution_time_min),delta = format_br_number(delta_execution_time) + " %")
with col3:
    st.metric("Slots Consumidos",format_br_number(total_slot),delta = format_br_number(delta_slot_consumed) + " %")
col1,col2= st.columns([2,1])
# --- 4. Exibição dos Gráficos ---
with col1:
    st.header("📈 Consultas Realizadas")
    # Renderiza o gráfico usando a função Plotly e st.plotly_chart
    st.plotly_chart(plot_queries_perfomed(df_filtered_project), use_container_width=True)
with col2:
    st.header("📈 Slot Consumido")
    # Renderiza o gráfico usando a função Plotly e st.plotly_chart
    st.plotly_chart(plot_slots_consumed(df_filtered_project), use_container_width=True)

st.header("📈 Tempo Médio de Execução")
# Renderiza o gráfico usando a função Plotly e st.plotly_chart
st.plotly_chart(plot_average_execution_time(df_filtered_project), use_container_width=True)

st.markdown("---")
st.caption("Desenvolvido para observabilidade BigQuery. João, the Real Badass")