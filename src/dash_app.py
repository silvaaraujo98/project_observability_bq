import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go # Para gráficos mais customizados, se precisar
from etl_transformation import *
from viz_queries_perfomed import *
from viz_slots_consumed import *
from viz_average_execution_time import *
from dash_timefilter import display_filter
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
        options = df_raw['ProjectId'].unique(),
        default = list(df_raw['ProjectId'].unique()))
    if projects_selected:
        df_filtered = df_raw[df_raw['ProjectId'].isin(projects_selected)]
    else:
        df_filtered = df_raw
    return df_filtered

def apply_date_filter(initial_date,final_date,df_raw):
    aware_initial_date = initial_date.replace(tzinfo=timezone.utc)
    aware_final_date = final_date.replace(tzinfo=timezone.utc)
    df_filtered = df_raw[(df_raw['Clusterized_Date'] >= aware_initial_date) & (df_raw['Clusterized_Date'] <= aware_final_date)]
    return df_filtered

@st.cache_data
def process_data(df_raw):
    # Carrega os dados brutos

    df_queries_performed_specific_columns = get_specific_columns(df_raw,'ProjectId','Clusterized_Date','Queries')
    df_queries_performed_grouped  = group_and_aggregate_data(df_queries_performed_specific_columns,'Queries','ProjectId','Clusterized_Date')
    
    
    df_slot_consumed_specific_columns = get_specific_columns(df_raw,'ProjectId','Clusterized_Date','TotalSlotMin')
    df_slot_consumed_grouped  = group_and_aggregate_data(df_slot_consumed_specific_columns,'TotalSlotMin','Clusterized_Date','ProjectId')

    df_execution_time_specific_columns = get_specific_columns(df_raw,'ProjectId','Clusterized_Date','execution_time_min')
    df_execution_time_grouped = group_and_aggregate_data(df_execution_time_specific_columns,'execution_time_min','Clusterized_Date','ProjectId',aggregation_method='mean')

    queries_perfomed_last_24hours = float(get_value_columns_in_hours(df_queries_performed_grouped,'Clusterized_Date','Queries'))
    queries_perfomed_last_48hours = float(get_value_columns_in_hours(df_queries_performed_grouped,'Clusterized_Date','Queries',hours=48))
    queries_perfomed_btw_24_48 = queries_perfomed_last_48hours - queries_perfomed_last_24hours

    execution_time_last_24hours = float(get_value_columns_in_hours(df_execution_time_grouped,'Clusterized_Date','execution_time_min'))
    execution_time_last_48hours = float(get_value_columns_in_hours(df_execution_time_grouped,'Clusterized_Date','execution_time_min',hours=48))
    execution_time_btw_24_48 = execution_time_last_48hours - execution_time_last_24hours

    slot_consumed_last_24hours = float(get_value_columns_in_hours(df_slot_consumed_grouped,'Clusterized_Date','TotalSlotMin'))
    slot_consumed_last_48hours = float(get_value_columns_in_hours(df_slot_consumed_grouped,'Clusterized_Date','TotalSlotMin',hours=48))
    slot_consumed_btw_24_48 = slot_consumed_last_48hours - slot_consumed_last_24hours

    return df_queries_performed_grouped,\
        df_slot_consumed_grouped,\
        df_execution_time_grouped,\
        queries_perfomed_last_24hours,\
        queries_perfomed_btw_24_48,\
        execution_time_last_24hours,\
        execution_time_btw_24_48,\
        slot_consumed_last_24hours,\
        slot_consumed_btw_24_48

df_raw = get_data()
initial_date,final_date = display_filter(df_raw['Clusterized_Date'].max())
df_filtered_date = apply_date_filter(initial_date,final_date,df_raw)

df_filtered_project = apply_project_filter(df_filtered_date)




# Carrega e processa os dados uma única vez
df_queries_performed_grouped,\
        df_slot_consumed_grouped,\
        df_execution_time_grouped,\
        queries_perfomed_last_24hours,\
        queries_perfomed_btw_24_48,\
        execution_time_last_24hours,\
        execution_time_btw_24_48,\
        slot_consumed_last_24hours,\
        slot_consumed_btw_24_48= process_data(df_filtered_project)
col1, col2, col3 = st.columns(3)
delta_queries_perfomed = safely_division((queries_perfomed_last_24hours-queries_perfomed_btw_24_48)*100,queries_perfomed_btw_24_48)
delta_execution_time = safely_division((execution_time_last_24hours-execution_time_btw_24_48)*100,execution_time_btw_24_48)
delta_slot_consumed = safely_division((slot_consumed_last_24hours-slot_consumed_btw_24_48)*100,slot_consumed_btw_24_48)

with col1:
    st.metric("Queries executadas nas ultimas 24 horas",format_br_number(queries_perfomed_last_24hours),delta = format_br_number(delta_queries_perfomed) + " %")

with col2:
    st.metric("Soma de Tempo médio de consultas nas ultimas 24 horas",format_br_number(execution_time_last_24hours),delta = format_br_number(delta_execution_time) + " %")
with col3:
    st.metric("Slots Consumidos nas últimas 24 horas",format_br_number(slot_consumed_last_24hours),delta = format_br_number(delta_slot_consumed) + " %")
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