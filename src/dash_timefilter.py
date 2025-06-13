import streamlit as st
from datetime import datetime,timedelta,time,date
def display_filter(max_date):
    ## Receives nothing, display filter in a page and e returns the user´s option
    # Opções para o seletor principal
    opcoes_tempo_relativo = {
    "Hoje": "today",
    "Ontem": "yesterday",
    "Últimos 15 minutos": "15m",
    "Últimos 30 minutos": "30m",
    "Horários de início e término": "custom_range"}
    # Traduzindo as chaves para a exibição no selectbox
    opcoes_exibicao = list(opcoes_tempo_relativo.keys())

    # O seletor principal
    opcao_selecionada = st.selectbox(
        "Tempo relativo (15m, 1h, 1d, 1w)",
        options=opcoes_exibicao,
        index=0 # Define 'Hoje' como a opção inicial
    )
    if opcao_selecionada == 'Hoje':
        final_date = max_date
        initial_date = datetime(max_date.year,max_date.month,max_date.day)
    elif opcao_selecionada == 'Ontem':
        final_date = datetime(max_date.year,max_date.month,max_date.day-1)
        initial_date = datetime(max_date.year,max_date.month,max_date.day-1,23,59,59)
    elif opcao_selecionada == 'Últimos 15 minutos':
        final_date = max_date
        initial_date = max_date - timedelta(minutes=15)
    elif opcao_selecionada == 'Últimos 30 minutos':
        final_date = max_date
        initial_date = max_date - timedelta(minutes=30)
    


    elif opcao_selecionada == "Horários de início e término":
        st.write(f"Você selecionou: **{opcao_selecionada}**")
        st.subheader("Definir Horário de Início e Término")
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input("Data de Início", value=date.today())
            hora_inicio = st.time_input("Hora de Início", value=time(23, 59))
        with col2:
            data_fim = st.date_input("Data de Término", value=date.today())
            hora_fim = st.time_input("Hora de Término", value=time(0, 0))
        final_date = datetime.combine(data_fim, hora_fim)
        initial_date = datetime.combine(data_inicio, hora_inicio)
    st.write(opcao_selecionada,initial_date,final_date)

    st.markdown("---")
    return initial_date,final_date

display_filter(datetime.now())
