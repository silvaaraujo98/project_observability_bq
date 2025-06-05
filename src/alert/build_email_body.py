


def criar_corpo_email_alerta_dinamico(grouped_exeuctiontime_queries_threshold_df):

    
    
    nome_projeto = grouped_exeuctiontime_queries_threshold_df['ProjectId'][0]
    hora_ocorrencia = grouped_exeuctiontime_queries_threshold_df['Clusterized_Date'][0]
    tempo_excedido_realizado = grouped_exeuctiontime_queries_threshold_df['execution_time_min'][0]
    queries_realizadas= grouped_exeuctiontime_queries_threshold_df['Queries'][0]
    queries_threshold = grouped_exeuctiontime_queries_threshold_df['threshold_queries'][0]
    tempo_excedido_threshold = grouped_exeuctiontime_queries_threshold_df['threshold_executiontime'][0]

    # Determina quais tipos de alerta ocorreram
    alerta_tempo = grouped_exeuctiontime_queries_threshold_df['execution_time_send_email_flag'][0]
    alerta_queries = grouped_exeuctiontime_queries_threshold_df['queries_send_email_flag'][0]

    # Texto introdutório e lista de detalhes
    introducao_alerta = ""
    detalhes_alerta = ""

    if alerta_tempo and alerta_queries:
        introducao_alerta = f"Informamos que o projeto **{nome_projeto}** excedeu os thresholds de **tempo de execução** e **quantidade de queries**."
        detalhes_alerta += f"          <li><strong>Tempo de Execução Excedido:</strong> {tempo_excedido_realizado - tempo_excedido_threshold} segundos a mais que o limite de {tempo_excedido_threshold}.</li>\n"
        detalhes_alerta += f"          <li><strong>Quantidade de Queries Excedidas:</strong> {queries_realizadas - queries_threshold} queries a mais que o limite de {queries_threshold}.</li>\n"
    elif alerta_tempo:
        introducao_alerta = f"Informamos que o projeto **{nome_projeto}** excedeu o threshold de **tempo de execução**."
        detalhes_alerta += f"          <li><strong>Tempo de Execução Excedido:</strong> {tempo_excedido_realizado - tempo_excedido_threshold} segundos a mais que o limite de {tempo_excedido_threshold}.</li>\n"
    elif alerta_queries:
        introducao_alerta = f"Informamos que o projeto **{nome_projeto}** excedeu o threshold de **quantidade de queries**."
        detalhes_alerta += f"          <li><strong>Quantidade de Queries Excedidas:</strong> {queries_realizadas - queries_threshold} queries a mais que o limite de {queries_threshold}</li>\n"
    else:
        # Caso não haja alerta, o que não deveria acontecer se a função for chamada apenas em caso de alerta
        introducao_alerta = "Um alerta foi disparado para o projeto, mas os detalhes não foram especificados."


    corpo_email_html = f"""\
    <html>
      <body>
        <p>Prezados,</p>
        <p>{introducao_alerta}</p>
        <p>Detalhes do alerta:</p>
        <ul>
          <li><strong>Projeto:</strong> {nome_projeto}</li>
          <li><strong>Hora da Ocorrência:</strong> {hora_ocorrencia}</li>
{detalhes_alerta}
        </ul>
        <p>Por favor, verifiquem a execução do projeto para identificar a causa do aumento.</p>
        <p>Atenciosamente,<br>
           Equipe de Monitoramento
        </p>
      </body>
    </html>
    """
    return corpo_email_html