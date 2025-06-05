import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd

grouped_exeuctiontime_queries_threshold_df = pd.read_csv("./data/grouped_exeuctiontime_queries_threshold_df(3).csv")

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


def enviar_email_gmail(remetente_email, remetente_senha_app, destinatario_email, assunto, corpo_email_html):
    # Configurações do servidor SMTP do Gmail
    smtp_server = "smtp.gmail.com"
    port = 465  # Para SSL

    # Cria um contexto SSL seguro
    context = ssl.create_default_context()

    try:
        # Cria a conexão segura com o servidor SMTP
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            # Faça login com suas credenciais do Gmail (email e senha de app)
            server.login(remetente_email, remetente_senha_app)

            # Cria a mensagem
            msg = MIMEMultipart("alternative")
            msg["From"] = remetente_email
            msg["To"] = destinatario_email
            msg["Subject"] = assunto

            # Anexa as partes do corpo do e-mail (texto e HTML)
            part1 = MIMEText(corpo_email_html, "html") # Se você quiser um email em texto puro, use "plain"
            msg.attach(part1)

            # Envia o e-mail
            server.sendmail(remetente_email, destinatario_email, msg.as_string())
            print("E-mail enviado com sucesso!")

    except smtplib.SMTPAuthenticationError:
        print("Erro de autenticação: Verifique seu e-mail e sua Senha de App.")
        print("Certifique-se de que a Verificação em Duas Etapas esteja ativada e você gerou uma Senha de App.")
    except Exception as e:
        print(f"Ocorreu um erro ao enviar o e-mail: {e}")

if __name__ == "__main__":
    # Suas credenciais e informações do e-mail
    meu_email = "joao.araujo@ipnet.cloud"  # Substitua pelo seu e-mail do Gmail
    minha_senha_app = "fbte wdqg tiij atqj"  # Substitua pela Senha de App gerada

    email_destinatario = "breno.dutra@ipnet.cloud" # Substitua pelo e-mail do destinatário
    assunto_email = "Assunto do Meu E-mail Teste"

    # Você pode usar HTML para formatar o corpo do e-mail
    corpo_do_email_html = criar_corpo_email_alerta_dinamico(grouped_exeuctiontime_queries_threshold_df)


    enviar_email_gmail(meu_email, minha_senha_app, email_destinatario, assunto_email, corpo_do_email_html)