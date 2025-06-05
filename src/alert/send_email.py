import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from build_email_body import criar_corpo_email_alerta_dinamico
from create_credential import get_credential
import pandas as pd

grouped_exeuctiontime_queries_threshold_df = pd.read_csv("./data/grouped_exeuctiontime_queries_threshold_df(3).csv")
def enviar_email_gmail(smtp_server,port,remetente_email, remetente_senha_app, destinatario_email, assunto, corpo_email_html):


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
   
    SMTP_EMAIL,PORT_EMAIL,MY_EMAIL, EMAIL_PASSWORD, RECIPE_EMAIL, SUBJECT_EMAIL= get_credential()
    corpo_do_email_html = criar_corpo_email_alerta_dinamico(grouped_exeuctiontime_queries_threshold_df)

    enviar_email_gmail(SMTP_EMAIL,PORT_EMAIL,MY_EMAIL, EMAIL_PASSWORD, RECIPE_EMAIL, SUBJECT_EMAIL, corpo_do_email_html)