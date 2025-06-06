import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from alert_build_email_body import criar_corpo_email_alerta_dinamico
from alert_create_credential import get_credential
import pandas as pd


def build_email_credentials(df):
    
    SMTP_EMAIL,PORT_EMAIL,MY_EMAIL, EMAIL_PASSWORD, RECIPE_EMAIL, SUBJECT_EMAIL= get_credential()
    body_email_html = criar_corpo_email_alerta_dinamico(df)
    
    return SMTP_EMAIL,PORT_EMAIL,MY_EMAIL, EMAIL_PASSWORD, RECIPE_EMAIL, SUBJECT_EMAIL,body_email_html

def send_email(df):
    
    SMTP_EMAIL,PORT_EMAIL,MY_EMAIL, EMAIL_PASSWORD, RECIPE_EMAIL, SUBJECT_EMAIL,body_email_html = build_email_credentials(df)

    # Cria um contexto SSL seguro
    context = ssl.create_default_context()

    try:
        # Cria a conexão segura com o servidor SMTP
        with smtplib.SMTP_SSL(SMTP_EMAIL, PORT_EMAIL, context=context) as server:
            # Faça login com suas credenciais do Gmail (email e senha de app)
            server.login(MY_EMAIL, EMAIL_PASSWORD)

            # Cria a mensagem
            msg = MIMEMultipart("alternative")
            msg["From"] = MY_EMAIL
            msg["To"] = RECIPE_EMAIL
            msg["Subject"] = SUBJECT_EMAIL

            # Anexa as partes do corpo do e-mail (texto e HTML)
            part1 = MIMEText(body_email_html, "html") # Se você quiser um email em texto puro, use "plain"
            msg.attach(part1)

            # Envia o e-mail
            server.sendmail(MY_EMAIL, RECIPE_EMAIL, msg.as_string())
            print("E-mail enviado com sucesso!")

    except smtplib.SMTPAuthenticationError:
        print("Erro de autenticação: Verifique seu e-mail e sua Senha de App.")
        print("Certifique-se de que a Verificação em Duas Etapas esteja ativada e você gerou uma Senha de App.")
    except Exception as e:
        print(f"Ocorreu um erro ao enviar o e-mail: {e}")

def iterating_dataframe_to_write_email(df):
    
    #Receives a df filtered by projects that i have to send a email and call the function to send a email for row.
    
    for i in range(df.shape[0]):
        df_one_row = df.iloc[i]
        send_email(df_one_row)


