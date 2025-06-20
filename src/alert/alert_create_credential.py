from dotenv import load_dotenv
import os

load_dotenv()

def get_credential():
    # Suas credenciais e informações do e-mail
    MY_EMAIL = os.getenv('EMAIL_LOGIN') # Substitua pelo seu e-mail do Gmail
    EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')  # Substitua pela Senha de App gerada

    RECIPE_EMAIL = os.getenv('EMAIL_RECIPIENT') # Substitua pelo e-mail do destinatário
    SUBJECT_EMAIL = "Aviso de Projeto acima de threshold"

    SMTP_EMAIL = os.getenv('SMTP_EMAIL')
    PORT_EMAIL = os.getenv('PORT_EMAIL')


    return SMTP_EMAIL,PORT_EMAIL,MY_EMAIL, EMAIL_PASSWORD, RECIPE_EMAIL, SUBJECT_EMAIL


    