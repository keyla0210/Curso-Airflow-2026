import smtplib
from email.message import EmailMessage
import ssl

def send_completion_email() -> None:
    """Envía notificación cuando el archivo de episodios está disponible."""

    EMAIL_SUBJECT = "Procesamiento de episodios finalizado"
    EMAIL_BODY = """
    El procesamiento del flujo de episodios ha finalizado exitosamente.

    """
    email_sender = "juandavid2931@gmail.com"
    email_password = "mjvc nyga klvo yimw"
    email_receiver = "jdravila@bancolombia.com.co"

    message = EmailMessage()
    message["From"] = email_sender
    message["To"] = email_receiver
    message["Subject"] = EMAIL_SUBJECT
    message.set_content(EMAIL_BODY)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.send_message(message)