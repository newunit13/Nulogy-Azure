import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from utils.config import SMTP_USERNAME, SMTP_PASSWORD

class Email():
    def __init__(self, username: str=SMTP_USERNAME, password: str=SMTP_PASSWORD, host: str='smtp.office365.com', port: int=587):
        self.username = username
        self.password = password
        self.connection = smtplib.SMTP(host, port)
        self.mimemsg = MIMEMultipart()
        self.mimemsg['From'] = username
        self.mail_attachments = []

    def sendMessage(self, to: str, subject: str, body: str, cc: str=''):
        self.mimemsg['To'] = to
        self.mimemsg['Cc'] = cc
        self.mimemsg['Subject'] = subject
        self.mimemsg.attach(MIMEText(body, 'HTML'))

        if self.mail_attachments:
            for path, name in self.mail_attachments:
                with open(path, 'rb') as attachment:
                    mimefile = MIMEBase('application', 'octet-stream')
                    mimefile.set_payload((attachment).read())
                    encoders.encode_base64(mimefile)
                    mimefile.add_header('Content-Disposition', f'attachment; filename={name}')
                    self.mimemsg.attach(mimefile)

        self.connection.starttls()
        self.connection.login(self.username, self.password)
        self.connection.send_message(self.mimemsg)
        self.connection.quit()

    def addAttachment(self, attachment_path: str):
        attachment_path = os.path.abspath(attachment_path)
        self.mail_attachments.append((attachment_path, os.path.basename(attachment_path)))


if __name__ == '__main__':
    email = Email()
    email.addAttachment(r'C:\Users\erussell\Repositories\Nulogy-Export\output\item_cost_update.csv')
    email.addAttachment(r'C:\Users\erussell\Repositories\Nulogy-Export\output\inventory_snapshot.csv')
    email.sendMessage('erussell@ciservicesnow.com', 'This is just a test', 'Hello world!')
