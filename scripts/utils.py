import oracledb
import pandas as pd
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
from datetime import datetime
import io

# Determine the path to the .env file
if getattr(sys, 'frozen', False):
    # If the application is run as a bundle, the .env file is in the same directory as the executable
    bundle_dir = sys._MEIPASS
else:
    # If the application is run in a normal Python environment, the .env file is in the current directory
    bundle_dir = os.path.dirname(os.path.abspath(__file__))

env_path = os.path.join(bundle_dir, '.env')
load_dotenv(env_path)

def get_db_credentials():
    return {
        'user': os.environ.get('ONCORE_USER'),
        'password': os.environ.get('ONCORE_PASSWORD'),
        'dsn': os.environ.get('ONCORE_SERVERNAME')
    }

def query_database(sql_query):
    creds = get_db_credentials()
    with oracledb.connect(user=creds['user'], password=creds['password'], dsn=creds['dsn']) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)
            col_names = [c.name for c in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=col_names)
        
def save_to_csv(df, directory='logs'):
    today_date = datetime.today().strftime('%Y-%m-%d')
    file_name = f'data_{today_date}.csv'
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_path = os.path.join(directory, file_name)
    df.to_csv(file_path, index=False)
    return file_path

def send_email(to_email, subject, body, filename=None, attachment=None):
    fromname = "OnCore Notifications"
    fromaddr = "no-reply@oncore_yale.edu"
    environment = os.environ.get('ENVIRONMENT')

    print(environment)

    if environment == 'dev':
        toaddr = [email.strip() for email in os.environ.get('DEV_EMAIL', '').split(',') if email.strip()]
        print(toaddr)
    elif environment == 'prod':
        toaddr = [email.strip() for email in to_email.split(';') if email.strip()]
    else:
        raise Exception("No environment has been specified")

    bcc = [os.environ.get('BCC_EMAIL')]

    msg = MIMEMultipart("alternative")
    msg["From"] = f"{fromname} <{fromaddr}>"
    msg["To"] = ', '.join(toaddr)
    msg["Bcc"] = ', '.join(bcc)
    msg["Subject"] = subject
    part1 = MIMEText(body, 'html')
    msg.attach(part1)

    if attachment:
        attachment.set_payload(attachment.read())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(attachment)

    with smtplib.SMTP(host="localhost") as s:
        s.sendmail(fromaddr, toaddr + bcc, msg.as_string())
