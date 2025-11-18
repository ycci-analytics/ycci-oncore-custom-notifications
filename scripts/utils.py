import oracledb
import pandas as pd
import os
import psycopg
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
from datetime import datetime
import io
import csv
import dns.resolver


# Determine the path to the .env file
if getattr(sys, 'frozen', False):
    # If the application is run as a bundle, the .env file is in the same directory as the executable
    bundle_dir = sys._MEIPASS
else:
    # If the application is run in a normal Python environment, the .env file is in the current directory
    bundle_dir = os.path.dirname(os.path.abspath(__file__))

env_path = os.path.join(bundle_dir, '.env')
load_dotenv(env_path)


def get_db_credentials(db_type='oracle'):
    if db_type == 'oracle':
        return {
            'user': os.getenv('ORACLE_USER'),
            'password': os.getenv('ORACLE_PASSWORD'),
            'dsn': os.getenv('ORACLE_DSN')
        }
    elif db_type == 'postgres':
        return {
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT')
        }
    else:
        raise ValueError("Unsupported database type")

def query_database(sql_query, db_type='oracle'):
    creds = get_db_credentials(db_type)

    if db_type == 'oracle':
        with oracledb.connect(user=creds['user'], password=creds['password'], dsn=creds['dsn']) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                col_names = [c.name for c in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=col_names)

    elif db_type == 'postgres':
        conn_str = (
            f"dbname={creds['dbname']} user={creds['user']} "
            f"password={creds['password']} host={creds['host']} port={creds['port']}"
        )
        with psycopg.connect(conn_str) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                col_names = [desc.name for desc in cursor.description]
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


def log_email_status(recipient, status):
    # Create logs directory if it doesn't exist
    log_directory = 'logs/email_logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)



    # Create a new log file with datetime appended to the end of it
    log_filename = f'email_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    log_filepath = os.path.join(log_directory, log_filename)

    with open(log_filepath, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([recipient, datetime.now(), status])



def validate_email(email):
    # Get domain from email
    domain = email.split('@')[1]

    dev_email = [email.strip() for email in os.environ.get('DEV_EMAIL', '').split(',') if email.strip()]

    # Check if domain has valid MX records
    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
    except dns.resolver.NXDOMAIN:
        return False, "Domain does not exist"
    except dns.resolver.NoAnswer:
        return False, "No MX records found"

    # SMTP verification
    try:
        mx_record = str(mx_records[0].exchange)
        server = smtplib.SMTP(mx_record)
        server.set_debuglevel(0)
        server.helo()
        server.mail(dev_email)
        code, message = server.rcpt(email)
        server.quit()
        if code == 250:
            return True, "Email is valid and active"
        else:
            return False, "Email is undeliverable"
    except Exception as e:
        return False, f"SMTP verification failed: {str(e)}"



def send_email(to_email, fromname, fromaddr, subject, body, filename=None, attachment=None):
    fromname = fromname
    fromaddr = fromaddr
    environment = os.environ.get('ENVIRONMENT')
    mail_server = os.environ.get('MAIL_SERVER')

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
    


    with smtplib.SMTP(host=mail_server) as s:
        s.sendmail(fromaddr, toaddr + bcc, msg.as_string())
