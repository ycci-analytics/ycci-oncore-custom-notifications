# The purpose of this script is to query for visits that will occur in the next 5 days and email out a report table by coordinator email. 

import oracledb
import pandas as pd
import os
import smtplib
import html
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
from pretty_html_table import build_table
from datetime import datetime
import io

load_dotenv()

# Get and set database credentials
un = os.environ.get('ONCORE_USER')
cs = os.environ.get('ONCORE_SERVERNAME')
pw = os.environ.get('ONCORE_PASSWORD')

# Set variables that can be updated for each notification script
sql_query = "select * from oncore_report_ro.ycci_visit_tracking where unacknowledged_visit_outside_policy = 'yes'"
to_email = 'COORDINATOR_EMAIL'
alert_name = "Unacknowledged Visits Report"
email_body = '''\
        <html>
        <head></head>
        <body>
            <p><br>
            Hello,<br><br>
            The included report provides an overview of visits that have not been marked as occurred, N/A or missed and are outside the 2 day grace period based on the visit date..<br>
            Please use the links to the study visit records in OnCore to review and mark as occurred, missed, or N/A or update the visit date if applicable.<br>
            </p>
            <p>
            {TABLE}
            </p>
        </body>
        </html>
        '''

# connect to oracle application database and get a dataframe from relevant query
with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
    with connection.cursor() as cursor:
       cursor.execute(sql_query)
       col_names = [c.name for c in cursor.description]
       data = cursor.fetchall()
       df = pd.DataFrame(data, columns=col_names)
    
# Get today's date in YYYY-MM-DD format
today_date = datetime.today().strftime('%Y-%m-%d')

# Define the directory and file name to store logs file
directory = 'logs'
file_name = f'data_{today_date}.csv'

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Define the full path to save the file
file_path = os.path.join(directory, file_name)

# Write the DataFrame to a CSV file
df.to_csv(file_path, index=False)
      
# Convert URLs to clickable links for HTML
df['CRA_CONSOLE_VISIT_URL_HTML'] = df['CRA_CONSOLE_VISIT_URL'].apply(lambda x: f'<a href="{x}">link</a>')


#iterate trough rows and send email
grp = df.groupby(to_email) #group data by email
sent_mail = []
for email, group in grp:
    if email in sent_mail:
        pass
    else:
        # for prod , add split string on ; for 
        toaddr = ['nicholas.vankuren@yale.edu']
        bcc = ['nicholas.vankuren@yale.edu']
        fromname = "OnCore Notifications"
        fromaddr = "no-reply@oncore_yale.edu"
        sent_mail.append(email)
        msg = MIMEMultipart("alternative")
        msg["From"] = "{} <{}>".format(fromname, fromaddr)
        msg["To"] = ', ' .join(toaddr)
        msg["Bcc"] = ', ' .join(bcc)
        msg["Subject"] = "OnCore Alert: {}".format(alert_name)
        email_table = group[["PROTOCOL_NO", "SEQUENCE_NUMBER", "SEGMENT_NAME", "VISIT_NAME", "CRA_CONSOLE_VISIT_URL"]].sort_values(by=['PROTOCOL_NO', 'SEQUENCE_NUMBER'])
        
        if len(email_table) > 20:
            # Save DataFrame as Excel in memory
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                email_table.to_excel(writer, index=False, sheet_name='Sheet1')
                workbook = writer.book
                worksheet = writer.sheets['Sheet1']
                for idx, url in enumerate(email_table['CRA_CONSOLE_VISIT_URL'], start=1):
                    worksheet.write_url(f'E{idx+2}', url, string='link')
            excel_buffer.seek(0)

            # Attach Excel file
            attachment = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            attachment.set_payload(excel_buffer.read())
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', 'attachment; filename="upcoming_visits_report.xlsx"')
            msg.attach(attachment)
            html = email_body.replace('{TABLE}', '')  # Remove {TABLE} placeholder
        else:
            html_table = group[["PROTOCOL_NO", "SEQUENCE_NUMBER", "SEGMENT_NAME", "VISIT_NAME", "CRA_CONSOLE_VISIT_URL_HTML"]].sort_values(by=['PROTOCOL_NO', 'SEQUENCE_NUMBER']).to_html(index=False, render_links=True, escape=False)

                        # Add CSS styling for left-justified column headers
            html_table = f"""
            <style>
                th {{
                    text-align: left;
                }}
            </style>
            {html_table}
            """

            # Create the body of the message (a plain-text and an HTML version).
            text = "Placeholder text for now"
            html = email_body.format(TABLE = html_table)

        part1 = MIMEText(html, 'html')

        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part1)

        #Send the message via local SMTP server.
        s = smtplib.SMTP(host="localhost") 
        #s.ehlo()
        # sendmail function takes 3 arguments: sender's address, recipient's address
        # and message to send - here it is sent as one string.
        s.sendmail(fromaddr, (toaddr+bcc), msg.as_string())
        s.quit()