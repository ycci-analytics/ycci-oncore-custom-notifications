import oracledb
import pandas
import os
import smtplib
import html
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from pretty_html_table import build_table

load_dotenv()

# Get and set database credentials
un = os.environ.get('ONCORE_USER')
cs = os.environ.get('ONCORE_SERVERNAME')
pw = os.environ.get('ONCORE_PASSWORD')

# Set variables that can be updated for each notification script
sql_query = "select * from oncore_report_ro.get_latest_rpe_staff_warnings"
alert_name = "RPE Staff Warning"
to_email = 'RPE_SUBMITTER_EMAIL'
email_body = '''\
        <html>
        <head></head>
        <body>
            <p><br>
            Hello,<br><br>
            You recently sent the following protocol(s) via the RPE console in OnCore and the staff listed are missing required IDs to map correctly into Epic. <br>
            Please use the links to review the contact records and enter the appropriate ID into OnCore.<br>
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
       df = pandas.DataFrame(data, columns=col_names)

# Format any url fields as links for html table.       
df['ONCORE_CONTACT_DETAIL_URL'] = df['ONCORE_CONTACT_DETAIL_URL'].apply(lambda x: '<a href="{0}">link</a>'.format(x))

#iterate trough rows and send email
grp = df.groupby(to_email) #group data by email
sent_mail = []
for index, row in df.iterrows():
    if row[to_email] in sent_mail:
        pass
    else:
        toaddr = ["nicholas.vankuren@yale.edu"]
        bcc = ['nicholas.vankuren@yale.edu']
        fromname = "OnCore Notifications"
        fromaddr = "no-reply@oncore_yale.edu"
        sent_mail.append(row[to_email])
        msg = MIMEMultipart("alternative")
        msg["From"] = "{} <{}>".format(fromname, fromaddr)
        msg["To"] = ', ' .join(toaddr)
        msg["Bcc"] = ', ' .join(bcc)
        msg["Subject"] = "OnCore Alert: {}".format(alert_name)
        email_table = df[["PROTOCOL_NO","RPE_SENT_DATE","STAFF_ROLE","STAFF_FULL_NAME","ONCORE_CONTACT_DETAIL_URL"]] 
        table = grp.get_group(row[to_email]).drop(columns=[to_email]).to_html(index = False, render_links=True, escape=False)

        # Create the body of the message (a plain-text and an HTML version).
        text = "Placeholder text for now"
        html = email_body.format(TABLE = table)

        part1 = MIMEText(html, 'html')

        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part1)

        # # Send the message via local SMTP server.
        s = smtplib.SMTP(host="localhost") 
        #s.ehlo()
        # sendmail function takes 3 arguments: sender's address, recipient's address
        # and message to send - here it is sent as one string.
        s.sendmail(fromaddr, (toaddr+bcc), msg.as_string())
        s.quit()



    

