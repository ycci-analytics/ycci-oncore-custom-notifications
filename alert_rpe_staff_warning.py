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

un = os.environ.get('ONCORE_USER')
cs = os.environ.get('ONCORE_SERVERNAME')
pw = os.environ.get('ONCORE_PASSWORD')

print(un)

# connect to oracle application database and get a dataframe from relevant query
with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
    with connection.cursor() as cursor:
       cursor.execute("select * from oncore_report_ro.get_latest_rpe_staff_warnings")
       col_names = [c.name for c in cursor.description]
       data = cursor.fetchall()
       df = pandas.DataFrame(data, columns=col_names)
       
df['ONCORE_CONTACT_DETAIL_URL'] = df['ONCORE_CONTACT_DETAIL_URL'].apply(lambda x: '<a href="{0}">link</a>'.format(x))

#iterate trough rows and send email
grp = df.groupby('RPE_SUBMITTER_EMAIL') #group data by email
sent_mail = []
for index, row in df.iterrows():
    if row['RPE_SUBMITTER_EMAIL'] in sent_mail:
        pass
    else:
        alertname = "RPE Staff Warning"
        toaddr = "nicholas.vankuren@yale.edu"
        fromname = "OnCore Notifications - {}".format(alertname)
        fromaddr = "no-replyt@oncore_yale.edu"
        sendemail = row['RPE_SUBMITTER_EMAIL']
        sent_mail.append(sendemail)
        msg = MIMEMultipart("alternative")
        msg["From"] = "{} <{}>".format(fromname, fromaddr)
        msg["To"] = toaddr
        msg["Subject"] = "OnCore Alert: RPE Staff Warning"
        table = grp.get_group(sendemail).to_html(index = False, render_links=True)
        table = html.unescape(table)


        # Create the body of the message (a plain-text and an HTML version).
        text = "Hello,\nHow are you?\nHere is the link you wanted:\nhttp://www.python.org"
        html = '''\
        <html>
        <head></head>
        <body>
            <p><br>
            Hello,<br>
            You recently sent the following protocol(s) via the RPE in OnCore and the staff listed are missing required IDs to map correctly into Epic. <br>
            Please use the links to review the contact records and enter the appropriate ID into OnCore.<br>
            </p>
            <p>
            {TABLE}
            </p>
        </body>
        </html>
        '''.format(TABLE = table)

        print(html)

        # # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(html, 'html')

        # # Attach parts into message container.
        # # According to RFC 2046, the last part of a multipart message, in this case
        # # the HTML message, is best and preferred.
        msg.attach(part1)
        msg.attach(part2)

        # # Send the message via local SMTP server.
        s = smtplib.SMTP(host="localhost") 
        #s.ehlo()
        # sendmail function takes 3 arguments: sender's address, recipient's address
        # and message to send - here it is sent as one string.
        s.sendmail(fromaddr, toaddr, msg.as_string())
        s.quit()



    

