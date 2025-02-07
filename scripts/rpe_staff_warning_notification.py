from utils import query_database, save_to_csv, send_email
import pandas as pd
from email.mime.base import MIMEBase
import io

# Variables that can be updated for each notification script
sql_query = "select * from oncore_report_ro.get_latest_rpe_staff_warnings"
to_email = 'RPE_SUBMITTER_EMAIL'
notification_name = "RPE Staff Warning"
url_field = "ONCORE_CONTACT_DETAIL_URL"
email_table_columns = ["PROTOCOL_NO","RPE_SENT_DATE","STAFF_ROLE","STAFF_FULL_NAME","ONCORE_CONTACT_DETAIL_URL"]
email_body_template = '''\
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

def main():
    df = query_database(sql_query)
    save_to_csv(df)

    df[f'{url_field}_HTML'] = df[url_field].apply(lambda x: f'<a href="{x}">link</a>')
    grouped_df = df.groupby(to_email)
    sent_mail = []

    for email, group in grouped_df:
        if email in sent_mail:
            pass
        else:
            sent_mail.append(email)
            email_table = group[email_table_columns].sort_values(by=['PROTOCOL_NO', 'SEQUENCE_NUMBER'])
            if len(email_table) > 20:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    email_table.to_excel(writer, index=False, sheet_name='Sheet1')
                    workbook = writer.book
                    worksheet = writer.sheets['Sheet1']
                    for idx, url in enumerate(email_table[url_field], start=1):
                        worksheet.write_url(f'E{idx+2}', url, string='link')
                    excel_buffer.seek(0)
                attachment = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                attachment.set_payload(excel_buffer.read())
                email_body = email_body_template.replace('{TABLE}', '')
                filename = f"{notification_name.replace(' ', '_').lower()}.xlsx"
            else:
                html_table = email_table.to_html(index=False, render_links=True, escape=False)
                html_table = f"<style>th {{ text-align: left; }}</style>{html_table}"
                email_body = email_body_template.format(TABLE=html_table)
                attachment = None
                filename = None

        send_email(email, f"OnCore Notification: {notification_name}", email_body, filename, attachment)

if __name__ == "__main__":
    main()