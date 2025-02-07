from utils import query_database, save_to_csv, send_email
import pandas as pd
from email.mime.base import MIMEBase
import io

# Variables that can be updated for each notification script
sql_query = "select * from oncore_report_ro.ycci_visit_tracking where visit_in_next_5_days = 'yes' and rownum < 10"
to_email = 'COORDINATOR_EMAIL'
notification_name = "Upcoming Visits Next 5 days"
url_field = "CRA_CONSOLE_VISIT_URL"
email_table_columns = ["PROTOCOL_NO", "SEQUENCE_NUMBER", "SEGMENT_NAME", "VISIT_DATE", "VISIT_NAME", f'{url_field}_HTML']

email_body_template = '''
<html>
<head></head>
<body>
<p><br>
Hello,<br><br>
The included report provides an overview of upcoming subject visits that are planned for this work week.<br>
Please use the links to the study visit records in OnCore to review and mark as occurred, missed, or N/A or update the visit date if applicable.<br>
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
            email_table = group[email_table_columns].sort_values(by=['VISIT_DATE','PROTOCOL_NO', 'SEQUENCE_NUMBER'])
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

            
