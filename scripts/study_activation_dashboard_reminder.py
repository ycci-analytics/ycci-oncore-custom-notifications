from utils import query_database, save_to_csv, send_email
import pandas as pd
from email.mime.base import MIMEBase
import io
import os
import logging
from datetime import datetime
import sys
import re
from typing import List

# Create logs directory if it doesn't exist

# Get the directory where the .exe is running
exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
log_dir = os.path.join(exe_dir, 'script_logs')
os.makedirs(log_dir, exist_ok=True)


# Create a timestamped log filename
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_filename = f'activation_dashboard_{timestamp}.log'
log_path = os.path.join(log_dir, log_filename)

# Configure logging
logging.basicConfig(
    filename=log_path,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("Script started.")

# Variables that can be updated for each notification script
sql_query = """
SELECT distinct hic_irb_no, pi_contact_email, business_office_contact, ycci_ir_submitter_email__c, primary_study_coordinator_email
FROM prod_analytics.ycci_activation
WHERE case_status = 'In Activation' OR closed_more_than_30_days = 'no'
"""
notification_name = "YCCI Study Activation Dashboard"


email_body_template = '''
<html>
<head></head>
<body>
<p><br>
Hello,<br><br>
You currently have projects within your department/portfolio that are either in activation with YCCI or have been activated within the last 30 days. Below is a link to your YCCI Study Activation Dashboard where you can view the status of these projects. You can access this dashboard at any time by bookmarking the link.<br>
</p>
<p>
<a href="https://app.powerbi.com/links/A8hFQ4FKaP?ctid=dd8cbebb-2139-4df8-b411-4e3e87abeb5c&pbi_source=linkShare">YCCI Study Activation Dashboard</a>
</p>
<p>
To access the YCCI Study Activation User Guide, click the link below. This user guide provides an overview of the dashboard, including key metric and data definitions and a detailed overview of dashboard visuals. This user guide can also be accessed at any time by bookmarking the link.
</p>
<p>
<a href="https://yaleedu.sharepoint.com/:u:/s/YCCIAnalytics/EYP6cMFLkxlCuKMmvNp6yRIBq35dMrPcTMwZnIoX-a2WUA?e=QQ6NxV">YCCI Study Activation Dashboard User Guide</a>
</p>
<p>
For any questions about the status of your project(s), please reach out to the appropriate contact listed within the dashboard.
</p>
<p>
  Thank you,
  <br>
  Yale Center for Clinical Investigation
</p>
</body>
</html>
'''

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

def get_email_list(activation_df: pd.DataFrame, email_csv_path: str = 'email_list.csv') -> List[str]:
    """
    Build the recipient list as:
      (All valid emails found in activation_df email columns)
      UNION
      (leadership emails from email_list.csv where type == 'leadership')

    Notes:
    - No 'standard' filtering from CSV; that requirement is removed.
    - Handles multiple addresses separated by commas/semicolons/whitespace.
    """
    email_fields = [
        'pi_contact_email',
        'business_office_contact',
        'ycci_ir_submitter_email__c',
        'primary_study_coordinator_email'
    ]

    recipients: set[str] = set()

    # 1) Collect emails from DataFrame
    for field in email_fields:
        if field not in activation_df.columns:
            logging.warning(f"Expected email field '{field}' not found in DataFrame.")
            continue

        series = (
            activation_df[field]
            .dropna()
            .astype(str)
            .str.replace(r'[\r\n]+', ' ', regex=True)
            .str.split(r'[;, ]+')  # split on semicolons, commas, or whitespace
        )

        for parts in series:
            for addr in parts:
                addr = addr.strip().lower()
                if addr and EMAIL_REGEX.match(addr):
                    recipients.add(addr)

    # 2) Add leadership emails from CSV (if available)
    try:
        email_df = pd.read_csv(email_csv_path)  # Columns: email, type
        leadership_emails = (
            email_df[email_df['type'].str.lower() == 'leadership']['email']
            .dropna()
            .astype(str)
            .str.strip()
            .str.lower()
            .tolist()
        )
        for addr in leadership_emails:
            if EMAIL_REGEX.match(addr):
                recipients.add(addr)
        logging.info(f"Loaded {len(leadership_emails)} leadership emails from {email_csv_path}.")
    except Exception as e:
        logging.warning(f"Could not load leadership emails from {email_csv_path}: {e}")

    email_list = sorted(recipients)
    logging.info(f"Built final recipient list with {len(email_list)} unique addresses.")
    return email_list

def main():
    try:
        db_type = 'postgres'
        df = query_database(sql_query, db_type=db_type)
        save_to_csv(df)
        logging.info("Saved query results to CSV.")
    except Exception as e:
        logging.error(f"Error in main query or saving CSV: {e}")
        return

    # Build recipients from DataFrame + leadership from CSV
    email_list = get_email_list(df, email_csv_path='email_list.csv')
    logging.info(f"Email list generated with {len(email_list)} recipients.")

    fromname = os.getenv('EMAIL_FROM_NAME', 'no-reply.YCCI')
    fromaddr = os.getenv('EMAIL_FROM_ADDRESS', 'no-reply.ycci@yale.edu')
    email_body = email_body_template

    sent_mail = []

    for email in email_list:
        if email in sent_mail:
            continue
        try:
            send_email(email,fromname, fromaddr, notification_name, email_body,)
            logging.info(f"Email sent to {email}")
            sent_mail.append(email)        
        except Exception as e:
            logging.error(f"Failed to send email to {email}: {e}")


        

if __name__ == "__main__":
    main()
    logging.info("Script finished.")