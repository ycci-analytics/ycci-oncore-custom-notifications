import oracledb
import pandas as pd
import os, sys
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

import logging
from logging import Logger
from pathlib import Path

import traceback
import uuid



def _candidate_env_paths():
    paths = []
    # 1) Frozen unpack dir (_MEIPASS)
    if getattr(sys, 'frozen', False):
        try:
            paths.append(os.path.join(sys._MEIPASS, '.env'))
        except Exception:
            pass
        # 2) Directory of the executable
        try:
            exe_dir = os.path.dirname(os.path.abspath(sys.executable))
            paths.append(os.path.join(exe_dir, '.env'))
        except Exception:
            pass
    # 3) Current working directory
    try:
        paths.append(os.path.join(os.getcwd(), '.env'))
    except Exception:
        pass
    # 4) Directory of this utils.py (non-frozen, or as a final fallback)
    try:
        paths.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
    except Exception:
        pass
    # Keep unique order
    seen, unique = set(), []
    for p in paths:
        if p and p not in seen:
            unique.append(p); seen.add(p)
    return unique

_loaded = False
for candidate in _candidate_env_paths():
    if os.path.exists(candidate):
        load_dotenv(candidate)
        _loaded = True
        break
# Optionally, also load from system environment if nothing was found
if not _loaded:
    load_dotenv()  # will no-op if no .env in CWD

def _choose_env(prefix: str, name: str, fallback_env_names=None):
    """
    Helper to choose an env var with optional fallbacks.
    Returns the first non-empty env value among:
      1) f"{prefix}{name}" (e.g., DEV_ONCORE_USER)
      2) each entry in fallback_env_names (in order), if provided
    """
    if fallback_env_names is None:
        fallback_env_names = []
    primary = f"{prefix}{name}" if prefix else name
    val = os.getenv(primary)
    if val:
        return val
    for fb in fallback_env_names:
        v = os.getenv(fb)
        if v:
            return v
    return None

def get_db_credentials(db_type='oracle'):
    """
    Returns DB credentials based on ENVIRONMENT and your .env naming scheme.

    Oracle (preferred names):
      - Prod: ONCORE_USER / ONCORE_PASSWORD / ONCORE_SERVERNAME (DSN string)
      - Dev : DEV_ONCORE_USER / DEV_ONCORE_PASSWORD / DEV_ONCORE_SERVERNAME

    Backward compatibility fallbacks (if ONCORE_* not present):
      - ORACLE_USER / ORACLE_PASSWORD / ORACLE_DSN

    Postgres (unchanged, but supports DEV_* overrides):
      - Prod: POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_HOST / POSTGRES_PORT
      - Dev : DEV_POSTGRES_DB / DEV_POSTGRES_USER / DEV_POSTGRES_PASSWORD / DEV_POSTGRES_HOST / DEV_POSTGRES_PORT
    """
    environment = (os.getenv('ENVIRONMENT') or 'prod').strip().lower()
    is_dev = environment == 'dev'
    dev_prefix = 'DEV_' if is_dev else ''

    if db_type == 'oracle':
        # Preferred naming: ONCORE_* (or DEV_ONCORE_*)
        user = _choose_env(dev_prefix, 'ONCORE_USER', fallback_env_names=['ORACLE_USER'])
        password = _choose_env(dev_prefix, 'ONCORE_PASSWORD', fallback_env_names=['ORACLE_PASSWORD'])
        # Your DSN lives in ONCORE_SERVERNAME (fallback to ORACLE_DSN)
        dsn = _choose_env(dev_prefix, 'ONCORE_SERVERNAME', fallback_env_names=['ORACLE_DSN'])

        if not all([user, password, dsn]):
            missing = [k for k, v in [('user', user), ('password', password), ('dsn', dsn)] if not v]
            raise EnvironmentError(f"Oracle credentials missing required value(s): {', '.join(missing)}")

        return {'user': user, 'password': password, 'dsn': dsn}

    elif db_type == 'postgres':
        # Support DEV_ overrides if present, else fall back to prod names
        dbname = _choose_env(dev_prefix, 'POSTGRES_DB')
        user = _choose_env(dev_prefix, 'POSTGRES_USER')
        password = _choose_env(dev_prefix, 'POSTGRES_PASSWORD')
        host = _choose_env(dev_prefix, 'POSTGRES_HOST')
        port = _choose_env(dev_prefix, 'POSTGRES_PORT')

        if not all([dbname, user, password, host, port]):
            missing = [k for k, v in [('dbname', dbname), ('user', user), ('password', password), ('host', host), ('port', port)] if not v]
            raise EnvironmentError(f"Postgres credentials missing required value(s): {', '.join(missing)}")

        return {'dbname': dbname, 'user': user, 'password': password, 'host': host, 'port': port}

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
        import psycopg
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


# ---------- Reusable logging + monitoring (add to utils.py) ----------
import logging
from logging import Logger
from pathlib import Path
from datetime import datetime
import traceback
import uuid
import pandas as pd  # pandas is already used elsewhere in utils; safe to import
import os

def _today_str():
    return datetime.now().strftime("%Y-%m-%d")

def _ymd_compact():
    return datetime.now().strftime("%Y%m%d")

def get_log_paths(job_code: str, base_dir: str = "logs") -> dict:
    """
    Returns a dict of useful paths for today's run:
      - dir:      logs/<job_code>/<YYYY-MM-DD>/
      - txt:      logs/<job_code>/<YYYY-MM-DD>/<job_code>_<YYYYMMDD>.log
      - sent_csv: logs/<job_code>/<YYYY-MM-DD>/<job_code>_sent_<YYYYMMDD>.csv
    """
    date_str = _today_str()
    ymd = _ymd_compact()
    base = Path(base_dir) / job_code / date_str
    base.mkdir(parents=True, exist_ok=True)
    return {
        "dir": str(base),
        "txt": str(base / f"{job_code}_{ymd}.log"),
        "sent_csv": str(base / f"{job_code}_sent_{ymd}.csv"),
    }

def init_daily_logger(job_code: str, base_dir: str = "logs",
                      level: int = logging.INFO,
                      console: bool = True) -> tuple[Logger, str]:
    """
    Create a logger that writes to today's file. Returns (logger, log_file_path).
    """
    paths = get_log_paths(job_code, base_dir)
    log_path = paths["txt"]

    logger = logging.getLogger(f"{job_code}_logger")
    logger.setLevel(level)
    # Avoid duplicate handlers if called twice in same process
    if not logger.handlers:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        if console:
            ch = logging.StreamHandler()
            ch.setFormatter(fmt)
            logger.addHandler(ch)
    return logger, log_path

def append_sent_records(job_code: str, rows, base_dir: str = "logs",
                        add_metadata: dict | None = None) -> str:
    """
    Append successful notification rows to today's CSV.
    `rows` can be:
      - a pandas DataFrame, or
      - a list of dicts (will be converted to DataFrame).
    Adds optional metadata (job_run_id, environment, etc.) as extra columns.
    Returns the CSV path.
    """
    paths = get_log_paths(job_code, base_dir)
    csv_path = paths["sent_csv"]

    if isinstance(rows, pd.DataFrame):
        df = rows.copy()
    elif isinstance(rows, list):
        df = pd.DataFrame(rows)
    else:
        raise TypeError("append_sent_records expects a DataFrame or a list of dicts")

    # Attach metadata
    meta = add_metadata or {}
    meta_cols = {k: [v] * len(df) for k, v in meta.items()}
    if meta_cols:
        meta_df = pd.DataFrame(meta_cols)
        df = pd.concat([df.reset_index(drop=True), meta_df.reset_index(drop=True)], axis=1)

    # Add a run timestamp column for audit
    if "run_timestamp" not in df.columns:
        df["run_timestamp"] = datetime.now().isoformat(timespec="seconds")

    # Append with header if file doesn't exist
    header = not Path(csv_path).exists()
    df.to_csv(csv_path, mode="a", index=False, header=header, encoding="utf-8")
    return csv_path

def _resolve_monitor_to(notification_prefix: str | None = None) -> str | None:
    """
    Determine who should receive failure alerts in PROD.
    Priority:
      1) <PREFIX>MONITOR_TO (e.g., PROCALT_MONITOR_TO)
      2) MONITOR_TO
      3) None (caller may rely on ENVIRONMENT=dev to route to DEV_EMAIL)
    """
    if notification_prefix:
        key = f"{notification_prefix}MONITOR_TO".upper()
        val = os.getenv(key)
        if val:
            return val
    return os.getenv("MONITOR_TO")

def send_failure_alert(job_code: str, error: Exception,
                       logger: Logger | None = None,
                       notification_prefix: str | None = None,
                       extra_context: dict | None = None,
                       fromname: str | None = None,
                       fromaddr: str | None = None) -> None:
    """
    Compose and send a failure email with concise details (no attachments).
    In dev, send_email() routes to DEV_EMAIL automatically.
    In prod, sends to MONITOR_TO or <PREFIX>MONITOR_TO.
    """
    try:
        # Gather context
        env = (os.getenv("ENVIRONMENT") or "prod").lower()
        ctx = extra_context or {}
        job_run_id = ctx.get("job_run_id") or str(uuid.uuid4())
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Last few log lines (best effort)
        last_lines_html = ""
        try:
            paths = get_log_paths(job_code)
            log_file = paths["txt"]
            if os.path.exists(log_file):
                with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                    tail = "".join(lines[-50:])  # last 50 lines
                    last_lines_html = (
                        "<pre style='white-space:pre-wrap;background:#f6f8fa;padding:8px;border:1px solid #e1e4e8;'>"
                        + (tail.replace("<", "&lt;").replace(">", "&gt;"))
                        + "</pre>"
                    )
        except Exception:
            pass

        # Exception detail
        exc_html = (
            "<pre style='white-space:pre-wrap;background:#fff5f5;padding:8px;border:1px solid #fed7d7;'>"
            + (traceback.format_exc().replace("<", "&lt;").replace(">", "&gt;"))
            + "</pre>"
        )

        subject = f"[Monitor][{env.upper()}] Job '{job_code}' FAILED (run_id={job_run_id})"
        meta_html_items = "".join(
            f"<li><strong>{k}</strong>: {v}</li>" for k, v in ctx.items()
        )
        body = f"""
            <div style="font-family:Segoe UI,Arial,sans-serif;font-size:13px;color:#24292f;">
              <p><strong>Job Failure</strong></p>
              <ul>
                <li><strong>Job</strong>: {job_code}</li>
                <li><strong>Run ID</strong>: {job_run_id}</li>
                <li><strong>When</strong>: {now}</li>
                <li><strong>Environment</strong>: {env}</li>
                {meta_html_items}
              </ul>
              <p><strong>Error Traceback</strong></p>
              {exc_html}
              <p><strong>Last log lines</strong></p>
              {last_lines_html or '<p><em>No log lines available</em></p>'}
              <p style="color:#6a737d;">This alert was generated automatically.</p>
            </div>
        """

        # Determine recipients
        to_prod = _resolve_monitor_to(notification_prefix=notification_prefix)
        # Fall back to passing any placeholder; in dev, send_email() ignores 'to' and uses DEV_EMAIL
        to_email = to_prod or "dev-placeholder@yale.edu"

        # From values (reuse optional global email names if provided via env)
        fromname = fromname or os.getenv("MONITOR_FROM_NAME", "OnCore Job Monitor")
        fromaddr = fromaddr or os.getenv("MONITOR_FROM_ADDR", "oncore-monitor@yale.edu")

        # Send
        send_email(to_email=to_email, fromname=fromname, fromaddr=fromaddr, subject=subject, body=body)

        # Also log to file if logger provided
        if logger:
            logger.error(f"[ALERT SENT] {subject}")

    except Exception as e2:
        # Never let alerting raise; just log to stderr if logger not available
        try:
            if logger:
                logger.error(f"[ALERT FAILURE] Could not send failure alert: {e2}")
        except Exception:
            pass