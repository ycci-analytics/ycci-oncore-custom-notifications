#!/usr/bin/env python3
"""
OnCore — Procedure Alternatives Weekly Reminder

Behavior:
- One reminder email per (visit_id, modified_user_email).
- Email body includes all currently unresolved clinical procedures for that grouping.
- No modified_date windowing or watermark logic; this queries current unresolved rows each run.
- Daily TXT log + daily CSV of sent reminders.
- Failure alert email on exceptions.
"""

import argparse
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from utils import (
    query_database,
    execute_database,
    send_email,
    init_daily_logger,
    append_sent_records,
    send_failure_alert,
)

PREFIX = "PROCALTWEEKLY_"


def penv(name: str, default: str | None = None) -> str | None:
    return os.getenv(f"{PREFIX}{name}", default)


JOB_CODE = "epay_no_procalt_weekly_reminder"

VIEW_FQN = penv("VIEW_FQN", "epayment_proc_alternative_qa")
COL_VISIT = penv("VISIT_ID_COL", "VISIT_ID")
COL_USER = penv("MODIFIED_USER_EMAIL_COL", "MODIFIED_USER_EMAIL")
COL_USERNAME = penv("MODIFIED_USER_NAME_COL", "MODIFIED_USER_NAME")
COL_PROTOCOL = penv("PROTOCOL_NO_COL", "PROTOCOL_NO")
COL_SUBJECT = penv("SUBJECT_NAME_COL", "SUBJECT_NAME")
COL_VDATE = penv("VISIT_DATE_COL", "VISIT_DATE")
COL_VNAME = penv("VISIT_NAME_COL", "VISIT_NAME")
COL_PROC = penv("CLINICAL_PROCEDURE_COL", "CLINICAL_PROCEDURE")

FROM_NAME = penv("ALERT_FROM_NAME", "OnCore Alerts")
FROM_ADDR = penv("ALERT_FROM_ADDR", "no-reply@yale.edu")

LOG_DIR = os.getenv("LOG_DIR", "logs")
REMINDER_INTERVAL_DAYS = int(penv("REMINDER_INTERVAL_DAYS", "7"))
INITIAL_JOB_CODE = penv("INITIAL_JOB_CODE", "epay_no_procalt")
AUDIT_TABLE = penv("AUDIT_TABLE", os.getenv("AUDIT_TABLE", "NOTIFICATION_AUDIT"))
AUDIT_SCHEMA_PROD = penv("AUDIT_SCHEMA_PROD", os.getenv("AUDIT_SCHEMA_PROD"))
AUDIT_SCHEMA_DEV = penv("AUDIT_SCHEMA_DEV", os.getenv("AUDIT_SCHEMA_DEV"))


def build_sql() -> str:
    return f"""
        SELECT
            {COL_VISIT}    AS visit_id,
            {COL_USER}     AS modified_user_email,
            {COL_USERNAME} AS modified_user_name,
            {COL_PROTOCOL} AS protocol_no,
            {COL_SUBJECT}  AS subject_name,
            {COL_VDATE}    AS visit_date,
            {COL_VNAME}    AS visit_name,
            {COL_PROC}     AS clinical_procedure
        FROM {VIEW_FQN}
    """

def _audit_table_fqn(dev_mode: bool) -> str:
    schema = AUDIT_SCHEMA_DEV if dev_mode else AUDIT_SCHEMA_PROD
    return f"{schema}.{AUDIT_TABLE}" if schema else AUDIT_TABLE

def _audit_history_sql(table_fqn: str) -> str:
    return f"""
        SELECT
            TO_CHAR(VISIT_ID) AS visit_id,
            LOWER(TRIM(MODIFIED_USER_EMAIL)) AS modified_user_email,
            MIN(CASE WHEN EVENT_TYPE = 'INITIAL_ALERT' THEN SENT_AT END) AS first_initial_sent_at,
            MAX(CASE WHEN EVENT_TYPE = 'WEEKLY_REMINDER' THEN SENT_AT END) AS last_weekly_sent_at
        FROM {table_fqn}
        WHERE ENVIRONMENT = :environment
          AND JOB_CODE IN (:initial_job_code, :weekly_job_code)
        GROUP BY TO_CHAR(VISIT_ID), LOWER(TRIM(MODIFIED_USER_EMAIL))
    """

def write_audit_event(*, dev_mode: bool, visit_id, modified_user_email: str, dedupe_key: str, job_run_id: str) -> None:
    table_fqn = _audit_table_fqn(dev_mode)
    sql = f"""
        INSERT INTO {table_fqn}
            (SENT_AT, JOB_CODE, EVENT_TYPE, VISIT_ID, MODIFIED_USER_EMAIL, DEDUPE_KEY, JOB_RUN_ID, ENVIRONMENT)
        VALUES
            (SYSTIMESTAMP, :job_code, :event_type, :visit_id, :modified_user_email, :dedupe_key, :job_run_id, :environment)
    """
    execute_database(
        sql_query=sql,
        db_type="oracle",
        params={
            "job_code": JOB_CODE,
            "event_type": "WEEKLY_REMINDER",
            "visit_id": str(visit_id) if visit_id is not None else None,
            "modified_user_email": str(modified_user_email) if modified_user_email is not None else None,
            "dedupe_key": dedupe_key,
            "job_run_id": job_run_id,
            "environment": os.getenv("ENVIRONMENT"),
        },
    )


def _fmt_date(val) -> str:
    try:
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return str(val or "")


def _section_intro(modified_user_name, protocol_no, subject_name, visit_date, missed_count) -> str:
    return f"""
      <p>Dear {'' if pd.isna(modified_user_name) else modified_user_name},</p>
      <p><strong>Reminder:</strong> The visit below still includes unresolved ePayment procedure alternatives.</p>
      <p>
        <strong>Protocol:</strong> {'' if pd.isna(protocol_no) else protocol_no}<br/>
        <strong>Subject:</strong> {'' if pd.isna(subject_name) else subject_name}<br/>
        <strong>Visit Date:</strong> {_fmt_date(visit_date)}<br/>
        <strong>Missed Procedures (count):</strong> {missed_count}
      </p>
    """


def _action_section() -> str:
    return """
      <p><strong>Action Required:</strong></p>
      <ul>
        <li>If a payment is required, choose the correct amount from the drop-down list.</li>
        <li>If no payment is required, select the <strong>N/A</strong> checkbox and provide a reason.</li>
        <li>Select <strong>Submit</strong> to save your changes.</li>
      </ul>
      <p>Updating this record allows payment processing to continue.</p>
    """


def _section_table(rows_df: pd.DataFrame) -> str:
    body_rows = "\n".join(
        f"<tr><td>{_fmt_date(r['visit_date'])}</td>"
        f"<td>{r.get('visit_name','')}</td>"
        f"<td>{r.get('clinical_procedure','')}</td></tr>"
        for _, r in rows_df.iterrows()
    )
    return (
        "<table style='border-collapse:collapse' border='1' cellpadding='6'>"
        "<thead><tr><th>VISIT_DATE</th><th>VISIT_NAME</th><th>CLINICAL_PROCEDURE</th></tr></thead>"
        f"<tbody>{body_rows}</tbody></table>"
    )


def build_visit_email_html(g: pd.DataFrame) -> str:
    g = g.rename(columns=str.lower).copy().sort_values(["visit_date", "clinical_procedure"])
    last_row = g.iloc[-1]
    intro_html = _section_intro(
        last_row.get("modified_user_name"),
        last_row.get("protocol_no"),
        last_row.get("subject_name"),
        last_row.get("visit_date"),
        len(g),
    )
    table_html = _section_table(g[["visit_date", "visit_name", "clinical_procedure"]])
    action_html = _action_section()
    footer = (
        "<p style='color:#6a737d'>This reminder was generated automatically. "
        "For questions, email <a href=\"mailto:ycci_data_ops@yale.edu\">ycci_data_ops@yale.edu</a>.</p>"
    )
    return f"<div style='font-family:Segoe UI,Arial,sans-serif;font-size:13px'>{intro_html}{table_html}{action_html}{footer}</div>"


def main(argv=None):
    parser = argparse.ArgumentParser(description="OnCore ProcAlt weekly reminder notifier")
    parser.add_argument("--dev", action="store_true", help="Run in dev mode (routes email to DEV_EMAIL)")
    args = parser.parse_args(argv)

    if args.dev:
        os.environ["ENVIRONMENT"] = "dev"
    else:
        os.environ.setdefault("ENVIRONMENT", "prod")
    dev_mode = (os.getenv("ENVIRONMENT", "prod").lower() == "dev")

    logger, log_file = init_daily_logger(JOB_CODE, base_dir=LOG_DIR)
    job_run_id = str(uuid.uuid4())
    logger.info(f"Starting job '{JOB_CODE}' (run_id={job_run_id}) in ENVIRONMENT={os.getenv('ENVIRONMENT')}")

    lock_dir = Path(LOG_DIR) / JOB_CODE / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "job.lock"
    if lock_path.exists():
        mtime = datetime.fromtimestamp(lock_path.stat().st_mtime)
        if datetime.now() - mtime < timedelta(hours=4):
            logger.warning("Another instance appears to be running. Exiting.")
            return
        logger.warning("Found stale lock; replacing.")
    lock_path.write_text(datetime.now().isoformat(timespec="seconds"))

    try:
        sql = build_sql()
        logger.info("Querying Oracle for unresolved weekly reminder records…")
        df = query_database(sql_query=sql, db_type="oracle")
        logger.info(f"Query complete. Rows returned: {0 if df is None else len(df)}")

        if df is None or df.empty:
            logger.info("No rows found. Job completed successfully (no reminders).")
            return

        dfl = df.rename(columns=str.lower).copy()
        table_fqn = _audit_table_fqn(dev_mode)
        hist_df = query_database(
            sql_query=_audit_history_sql(table_fqn),
            db_type="oracle",
            params={
                "environment": os.getenv("ENVIRONMENT"),
                "initial_job_code": INITIAL_JOB_CODE,
                "weekly_job_code": JOB_CODE,
            },
        ).rename(columns=str.lower)
        history_by_key = {}
        for _, r in hist_df.iterrows():
            key = (str(r.get("visit_id")), str(r.get("modified_user_email") or "").strip().lower())
            history_by_key[key] = {
                "first_initial_sent_at": pd.to_datetime(r.get("first_initial_sent_at"), errors="coerce"),
                "last_weekly_sent_at": pd.to_datetime(r.get("last_weekly_sent_at"), errors="coerce"),
            }

        sent_rows = []
        now_ts = pd.Timestamp.now()

        group_cols = ["visit_id", "modified_user_email"]
        for (visit_id, modifier), g in dfl.groupby(group_cols, dropna=False):
            if not modifier and not dev_mode:
                logger.warning(f"Skipping visit {visit_id}: empty MODIFIED_USER_EMAIL")
                continue
            key = (str(visit_id), str(modifier or "").strip().lower())
            hist = history_by_key.get(key)
            if not hist or pd.isna(hist["first_initial_sent_at"]):
                logger.info(f"Skipping weekly reminder for visit {visit_id} / {modifier}: no INITIAL_ALERT found in audit table")
                continue

            first_initial = hist["first_initial_sent_at"]
            if now_ts < first_initial + pd.Timedelta(days=REMINDER_INTERVAL_DAYS):
                logger.info(
                    f"Skipping weekly reminder for visit {visit_id} / {modifier}: "
                    f"INITIAL_ALERT is newer than {REMINDER_INTERVAL_DAYS} days"
                )
                continue

            last_weekly = hist["last_weekly_sent_at"]
            if not pd.isna(last_weekly) and now_ts < last_weekly + pd.Timedelta(days=REMINDER_INTERVAL_DAYS):
                logger.info(
                    f"Skipping weekly reminder for visit {visit_id} / {modifier}: "
                    f"last weekly reminder is newer than {REMINDER_INTERVAL_DAYS} days"
                )
                continue

            missed_cnt = len(g)
            subject = f"[OnCore Reminder] Procedure Alternatives Still Missing — Visit {visit_id}: {missed_cnt} missed"
            body = build_visit_email_html(g.sort_values(["visit_date", "clinical_procedure"]))
            dedupe_key = f"{visit_id}|{modifier}|{now_ts.strftime('%Y-%m-%d')}"

            if dev_mode:
                logger.info(f"(DEV) Sending weekly reminder for visit {visit_id} ({missed_cnt} rows) to DEV_EMAIL")
                send_email(
                    to_email=(modifier or "dev-placeholder"),
                    fromname=FROM_NAME,
                    fromaddr=FROM_ADDR,
                    subject=subject,
                    body=body,
                )
            else:
                logger.info(f"Sending weekly reminder for visit {visit_id} to {modifier} ({missed_cnt} rows)")
                send_email(
                    to_email=modifier,
                    fromname=FROM_NAME,
                    fromaddr=FROM_ADDR,
                    subject=subject,
                    body=body,
                )
            write_audit_event(
                dev_mode=dev_mode,
                visit_id=visit_id,
                modified_user_email=modifier,
                dedupe_key=dedupe_key,
                job_run_id=job_run_id,
            )

            g_for_csv = g[["visit_date", "visit_name", "clinical_procedure"]].copy()
            g_for_csv["visit_id"] = visit_id
            g_for_csv["recipient"] = modifier if not dev_mode else os.getenv("DEV_EMAIL")
            g_for_csv["subject"] = subject
            g_for_csv["job_run_id"] = job_run_id
            sent_rows.append(g_for_csv)

        if sent_rows:
            sent_df = pd.concat(sent_rows, ignore_index=True)
            csv_path = append_sent_records(
                JOB_CODE, sent_df, add_metadata={"environment": os.getenv("ENVIRONMENT")}
            )
            logger.info(f"Wrote sent reminders CSV: {csv_path}")
        else:
            logger.info("No reminders were sent in this run.")

        logger.info("Job completed successfully.")

    except Exception as e:
        try:
            logger.exception("Unhandled exception during weekly reminder job run.")
            send_failure_alert(
                job_code=JOB_CODE,
                error=e,
                logger=logger,
                notification_prefix=PREFIX,
                extra_context={
                    "job_run_id": job_run_id,
                    "log_file": str(log_file),
                    "env": os.getenv("ENVIRONMENT"),
                },
                fromname=os.getenv("ALERT_FROM_NAME", FROM_NAME),
                fromaddr=os.getenv("ALERT_FROM_ADDR", FROM_ADDR),
            )
        finally:
            raise
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
