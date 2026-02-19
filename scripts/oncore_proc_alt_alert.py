#!/usr/bin/env python3
"""
OnCore — Procedure Alternatives Not Selected
Hourly notifier with robust windowing, per-recipient grouping, logging, and monitoring.

Behavior:
- One email per (visit_id, modified_user_email, modified_date).
- Email body includes a table of all missing clinical procedures (rows for that key-group).
- Safe, fixed window per run with safety lag and overlap (no misses, no duplicates).
- Daily TXT log + daily CSV of sent notifications.
- Failure alert email on exceptions.

Pack (example):
  python -m PyInstaller --onefile main.py --name oncore_proc_alt_alert
"""

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# ----- import shared helpers from your utils.py -----
from utils import (
    query_database,
    send_email,
    init_daily_logger,
    append_sent_records,
    send_failure_alert,
)

# ==========================
# Configuration (ENV-driven)
# ==========================
# Notification prefix for per-job ENV vars
PREFIX = "PROCALT_"

def penv(name: str, default: str | None = None) -> str | None:
    return os.getenv(f"{PREFIX}{name}", default)

# Job identity (for logging paths, monitor overrides, etc.)
JOB_CODE = "epay_no_procalt"

# View + column names (pull from .env; defaults are placeholders)
VIEW_FQN   = penv("VIEW_FQN", "SCHEMA.VIEW_NAME")
COL_VISIT  = penv("VISIT_ID_COL", "VISIT_ID")
COL_MODTS  = penv("MODIFIED_DATE_COL", "MODIFIED_DATE")
COL_USER   = penv("MODIFIED_USER_EMAIL_COL", "MODIFIED_USER_EMAIL")
COL_PROTOCOL = penv("PROTOCOL_NO_COL", "PROTOCOL_NO")
COL_SUBJECT  = penv("SUBJECT_NAME_COL", "SUBJECT_NAME")
COL_VDATE    = penv("VISIT_DATE_COL", "VISIT_DATE")
COL_VNAME    = penv("VISIT_NAME_COL", "VISIT_NAME")
COL_PROC     = penv("CLINICAL_PROCEDURE_COL", "CLINICAL_PROCEDURE")

# Email sender identity
FROM_NAME  = penv("ALERT_FROM_NAME", "OnCore Alerts")
FROM_ADDR  = penv("ALERT_FROM_ADDR", "no-reply@example.org")

# Windowing
LOOKBACK_HOURS = int(penv("LOOKBACK_HOURS", "1"))                 # first-run sweep
SAFETY_LAG_MIN = int(penv("SAFETY_LAG_MIN", "10"))                # avoids in-flight changes
OVERLAP_MIN    = int(penv("OVERLAP_MIN", "3"))                    # overlap to avoid edge misses

# Optional: where to store logs
LOG_DIR = os.getenv("LOG_DIR", "logs")

# ==========================
# State & sent-keys helpers
# ==========================
def state_path(dev_mode: bool) -> Path:
    """Return per-env state path (separate file for dev vs prod)."""
    app_name = penv("STATE_APP_NAME", "OnCoreProcAltAlert") + ("_dev" if dev_mode else "")
    if os.name == "nt":
        base = os.getenv("APPDATA", str(Path.home() / "AppData" / "Roaming"))
        return Path(base) / app_name / "state.json"
    return Path.home() / ".local" / "share" / app_name / "state.json"

def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_state(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def load_sent_keys(s_path: Path) -> set[str]:
    p = Path(str(s_path)).with_name("sent_keys.json")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return set(data.get("keys", []))
    except Exception:
        return set()

def save_sent_keys(s_path: Path, keys: set[str], max_keep: int = 20000) -> None:
    p = Path(str(s_path)).with_name("sent_keys.json")
    keys_list = list(keys)
    if len(keys_list) > max_keep:
        keys_list = keys_list[-max_keep:]
    p.write_text(json.dumps({"keys": keys_list}, indent=2), encoding="utf-8")

# ==========================
# SQL builder
# ==========================
def _fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def build_sql(since_dt: datetime, until_dt: datetime) -> str:
    """
    Use an open/closed window: (since, until]
    NOTE: If COL_MODTS is a DATE (no time), switch TO_TIMESTAMP -> TO_DATE appropriately.
    """
    return f"""
        SELECT
            {COL_VISIT}      AS visit_id,
            {COL_MODTS}      AS modified_date,
            {COL_USER}       AS modified_user_email,
            {COL_PROTOCOL}   AS protocol_no,
            {COL_SUBJECT}    AS subject_name,
            {COL_VDATE}      AS visit_date,
            {COL_VNAME}      AS visit_name,
            {COL_PROC}       AS clinical_procedure
        FROM {VIEW_FQN}
        WHERE {COL_MODTS} >  TO_TIMESTAMP('{_fmt_ts(since_dt)}','YYYY-MM-DD HH24:MI:SS')
          AND {COL_MODTS} <= TO_TIMESTAMP('{_fmt_ts(until_dt)}','YYYY-MM-DD HH24:MI:SS')
    """

# ==========================
# Email HTML builders
# ==========================
def _fmt_date(val) -> str:
    try:
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return str(val or "")

def _section_intro(protocol_no, subject_name, visit_date, missed_count) -> str:
    return f"""
      <p>Hello,</p>
      <p>
        You recently modified a visit in OnCore for the following Subject and ePayment procedure alternatives were not selected.
        Please review the visit and select an appropriate choice for all ePayment procedures.
      </p>
      <p>
        <strong>Protocol:</strong> {'' if pd.isna(protocol_no) else protocol_no}<br/>
        <strong>Subject:</strong> {'' if pd.isna(subject_name) else subject_name}<br/>
        <strong>Visit Date:</strong> {_fmt_date(visit_date)}<br/>
        <strong>Missed Procedures (count):</strong> {missed_count}
      </p>
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

def build_visit_email_html(g: pd.DataFrame, since_dt: datetime, until_dt: datetime) -> str:
    """
    Build the email body for a single (visit_id, modified_user_email, modified_date) group.
    """
    g = g.rename(columns=str.lower).copy().sort_values(["visit_date", "clinical_procedure"])
    last_row   = g.iloc[-1]  # metadata should be constant across rows for the group
    intro_html = _section_intro(last_row.get("protocol_no"), last_row.get("subject_name"),
                                last_row.get("visit_date"), len(g))
    window_html = f"<p>Window: {since_dt:%Y-%m-%d %H:%M} → {until_dt:%Y-%m-%d %H:%M}</p>"
    table_html  = _section_table(g[["visit_date","visit_name","clinical_procedure"]])
    footer  = "<p style='color:#6a737d'>This email was generated automatically.</p>"
    return f"<div style='font-family:Segoe UI,Arial,sans-serif;font-size:13px'>{window_html}{intro_html}{table_html}{footer}</div>"

# ==========================
# Main
# ==========================
def main(argv=None):
    # --------- CLI args & environment ---------
    parser = argparse.ArgumentParser(description="OnCore ProcAlt Not Selected notifier")
    parser.add_argument("--dev", action="store_true",
                        help="Run in dev mode: use DEV_* DB creds and DEV_EMAIL recipients")
    args = parser.parse_args(argv)

    if args.dev:
        os.environ["ENVIRONMENT"] = "dev"
    else:
        os.environ.setdefault("ENVIRONMENT", "prod")
    dev_mode = (os.getenv("ENVIRONMENT", "prod").lower() == "dev")

    # --------- Logging ---------
    logger, log_file = init_daily_logger(JOB_CODE, base_dir=LOG_DIR)
    job_run_id = str(uuid.uuid4())
    logger.info(f"Starting job '{JOB_CODE}' (run_id={job_run_id}) in ENVIRONMENT={os.getenv('ENVIRONMENT')}")

    # --------- Optional lock to avoid overlap ---------
    lock_dir = Path(LOG_DIR) / JOB_CODE / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "job.lock"
    if lock_path.exists():
        mtime = datetime.fromtimestamp(lock_path.stat().st_mtime)
        if datetime.now() - mtime < timedelta(hours=2):
            logger.warning("Another instance appears to be running. Exiting.")
            return  # do not treat as failure
        else:
            logger.warning("Found stale lock; replacing.")
    lock_path.write_text(datetime.now().isoformat(timespec="seconds"))

    try:
        # --------- Fixed window with safety-lag & overlap ---------
        now = datetime.now()
        t0 = now
        until = t0 - timedelta(minutes=SAFETY_LAG_MIN)
        if until >= t0:
            until = t0 - timedelta(minutes=1)

        s_path = state_path(dev_mode)
        state = load_state(s_path)

        if "last_max_timestamp" in state:
            since_raw = datetime.fromisoformat(state["last_max_timestamp"])
        else:
            since_raw = until - timedelta(hours=LOOKBACK_HOURS)

        since = since_raw - timedelta(minutes=OVERLAP_MIN)
        logger.info(f"Window (open, closed]: {since.isoformat(timespec='seconds')} → {until.isoformat(timespec='seconds')}")

        # --------- Query ---------
        sql = build_sql(since, until)
        logger.info("Querying Oracle…")
        df = query_database(sql_query=sql, db_type="oracle")
        logger.info(f"Query complete. Rows returned: {0 if df is None else len(df)}")

        if df is None or df.empty:
            logger.info("No rows found. Advancing watermark to 'until' and exiting.")
            state["last_max_timestamp"] = until.isoformat(timespec="seconds")
            save_state(s_path, state)
            logger.info("Job completed successfully (no notifications).")
            return

        # --------- Normalize & load dedupe keys ---------
        dfl = df.rename(columns=str.lower).copy()
        sent_keys = load_sent_keys(s_path)
        sent_rows = []

        # --------- Group & send: one email per (visit_id, modified_user_email, modified_date) ---------
        group_cols = ["visit_id", "modified_user_email", "modified_date"]
        for (visit_id, modifier, mod_ts), g in dfl.groupby(group_cols, dropna=False):

            if not modifier and not dev_mode:
                logger.warning(f"Skipping visit {visit_id}: empty MODIFIED_USER_EMAIL")
                continue

            # Idempotency key: visit + recipient + ts (seconds precision)
            mod_dt = pd.to_datetime(mod_ts).to_pydatetime()
            dedupe_key = f"{visit_id}|{modifier or 'dev'}|{mod_dt.isoformat(timespec='seconds')}"

            if dedupe_key in sent_keys:
                logger.info(f"Skip duplicate for visit {visit_id}, recipient {modifier}, ts {mod_dt} (key={dedupe_key})")
                continue

            missed_cnt = len(g)
            subject = f"[OnCore] Procedure Alternatives Missing — Visit {visit_id}: {missed_cnt} missed"
            body    = build_visit_email_html(
                        g.sort_values(["visit_date", "clinical_procedure"]),
                        since, until
                     )

            if dev_mode:
                logger.info(f"(DEV) Sending test email for visit {visit_id} @ {mod_dt} ({missed_cnt} rows) to DEV_EMAIL")
                send_email(to_email=(modifier or "dev-placeholder"),
                           fromname=FROM_NAME, fromaddr=FROM_ADDR,
                           subject=subject, body=body)
            else:
                logger.info(f"Sending email for visit {visit_id} @ {mod_dt} to {modifier} ({missed_cnt} rows)")
                send_email(to_email=modifier,
                           fromname=FROM_NAME, fromaddr=FROM_ADDR,
                           subject=subject, body=body)

            # Mark as sent & collect rows for the daily CSV
            sent_keys.add(dedupe_key)
            g_for_csv = g[["visit_date", "visit_name", "clinical_procedure"]].copy()
            g_for_csv["visit_id"]    = visit_id
            g_for_csv["modified_ts"] = mod_dt.isoformat(timespec="seconds")
            g_for_csv["recipient"]   = modifier if not dev_mode else os.getenv("DEV_EMAIL")
            g_for_csv["subject"]     = subject
            g_for_csv["job_run_id"]  = job_run_id
            sent_rows.append(g_for_csv)

        # Persist dedupe keys
        save_sent_keys(s_path, sent_keys)

        # Write daily "sent notifications" CSV
        if sent_rows:
            sent_df = pd.concat(sent_rows, ignore_index=True)
            csv_path = append_sent_records(
                JOB_CODE, sent_df,
                add_metadata={"environment": os.getenv("ENVIRONMENT")}
            )
            logger.info(f"Wrote sent notifications CSV: {csv_path}")
        else:
            logger.info("No notifications were sent in this run.")

        # --------- Advance watermark (capped at 'until') ---------
        try:
            max_seen = pd.to_datetime(df[COL_MODTS]).max().to_pydatetime()
        except Exception:
            try:
                max_seen = pd.to_datetime(dfl["modified_date"]).max().to_pydatetime()
            except Exception:
                max_seen = until

        new_mark = min(max_seen, until)
        state["last_max_timestamp"] = new_mark.isoformat(timespec="seconds")
        save_state(s_path, state)
        logger.info(f"Advanced watermark to {state['last_max_timestamp']} (capped at 'until')")
        logger.info("Job completed successfully.")

    except Exception as e:
        # Log and alert (send_failure_alert routes dev to DEV_EMAIL; prod to MONITOR_TO / PROCALT_MONITOR_TO)
        try:
            logger.exception("Unhandled exception during job run.")
            send_failure_alert(
                job_code=JOB_CODE,
                error=e,
                logger=logger,
                notification_prefix=PREFIX,  # checks PROCALT_MONITOR_TO then MONITOR_TO
                extra_context={
                    "job_run_id": job_run_id,
                    "log_file": str(log_file),
                    "env": os.getenv("ENVIRONMENT"),
                },
                fromname=os.getenv("ALERT_FROM_NAME", FROM_NAME),
                fromaddr=os.getenv("ALERT_FROM_ADDR", FROM_ADDR),
            )
        finally:
            # Reraise so the scheduler can treat as failure
            raise
    finally:
        # Release lock
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass

if __name__ == "__main__":
    sys.exit(main())