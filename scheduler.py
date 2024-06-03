import os
import psycopg2
import time
import logging
from psycopg2.extras import DictCursor
import zulip
from datetime import datetime
from urllib.parse import quote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# timer for counting script time execution
start_time = time.time()

logging.info("-------------------------SCHEDULER IS RUNNING-------------------------")

# Database and Zulip configuration, environment vars are used
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_csv = os.getenv("DB_CSV")
db_orph = os.getenv("DB_ORPH")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
api_key = os.getenv("OTC_BOT_API")

# Zulip stream and topic mapping for each squad
squad_streams = {
    # "Database Squad": {"stream": "Database Squad", "topic": "Doc alerts"},
    # "Big Data and AI Squad": {"stream": "bigdata & ai", "topic": "helpcenter_alerts"},
    # "Compute Squad": {"stream": "compute", "topic": "hc_alerts topic"},
    # "Network Squad": {"stream": "network", "topic": "Alerts_HelpCenter"}
    "Database Squad": {"stream": "4grafana", "topic": "testing"},
    "Big Data and AI Squad": {"stream": "grafana", "topic": "testing"},
    "Orchestration Squad": {"stream": "4grafana", "topic": "testing"},
    "Compute Squad": {"stream": "grafana", "topic": "testing"}
}


def check_env_variables():
    required_env_vars = [
        "DB_HOST", "DB_PORT",
        "DB_CSV", "DB_ORPH", "DB_USER", "DB_PASSWORD", "OTC_BOT_API"
    ]
    for var in required_env_vars:
        if os.getenv(var) is None:
            raise Exception(f"Missing environment variable: {var}")


def connect_to_db(db_name):
    logging.info(f"Connecting to Postgres ({db_name})...")
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            cursor_factory=DictCursor
        )
    except psycopg2.Error as e:
        logging.error(f"Connecting to Postgres: an error occurred while trying to connect to the database: {e}")
        return None


def check_orphans(conn_orph, squad_name, stream_name, topic_name):
    results = []
    cur_orph = conn_orph.cursor()
    tables = ["open_prs", "open_prs_swiss"]
    for table in tables:
        # here each query marked with zone marker (Public or Hybrid) for bring it into message
        if table == "open_prs":
            logging.info(f"Looking for orphaned PRs for {squad_name} in {table}...")
            query = f"""SELECT *, 'Public' as zone, 'orphan' as type FROM {table} WHERE "Squad" = '{squad_name}';"""
            cur_orph.execute(query, (squad_name,))
            results = cur_orph.fetchall()
        elif table == "open_prs_swiss":
            logging.info(f"Looking for orphaned PRs for {squad_name} in {table}...")
            query = f"""SELECT *, 'Hybrid' as zone, 'orphan' as type FROM {table} WHERE "Squad" = '{squad_name}';"""
            cur_orph.execute(query, (squad_name,))
            results = cur_orph.fetchall()
        if results:
            for row in results:
                send_zulip_notification(row, api_key, stream_name, topic_name)


def check_open_issues(conn, squad_name, stream_name, topic_name):
    results = []
    cur = conn.cursor()
    tables = ["open_issues", "open_issues_swiss"]
    for table in tables:
        # here each query marked with zone marker (Public or Hybrid) for bring it into message
        if table == "open_issues":
            logging.info(f"Checking {table} for {squad_name}")
            query = f"""SELECT *, 'Public' as zone, 'issue' as type FROM {table} WHERE "Squad" = '{squad_name}' AND "Environment" = 'Github' AND "Assignees" = '' AND "Duration" > '7' ;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        elif table == "open_issues_swiss":
            logging.info(f"Checking {table} for {squad_name}")
            query = f"""SELECT *, 'Hybrid' as zone, 'issue' as type FROM {table} WHERE "Squad" = '{squad_name}' AND "Environment" = 'Github' AND "Assignees" = '' AND "Duration" > '7' ;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        if results:
            for row in results:
                send_zulip_notification(row, api_key, stream_name, topic_name)


def check_outdated_docs(conn, squad_name, stream_name, topic_name):
    results = []
    cur = conn.cursor()
    tables = ["last_update_commit", "last_update_commit_swiss"]
    for table in tables:
        # here each query marked with zone marker (Public or Hybrid) for bring it into message
        if table == "last_update_commit":
            logging.info(f"Checking {table} table for {squad_name}...")
            query = f"""SELECT *, 'Public' as zone, 'doc' as type FROM {table} WHERE "Squad" = %s;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        elif table == "last_update_commit_swiss":
            logging.info(f"Checking {table} table for {squad_name}...")
            query = f"""SELECT *, 'Hybrid' as zone, 'doc' as type FROM {table} WHERE "Squad" = %s;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        if results:
            for row in results:
                send_zulip_notification(row, api_key, stream_name, topic_name)


def send_zulip_notification(row, api_key, stream_name, topic_name):
    message = []
    current_date = datetime.now().strftime("%Y-%m-%d")
    client = zulip.Client(email="apimon-bot@zulip.tsi-dev.otc-service.com", api_key=api_key, site="https://zulip.tsi-vc.otc-service.com")
    if row["type"] == "doc":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[1]
        zone = row[-2]
        commit_url = row[6]
        days_passed = int(row[5])
        if days_passed == 344:
            weeks_to_threshold = 3
            message = f":notifications:    **Outdated Documents Alert**    :notifications:\n\nThis document's last relea" \
                      f"se date will break the **1-year threshold after {weeks_to_threshold} weeks.**\n"
        elif days_passed == 351:
            weeks_to_threshold = 2
            message = f":notifications::notifications:    **Outdated Documents Alert**    :notifications::notifications:" \
                      f"\n\nThis document's last release date will break the **1-year threshold after {weeks_to_threshold} weeks.**"
        elif days_passed == 358:
            weeks_to_threshold = 1
            message = f":notifications::notifications::notifications:   **Outdated Documents Alert**    :notifications::" \
                      f"notifications::notifications:\n\nThis document's last release date will break the **1-year threshold after {weeks_to_threshold} weeks.**"
        elif days_passed >= 365:
            message = ":exclamation:    **Outdated Documents Alert**    :exclamation:\n\nThis document's release date breaks 1-year threshold!"
        else:
            return

        message += f"\n\n**Squad name:** {squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** {current_date}\n\n**Commit" \
                   f" URL:** {commit_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-service.com/d/c67f0f4b-b31c-" \
                   f"4433-b530-a18896470d49/last-docs-commit?orgId=1&var-squad_commit={encoded_squad}&var-doctype_commit=All&var-duration_commit=ASC&var-zone=last_update_commit\n\n---------------------------------------------------------"
    elif row["type"] == "issue":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[2]
        zone = row[-2]
        issue_url = row[5]
        message = f":point_right:      **Unattended Issues Alert**      :point_left:\n\nYou have an issue which has no assignees for more than 7 days\n\n" \
                  f"**Squad name:** {squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** {current_date}\n\n**Issue URL:" \
                  f"** {issue_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-service.com/d/I-YJAuBVk/open-issues" \
                  f"-dashboard?orgId=1&var-squad_issues={encoded_squad}&var-env_issues=All&var-sort_duration=DESC&var-zone=open_issues\n\n---------------------------------------------------------"
    elif row["type"] == "orphan":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[2]
        zone = row[-2]
        zone_table = "open_prs" if zone == "Public" else "open_prs_swiss"
        orphan_url = row[4]
        message = f":boom:    **Orphaned PRs Alert**   :boom:\n\nYou have orphaned PR here!\n\n**Squad name:** {squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** {current_date}\n\n" \
                  f"**Orphan URL:** {orphan_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-service.com/d/4vLGLDB" \
                  f"4z/open-prs-dashboard?orgId=1&var-squad_filter={encoded_squad}&var-env=Github&var-env=Gitea&var-zone={zone_table}\n\n---------------------------------------------------------"
    result = client.send_message({
        "type": "stream",
        "to": stream_name,
        "subject": topic_name,
        "content": message
    })

    if result["result"] == "success":
        logging.info(f"Notification sent successfully for {row[-1]}")
    else:
        logging.error(f"Failed to send notification for {row[-1]}: {result['msg']}")


def main():
    check_env_variables()
    conn = connect_to_db(db_csv)
    conn_orph = connect_to_db(db_orph)

    for squad_name, channel in squad_streams.items():
        stream_name = channel["stream"]
        topic_name = channel["topic"]
        check_orphans(conn_orph, squad_name, stream_name, topic_name)
        check_open_issues(conn, squad_name, stream_name, topic_name)
        check_outdated_docs(conn, squad_name, stream_name, topic_name)

    conn.close()
    conn_orph.close()


if __name__ == "__main__":
    main()
    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info(f"Script executed in {int(minutes)} minutes {int(seconds)} seconds! Let's go drink some beer :)")
