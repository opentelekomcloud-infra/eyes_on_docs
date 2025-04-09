"""
This script sends Zulip messages to corresponding squads via Zulip bot, based on info taken from postgres tables
"""

import logging
import time
from datetime import datetime
from urllib.parse import quote

import zulip
from psycopg2.extras import DictCursor

from config import Database, EnvVariables, Timer, setup_logging

env_vars = EnvVariables()
database = Database(env_vars)

# Zulip stream and topic mapping for each squad
squad_streams = {
    "Dashboard Squad": {"stream": "Dashboard Squad", "topic": "Orphaned PR's"},
    "Database Squad": {"stream": "Database Squad", "topic": "Doc alerts"},
    "Big Data and AI Squad": {"stream": "bigdata & ai", "topic": "helpcenter_alerts"},
    "Compute Squad": {"stream": "compute", "topic": "hc_alerts topic"},
    "Security Services Squad": {"stream": "security services", "topic": "Doc Alerts"},
    "CMS Squad": {"stream": "CMS Squad", "topic": "Doc alerts"},
    "PAAS Squad": {"stream": "PaaS Squad", "topic": "Doc alerts"},
    "Storage Squad": {"stream": "Storage Squad", "topic": "helpcenter_alerts"},
    "Container Squad": {"stream": "Container squad", "topic": "Doc alerts"},
    "Network Squad": {"stream": "network", "topic": "Alerts_HelpCenter"},
    "eco": {"stream": "ecosystem", "topic": "Eyes-on-Docs alerts"}
}

# Rate limiting vars
MESSAGE_LIMIT = 190
message_counter = 0
last_reset_time = time.time()


def check_rate_limit():
    global message_counter, last_reset_time
    current_time = time.time()

    if current_time - last_reset_time >= 60:
        message_counter = 0
        last_reset_time = current_time
        logging.info("Rate limit counter reset")

    if message_counter >= MESSAGE_LIMIT:
        wait_time = 60 - (current_time - last_reset_time)
        if wait_time > 0:
            logging.info(f"Approaching rate limit. Pausing for {wait_time:.2f} seconds")
            time.sleep(wait_time)
            message_counter = 0
            last_reset_time = time.time()


def check_orphans(conn_orph, squad_name, stream_name, topic_name):
    results = []
    cur_orph = conn_orph.cursor(cursor_factory=DictCursor)
    tables = ["open_prs", "open_prs_swiss"]
    for table in tables:
        if table == "open_prs":
            logging.info("Looking for orphaned PRs for %s in %s...", squad_name, table)
            query = f"""SELECT *, 'Public' as zone, 'orphan' as type FROM {table} WHERE "Squad" = '{squad_name}';"""
            cur_orph.execute(query, (squad_name,))
            results = cur_orph.fetchall()
        elif table == "open_prs_swiss":
            logging.info("Looking for orphaned PRs for %s in %s...", squad_name, table)
            query = f"""SELECT *, 'Hybrid' as zone, 'orphan' as type FROM {table} WHERE "Squad" = '{squad_name}';"""
            cur_orph.execute(query, (squad_name,))
            results = cur_orph.fetchall()
        if results:
            for row in results:
                send_zulip_notification(row, env_vars.api_key, stream_name, topic_name)


def check_open_issues(conn, squad_name, stream_name, topic_name):
    results = []
    cur = conn.cursor(cursor_factory=DictCursor)
    tables = ["open_issues", "open_issues_swiss"]
    for table in tables:
        if table == "open_issues":
            logging.info("Checking %s for %s", table, squad_name)
            query = f"""SELECT *, 'Public' as zone, 'issue' as type FROM {table} WHERE "Squad" = '{squad_name}' AND
             "Environment" = 'Github' AND "Assignees" = '' AND "Duration" > '7' ;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        elif table == "open_issues_swiss":
            logging.info("Checking %s for %s", table, squad_name)
            query = f"""SELECT *, 'Hybrid' as zone, 'issue' as type FROM {table} WHERE "Squad" = '{squad_name}' AND
             "Environment" = 'Github' AND "Assignees" = '' AND "Duration" > '7' ;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        if results:
            for row in results:
                send_zulip_notification(row, env_vars.api_key, stream_name, topic_name)


def check_outdated_docs(conn, squad_name, stream_name, topic_name):
    results = []
    cur = conn.cursor(cursor_factory=DictCursor)
    tables = ["last_update_commit", "last_update_commit_swiss"]
    for table in tables:
        if table == "last_update_commit":
            logging.info("Checking %s table for %s...", table, squad_name)
            query = f"""SELECT *, 'Public' as zone, 'doc' as type FROM {table} WHERE "Squad" = %s;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        elif table == "last_update_commit_swiss":
            logging.info("Checking %s table for %s...", table, squad_name)
            query = f"""SELECT *, 'Hybrid' as zone, 'doc' as type FROM {table} WHERE "Squad" = %s;"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        if results:
            for row in results:
                send_zulip_notification(row, env_vars.api_key, stream_name, topic_name)


def check_labels_comments(conn, squad_name, stream_name, topic_name):
    results = []
    cur = conn.cursor(cursor_factory=DictCursor)
    tables = ["huawei_label", "huawei_label_swiss"]
    for table in tables:
        if table == "huawei_label":
            logging.info("Checking %s table for %s...", table, squad_name)
            query = f"""SELECT *, 'Public' as zone, 'analyzed' as type FROM {table} WHERE "Squad" = %s AND (
                    ("Label" = 'Analyzed' AND "Huawei comment" = 'Not commented') OR
                    ("Label" = 'Not labeled' AND "Huawei comment" = 'Commented') OR
                    ("Label" = 'Not labeled' AND "Huawei comment" = 'Not commented'));"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        elif table == "huawei_label_swiss":
            logging.info("Checking %s table for %s...", table, squad_name)
            query = f"""SELECT *, 'Hybrid' as zone, 'analyzed' as type FROM {table} WHERE "Squad" = %s AND (
                    ("Label" = 'Analyzed' AND "Huawei comment" = 'Not commented') OR
                    ("Label" = 'Not labeled' AND "Huawei comment" = 'Commented') OR
                    ("Label" = 'Not labeled' AND "Huawei comment" = 'Not commented'));"""
            cur.execute(query, (squad_name,))
            results = cur.fetchall()
        if results:
            for row in results:
                send_zulip_notification(row, env_vars.api_key, stream_name, topic_name)


def check_rst(conn, squad_name, stream_name, topic_name):
    cur = conn.cursor(cursor_factory=DictCursor)
    tables = ["huawei_to_otc", "huawei_to_otc_swiss"]

    for table in tables:
        zone = "Public" if table == "huawei_to_otc" else "Hybrid"
        logging.info("Checking %s table for %s...", table, squad_name)

        query_rst = f"""SELECT *, '{zone}' as zone, 'rst' as type FROM {table}
                        WHERE "Squad" = %s AND "If .rst" = 'Yes';"""
        cur.execute(query_rst, (squad_name,))
        results_with_rst = cur.fetchall()

        for row in results_with_rst:
            send_zulip_notification(row, env_vars.api_key, stream_name, topic_name)
            logging.info(f"Sent notification to {squad_name} for PR with RST file")

        query_no_rst = f"""SELECT *, '{zone}' as zone, 'rst' as type FROM {table}
                            WHERE "Squad" = %s AND "If .rst" = 'No';"""
        cur.execute(query_no_rst, (squad_name,))
        results_without_rst = cur.fetchall()

        eco_stream = squad_streams["eco"]["stream"]
        eco_topic = squad_streams["eco"]["topic"]
        for row in results_without_rst:
            send_zulip_notification(row, env_vars.api_key, eco_stream, eco_topic)
            logging.info(f"Sent notification to eco for {squad_name} PR without RST file")


def check_files_lines(conn, squad_name, stream_name, topic_name):
    cur = conn.cursor(cursor_factory=DictCursor)
    tables = [
        ("huawei_files_lines", "Public"),
        ("huawei_files_lines_swiss", "Hybrid")
    ]

    for table, zone in tables:
        logging.info("Checking %s table for %s...", table, squad_name)

        query = f"""
            SELECT *, 
                CASE 
                    WHEN "Lines count" < 1000 AND "Days passed" > 5 THEN 5
                    WHEN "Lines count" BETWEEN 1000 AND 5000 AND "Days passed" > 10 THEN 10
                    WHEN "Lines count" > 5000 AND "Days passed" > 15 THEN 15
                END AS days_range,  
                '{zone}' as zone, 'files_lines' as type
            FROM {table} 
            WHERE "Squad" = %s;
        """

        cur.execute(query, (squad_name,))
        results = cur.fetchall()

        if results:
            for row in results:
                send_zulip_notification(row, env_vars.api_key, stream_name, topic_name)


def send_zulip_notification(row, api_key, stream_name, topic_name):
    check_rate_limit()

    message = []
    current_date = datetime.now().strftime("%Y-%m-%d")
    client = zulip.Client(email="eod-bot@zulip.tsi-vc.otc-service.com", api_key=api_key,
                          site="https://zulip.tsi-vc.otc-service.com")
    if row["type"] == "doc":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[1]
        zone = row[-2]
        commit_url = row[6]
        days_passed = int(row[5])
        if days_passed == 344:
            weeks_to_threshold = 3
            message = f":notifications:    **Outdated Documents Alert**    :notifications:\n\nThis document's last " \
                      f"release date will break the **1-year threshold after {weeks_to_threshold} weeks.**\n"
        elif days_passed == 351:
            weeks_to_threshold = 2
            message = f":notifications::notifications:    **Outdated Documents Alert**    " \
                      f":notifications::notifications:\n\nThis document's last release date will break the **1-year " \
                      f"threshold after {weeks_to_threshold} weeks.**"
        elif days_passed == 358:
            weeks_to_threshold = 1
            message = f":notifications::notifications::notifications:   **Outdated Documents Alert**    " \
                      f":notifications::notifications::notifications:\n\nThis document's last release date will " \
                      f"break the **1-year threshold after {weeks_to_threshold} weeks.**"
        elif days_passed >= 365:
            message = ":exclamation:    **Outdated Documents Alert**    :exclamation:\n\nThis document's release " \
                      "date breaks 1-year threshold!"
        else:
            return

        message += f"\n\n**Squad name:** {squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** " \
                   f"{current_date}\n\n**Commit URL:** {commit_url}\n**Dashboard URL:** " \
                   f"https://dashboard.tsi-dev.otc-service.com/d/c67f0f4b-b31c-4433-b530-a18896470d49/last-docs-" \
                   f"commit?orgId=1&var-squad_commit={encoded_squad}&var-doctype_commit=All&var-duration_commit=ASC&" \
                   f"var-zone=last_update_commit\n\n---------------------------------------------------------"
    elif row["type"] == "issue":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[2]
        zone = row[-2]
        issue_url = row[5]
        message = f":point_right:      **Unattended Issues Alert**      :point_left:\n\nYou have an issue which has " \
                  f"no assignees for more than 7 days\n\n**Squad name:** {squad_name}\n**Service name:** " \
                  f"{service_name}\n**Zone:** {zone}\n**Date:** {current_date}\n\n**Issue URL** " \
                  f"{issue_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-service.com/d/I-YJAuBVk/open-issues" \
                  f"-dashboard?orgId=1&var-squad_issues={encoded_squad}&var-env_issues=All&var-sort_duration=DESC&" \
                  f"var-zone=open_issues\n\n---------------------------------------------------------"
    elif row["type"] == "orphan":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[2]
        zone = row[-2]
        zone_table = "open_prs" if zone == "Public" else "open_prs_swiss"
        orphan_url = row[4]
        message = f":boom:    **Orphaned PRs Alert**   :boom:\n\nYou have orphaned PR here!\n\n**Squad name:** " \
                  f"{squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** {current_date}\n\n" \
                  f"**Orphan URL:** {orphan_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-service.com" \
                  f"/d/4vLGLDB4z/open-prs-dashboard?orgId=1&var-squad_filter={encoded_squad}&var-env=Github&" \
                  f"var-env=Gitea&var-zone={zone_table}\n\n---------------------------------------------------------"
    elif row["type"] == "analyzed":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[2]
        zone = row[-2]
        zone_table = "huawei_label" if zone == "Public" else "huawei_label_swiss"
        pr_url = row[4]
        message = f":ghost:   **Huawei PRs Alert**  :ghost:\n\nPlease check label and comments here!\n\n " \
                  f"**Squad name:** {squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** " \
                  f"{current_date}\n\n **PR URL:** {pr_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-service."\
                  f"com/d/cee8f476-5c2a-4b88-b38d-518c34bb3b17/huawei%3a-analyzed-and-labeled?orgId=1&var-squad_filte"\
                  f"r={encoded_squad}&var-zone={zone_table}\n\n--------------------------------------------------------"
    elif row["type"] == "rst":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[2]
        zone = row[-2]
        zone_table = "huawei_to_otc" if zone == "Public" else "huawei_to_otc_swiss"
        pr_url = row[4]
        message = f":ghost:   **Huawei PRs Alert**  :ghost:\n\nPlease check label and comments here!\n\n " \
                  f"**Squad name:** {squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** " \
                  f"{current_date}\n\n **PR URL:** {pr_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-servi " \
                  f"ce.com/d/c80c0d2f-703e-4906-8678-43f5956e65b2/huawei-to-otc%3a-rst?orgId=1&var-squad_filter= " \
                  f"{encoded_squad}&var-zone={zone_table}\n\n---------------------------------------------------------"
    elif row["type"] == "files_lines":
        squad_name = row[3]
        encoded_squad = quote(squad_name)
        service_name = row[2]
        zone = row[-2]
        zone_table = "huawei_files_lines" if zone == "Public" else "huawei_files_lines_swiss"
        pr_url = row[4]
        message = f":holyhandgrenade:   **Reviewing PRs content Alert**  :holyhandgrenade:\n\n Time to check content " \
                  f"in this PR!\n\n " \
                  f"**Squad name:** {squad_name}\n**Service name:** {service_name}\n**Zone:** {zone}\n**Date:** " \
                  f"{current_date}\n\n **PR URL:** {pr_url}\n**Dashboard URL:** https://dashboard.tsi-dev.otc-servic " \
                  f"e.com/d/b04be79a-d0ec-49ff-aeac-a2eba053937c/files-and-lines-content?orgId=1&var-squad_filter= " \
                  f"{encoded_squad}var-zone={zone_table}\n\n---------------------------------------------------------"

    result = client.send_message({
        "type": "stream",
        "to": stream_name,
        "subject": topic_name,
        "content": message
    })

    global message_counter
    message_counter += 1
    # logging.info(f"Message counter: {message_counter}/190")

    if result["result"] == "success":
        logging.info("Notification sent successfully for %s", row[-1])
    else:
        logging.error("Failed to send notification for %s: %s", row[-1], result['msg'])


def main():
    conn = database.connect_to_db(env_vars.db_csv)
    conn_orph = database.connect_to_db(env_vars.db_orph)

    for squad_name, channel in squad_streams.items():
        stream_name = channel["stream"]
        topic_name = channel["topic"]
        check_orphans(conn_orph, squad_name, stream_name, topic_name)
        check_open_issues(conn, squad_name, stream_name, topic_name)
        check_outdated_docs(conn, squad_name, stream_name, topic_name)
        check_labels_comments(conn, squad_name, stream_name, topic_name)
        check_rst(conn, squad_name, stream_name, topic_name)
        check_files_lines(conn, squad_name, stream_name, topic_name)
    conn.close()
    conn_orph.close()


def run():
    timer = Timer()
    timer.start()
    setup_logging()
    logging.info("-------------------------SCHEDULER IS RUNNING-------------------------")
    main()
    timer.stop()


if __name__ == "__main__":
    run()