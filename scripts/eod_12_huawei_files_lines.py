"""
This script counts files and lines of code in PRs from Huawei to OTC
"""

import logging
import os
from datetime import datetime

import psycopg2
import requests

from config import Database, EnvVariables, Timer, setup_logging

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)

TEXT_EXTENSIONS = {".py", ".js", ".ts", ".json", ".yaml", ".yml", ".md", ".rst", ".txt", ".sh", ".ini", ".conf"}
LABELS = {"on hold", "new_service", "broken_pr_huawei", "broken_pr_eco"}


def create_prs_table(conn, cur, table_name):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "PR Number" INT,
            "Service Name" VARCHAR(255),
            "Squad" VARCHAR(255),
            "PR URL" VARCHAR(255),
            "Days passed" INT,
            "Files count" INT,
            "Lines count" INT
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      table_name, e)


def create_temp_table(conn, cur, temp_tab):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {temp_tab} (
            id SERIAL PRIMARY KEY,
            repo VARCHAR(255),
            pr_number INT,
            file_url TEXT,
            lines_count INT
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", temp_tab)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      temp_tab, e)


def get_repos(cur, rtc):
    repos = []
    try:
        cur.execute(f"SELECT DISTINCT \"Repository\" FROM {rtc} WHERE \"Env\" IN ('public');")
        repos = [row[0] for row in cur.fetchall()]
        if not repos:
            logging.info("No repositories found.")
    except Exception as e:
        logging.error("Fetching repos: %s", e)

    return repos


def convert_iso_to_datetime(iso_str):
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))


def gather_prs(org, repos, conn, cur, fil_lin_tab):
    headers = {"Authorization": f"token {env_vars.gitea_token}"}
    all_prs = []

    for repo in repos:
        try:
            prs_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state=open&page=1",
                                   headers=headers)
            prs_resp.raise_for_status()
            prs_data = prs_resp.json()
            for pr in prs_data:
                body = pr.get("body", "")
                pr_number = pr.get("number")
                pr_url = pr.get("url")
                files_count = pr.get("changed_files", [])
                pr_labels = pr.get("labels", [])

                # print(f"Checking PR #{pr_number} labels: {pr_labels}")

                label_names = {label["name"] for label in pr_labels}
                if LABELS & label_names:
                    # print(f"PR #{pr_number} has labels: {label_names}")
                    continue
                created_at = convert_iso_to_datetime(pr.get("created_at")).date()
                days_passed = (datetime.utcnow().date() - created_at).days

                if body.startswith("This is an automatically created Pull Request"):
                    all_prs.append({"number": pr_number, "repo": repo})

                    cur.execute(f"""
                        INSERT INTO {fil_lin_tab} ("PR Number", "Service Name", "PR URL", "Days passed", "Files count",
                        "Lines count")
                        VALUES (%s, %s, %s, %s, %s, 0);
                    """, (pr_number, repo, pr_url, days_passed, files_count))

        except requests.exceptions.RequestException as e:
            logging.error("Error fetching PRs: %s", e)

    return all_prs


def get_pr_files(org, prs):
    headers = {"Authorization": f"token {env_vars.gitea_token}"}
    all_files = []

    for pr in prs:
        repo = pr["repo"]
        pr_number = pr["number"]
        page = 1

        while True:
            try:
                response = session.get(
                    f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/files?page={page}",
                    headers=headers
                )
                response.raise_for_status()
                files_data = response.json()

                for file in files_data:
                    if file["status"] == "deleted":
                        continue
                    file_url = file["raw_url"]
                    _, ext = os.path.splitext(file_url)

                    if ext.lower() in TEXT_EXTENSIONS:
                        all_files.append({
                            "repo": repo,
                            "pr_number": pr_number,
                            "file_url": file_url,
                            "lines_count": 0
                        })
                    else:
                        all_files.append({
                            "repo": repo,
                            "pr_number": pr_number,
                            "file_url": file_url,
                            "lines_count": 1
                        })
                link_header = response.headers.get("Link")
                if link_header is None or 'rel="next"' not in link_header:
                    # print(f"Repo {repo}, PR {pr_number}: NO NEXT PAGE.")
                    break

                page += 1
                # print(f"PR {pr_number}: Fetching page {page}")

            except requests.exceptions.RequestException as e:
                logging.error("Error fetching PR files for PR %s: %s", pr_number, e)
                break

    return all_files


def save_files_to_temp(conn, cur, files, temp_tab):
    for file in files:
        cur.execute(f"""
            INSERT INTO {temp_tab} (repo, pr_number, file_url, lines_count)
            VALUES (%s, %s, %s, %s);
        """, (file["repo"], file["pr_number"], file["file_url"], file["lines_count"]))
    conn.commit()


def count_lines_in_file(file_url):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }
    try:
        response = requests.get(file_url, headers=headers)
        response.raise_for_status()
        return len(response.text.splitlines())
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {file_url}: {e}")
        return 0


def process_pr_files(org, prs, temp_tab):
    conn = database.connect_to_db(env_vars.db_csv)
    cur = conn.cursor()

    files = get_pr_files(org, prs)

    for file in files:
        if file["lines_count"] == 1:
            continue
        else:
            file["lines_count"] = count_lines_in_file(file["file_url"])

    save_files_to_temp(conn, cur, files, temp_tab)

    cur.close()
    conn.close()


def aggregate_lines_count(conn, cur, temp_tab, fil_lin_tab):
    cur.execute(f"""
        UPDATE {fil_lin_tab} AS main
        SET "Lines count" = subquery.total_lines
        FROM (
            SELECT pr_number, repo, SUM(lines_count) AS total_lines
            FROM {temp_tab}
            GROUP BY pr_number, repo
        ) AS subquery
        WHERE main."PR Number" = subquery.pr_number
          AND main."Service Name" = subquery.repo;
    """)
    conn.commit()
    logging.info("Updated line counts in PR table")


def update_squad_and_title(conn, cur, rtc, fil_lin_tab):
    logging.info("Updating squads and titles...")
    try:
        cur.execute(f"SELECT * FROM {fil_lin_tab};")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                f"""UPDATE {fil_lin_tab}
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Squad"
                    FROM {rtc} AS rtc
                    WHERE {fil_lin_tab}."Service Name" = rtc."Repository"
                    AND {fil_lin_tab}.id = %s;""",
                (row[0],)
            )
        conn.commit()

    except Exception as e:
        logging.error("Error updating squad and title: %s", e)


def main(org, rtc, fil_lin_tab, temp_tab):
    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()
    cur_csv.execute(f"DROP TABLE IF EXISTS {fil_lin_tab}")
    cur_csv.execute(f"DROP TABLE IF EXISTS {temp_tab}")

    conn_csv.commit()
    create_temp_table(conn_csv, cur_csv, temp_tab)
    create_prs_table(conn_csv, cur_csv, fil_lin_tab)

    repos = get_repos(cur_csv, rtc)
    logging.info("Gathering all child PRs files and lines info...")

    all_prs = gather_prs(org, repos, conn_csv, cur_csv, fil_lin_tab)
    process_pr_files(org, all_prs, temp_tab)
    aggregate_lines_count(conn_csv, cur_csv, temp_tab, fil_lin_tab)
    update_squad_and_title(conn_csv, cur_csv, rtc, fil_lin_tab)


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------HUAWEI FILES AND LINES SCRIPT IS RUNNING-------------------------")

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    org_string = "docs"
    rtc_table = "repo_title_category"
    files_lines_table = "huawei_files_lines"
    temp_table = "temp_huawei_files_lines"

    done = False

    conn_csv.commit()

    main(org_string, rtc_table, files_lines_table, temp_table)
    main(f"{org_string}-swiss", f"{rtc_table}_swiss", f"{files_lines_table}_swiss",
         f"{temp_table}_swiss")

    if done:
        logging.info("Search successfully finish!")

    cur_csv.close()
    conn_csv.close()

    timer.stop()


if __name__ == "__main__":
    run()
