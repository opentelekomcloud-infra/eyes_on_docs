import json
import logging
from datetime import datetime

import psycopg2
import requests

from config import Database, EnvVariables, Timer, setup_logging

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)


def create_prs_table(conn, cur, table_name):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "PR Number" VARCHAR(255),
            "Service Name" VARCHAR(255),
            "Squad" VARCHAR(255),
            "PR URL" VARCHAR(255),
            "Days passed" INT,
            "If .rst" VARCHAR
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      table_name, e)


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


def gather_prs(org, repos):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }
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
                requested_reviewers = pr.get("requested_reviewers", [])

                if body.startswith("This is an automatically created Pull Request"):
                    if requested_reviewers:
                        print(f"Skipping PR #{pr_number} - has reviewers")
                        continue
                    else:
                        created_at_str = pr.get("created_at")
                        created_at = convert_iso_to_datetime(created_at_str).date()
                        current_date = datetime.utcnow().date()
                        days_passed = (current_date - created_at).days

                        if days_passed > 3:
                            all_prs.append({
                                "number": pr_number,
                                "repo": repo,
                                "url": pr_url,
                                "days_passed": days_passed
                            })

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.info("No PRs found in repo %s (404 error). Skipping.", repo)
        except requests.exceptions.RequestException as e:
            logging.error("Error occurred:", e)
        except json.JSONDecodeError as e:
            logging.error("Error occurred while trying to decode JSON: %s", e)

    return all_prs


def check_rst(org, prs):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }
    if_rst = []
    for pr in prs:
        repo = pr.get("repo")
        pr_number = pr.get("number")
        pr_url = pr.get("url")
        days_passed = pr.get("days_passed")
        page = 1

        while True:
            try:
                rst_rsp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/files?page={page}",
                                       headers=headers)
                rst_rsp.raise_for_status()
                rst_data = rst_rsp.json()
                has_rst = any(file["filename"].endswith(".rst") for file in rst_data)

                if has_rst:
                    print(f"PR #{pr_number} has .rst files")
                    if_rst.append({
                        "number": pr_number,
                        "repo": repo,
                        "url": pr_url,
                        "days_passed": days_passed,
                        "if_rst": "Yes"
                    })
                else:
                    print(f"PR #{pr_number} HAS NOT .rst files")
                    if_rst.append({
                        "number": pr_number,
                        "repo": repo,
                        "url": pr_url,
                        "days_passed": days_passed,
                        "if_rst": "No"
                    })

                link_header = rst_rsp.headers.get("Link")
                if link_header is None or 'rel="next"' not in link_header:
                    # print(f"Repo {repo}, PR {pr_number}: NO NEXT PAGE.")
                    break

                page += 1
                # print(f"PR {pr_number}: Fetching page {page}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logging.info("No PRs found in repo %s (404 error). Skipping.", repo)
            except requests.exceptions.RequestException as e:
                logging.error("Error occurred:", e)
            except json.JSONDecodeError as e:
                logging.error("Error occurred while trying to decode JSON: %s", e)
    return if_rst


def insert_data_postgres(prs_tab, pr, conn, cur):
    pr_number = pr.get("number")
    repo = pr.get("repo")
    pr_url = pr.get("url")
    days_passed = pr.get("days_passed")
    if_rst = pr.get("if_rst")
    cur.execute(f"""
        INSERT INTO {prs_tab} ("PR Number", "Service Name", "Squad", "PR URL", "Days passed", "If .rst")
        VALUES (%s, %s, %s, %s, %s, %s);
        """, (pr_number, repo, '', pr_url, days_passed, if_rst))
    conn.commit()


def update_squad_and_title(conn, cur, rtc, prs_tab):
    logging.info("Updating squads and titles...")
    try:
        cur.execute(f"SELECT * FROM {prs_tab};")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                f"""UPDATE {prs_tab}
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Squad"
                    FROM {rtc} AS rtc
                    WHERE {prs_tab}."Service Name" = rtc."Repository"
                    AND {prs_tab}.id = %s;""",
                (row[0],)
            )
        conn.commit()

    except Exception as e:
        logging.error("Error updating squad and title: %s", e)


def main(org, rtc, prs_tab):
    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()
    cur_csv.execute(f"DROP TABLE IF EXISTS {prs_tab}")
    conn_csv.commit()

    create_prs_table(conn_csv, cur_csv, prs_tab)

    repos = get_repos(cur_csv, rtc)
    logging.info("Gathering all child PRs...")

    all_prs = gather_prs(org, repos)
    if_rst = check_rst(org, all_prs)

    for pr in if_rst:
        insert_data_postgres(prs_tab, pr, conn_csv, cur_csv)
    update_squad_and_title(conn_csv, cur_csv, rtc, prs_tab)


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------HUAWEI TO OTC SCRIPT IS RUNNING-------------------------")

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    org_string = "docs"
    rtc_table = "repo_title_category"
    prs_table = "huawei_to_otc"

    done = False

    conn_csv.commit()

    main(org_string, rtc_table, prs_table)
    main(f"{org_string}-swiss", f"{rtc_table}_swiss", f"{prs_table}_swiss")

    if done:
        logging.info("Search successfully finish!")

    cur_csv.close()
    conn_csv.close()

    timer.stop()


if __name__ == "__main__":
    run()