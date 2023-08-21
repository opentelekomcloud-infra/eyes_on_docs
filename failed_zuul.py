import os
import requests
import json
import re
import psycopg2
from datetime import datetime
import time

start_time = time.time()

print("**FAILED PRS SCRIPT IS RUNNING**")

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()
session.debug = False
gitea_token = os.getenv("GITEA_TOKEN")
github_token = os.getenv("GITHUB_TOKEN")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_ZUUL")  # here we're using dedicated postgres db 'zuul' since Failed Zuul PRs panel should be placed on a same dashboard such as Open PRs
db_csv = os.getenv("DB_CSV")  # here rtc service table is located
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def check_env_variables():
    required_env_vars = [
        "GITHUB_TOKEN", "DB_HOST", "DB_PORT",
        "DB_NAME", "DB_USER", "DB_PASSWORD", "GITEA_TOKEN"
    ]
    for var in required_env_vars:
        if os.getenv(var) is None:
            raise Exception(f"Missing environment variable: {var}")


def connect_to_db(db_name):
    print(f"Connecting to Postgres ({db_name})...")
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
    except psycopg2.Error as e:
        print(f"Connecting to Postgres: an error occurred while trying to connect to the database {db_name}: {e}")
        return None


def create_prs_table(conn_zuul, cur_zuul, table_name):
    try:
        cur_zuul.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Service Name" VARCHAR(255),
            "Failed PR Title" VARCHAR(255),
            "Failed PR URL" VARCHAR(255),
            "Squad" VARCHAR(255),
            "Failed PR State" VARCHAR(255),
            "Zuul URL" VARCHAR(255),
            "Zuul Check Status" VARCHAR(255),
            "Created at" VARCHAR(255),
            "Days Passed" INT,
            "Parent PR Number" INT
            );'''
        )
        conn_zuul.commit()
        print(f"Table {table_name} has been created successfully")
    except psycopg2.Error as e:
        print(f"Create table: an error occurred while trying to create a table {table_name} in the database: {e}")


def get_repos(org, gitea_token):
    repos = []
    page = 1
    while True:
        try:
            repos_resp = session.get(f"{gitea_api_endpoint}/orgs/{org}/repos?page={page}&limit=50&token={gitea_token}")
            repos_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Get repos: an error occurred while trying to get repos: {e}")
            break

        try:
            repos_dict = json.loads(repos_resp.content.decode())
        except json.JSONDecodeError as e:
            print(f"Get repos: an error occurred while trying to decode JSON: {e}")
            break

        for repo in repos_dict:
            repos.append(repo["name"])

        link_header = repos_resp.headers.get("Link")
        if link_header is None or "rel=\"next\"" not in link_header:
            break
        else:
            page += 1

    print(len(repos), "repos has been processed")

    return repos


def extract_number_from_body(text):
    try:
        match = re.search(r"#\d+", str(text))
        if match:
            return int(match.group()[1:])
    except ValueError as e:
        print(f"Extract number from body: an error occurred while converting match group to int: {e}")
        return None
    return None


def get_f_pr_commits(org, repo, f_pr_number, gitea_token):
    try:
        zuul_url = None
        status = None
        created_at = None
        days_passed = None

        pull_request_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{f_pr_number}/commits?token={gitea_token}")
        pull_request_resp.raise_for_status()

        f_pr_info = json.loads(pull_request_resp.content.decode("utf-8"))

        if len(f_pr_info) > 0:
            f_commit_sha = f_pr_info[0]["sha"]
            commit_status_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/statuses/{f_commit_sha}?token={gitea_token}")
            commit_status_resp.raise_for_status()

            commit_info = json.loads(commit_status_resp.content.decode("utf-8"))
            commit_status = commit_info[0]["status"]
            if commit_status == "failure":
                status = commit_info[0]["status"]
                zuul_url = commit_info[0]["target_url"]
                created_at = datetime.strptime(commit_info[0]["created_at"], '%Y-%m-%dT%H:%M:%SZ')
                now = datetime.utcnow()
                days_passed = (now - created_at).days

            return zuul_url, status, created_at, days_passed

    except requests.exceptions.RequestException as e:
        print(f"Get failed PR commits: an error occurred while trying to get pull requests of {repo} repo for {org} org: {e}")


def get_failed_prs(org, repo, gitea_token, conn_zuul, cur_zuul, table_name):

    try:
        if repo != "doc-exports" and repo != "dsf":
            page = 1
            while True:
                    repo_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state=open&page={page}&limit=1000&token={gitea_token}")
                    pull_requests = []
                    if repo_resp.status_code == 200:
                        try:
                            pull_requests = json.loads(repo_resp.content.decode("utf-8"))
                        except json.JSONDecodeError as e:
                            print(f"Get parent PR: an error occurred while decoding JSON: {e}")
                        if not pull_requests:
                            break

                        for pull_req in pull_requests:
                            body = pull_req["body"]
                            if body.startswith("This is an automatically created Pull Request"):
                                if pull_req["merged"] is True:
                                    continue
                                else:
                                    f_par_pr_num = extract_number_from_body(body)
                                    f_pr_number = pull_req["number"]
                                    service_name = repo
                                    squad = ""
                                    title = pull_req["title"]
                                    f_pr_url = pull_req["url"]
                                    f_pr_state = pull_req["state"]
                                    zuul_url, status, created_at, days_passed = get_f_pr_commits(org, repo, f_pr_number, gitea_token)
                                try:
                                    if all(item is not None for item in [zuul_url, status, created_at, days_passed]):
                                        cur_zuul.execute(f"""

                                            INSERT INTO public.{table_name}
                                            ("Service Name", "Failed PR Title", "Failed PR URL", "Squad", "Failed PR State", "Zuul URL", "Zuul Check Status", "Days Passed",  "Parent PR Number")
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        """,
                                        (service_name, title, f_pr_url, squad, f_pr_state, zuul_url, status, days_passed, f_par_pr_num)
                                                    )
                                        conn_zuul.commit()
                                except Exception as e:
                                    print(f"Failed PRs: an error occurred while inserting into {table_name} table: {e}")
                            else:
                                continue
                    elif org == "docs-swiss" and repo_resp.status_code != 200:
                        break
                    page += 1

    except Exception as e:
        print('Failed PRs: an error occurred:', e)


def update_squad_and_title(cur_dict, conn_table, cur_table, rtctable, opentable):
    print(f"Updating squads and titles in {opentable}...")
    try:
        cur_table.execute(f"SELECT * FROM {opentable};")
        failed_prs_rows = cur_table.fetchall()

        for row in failed_prs_rows:
            service_name_index = 1
            id_index = 0

            cur_dict.execute(
                f"""SELECT "Title", "Category"
                    FROM {rtctable}
                    WHERE "Repository" = %s;""",
                (row[service_name_index],)
            )
            rtc_row = cur_dict.fetchone()

            if rtc_row:
                cur_table.execute(
                    f"""UPDATE {opentable}
                        SET "Service Name" = %s, "Squad" = %s
                        WHERE id = %s;""",
                    (rtc_row[0], rtc_row[1], row[id_index])
                )

            if row[service_name_index] in ('doc-exports', 'docs_on_docs', 'docsportal'):
                cur_table.execute(
                    f"""UPDATE {opentable}
                        SET "Squad" = 'Other'
                        WHERE id = %s;""",
                    (row[id_index],)
                )

            conn_table.commit()

    except Exception as e:
        print(f"Error updating squad and title: {e}")
        conn_table.rollback()


def main(org, table_name, rtc):
    check_env_variables()

    conn_zuul = connect_to_db(db_name)
    cur_zuul = conn_zuul.cursor()
    conn_csv = connect_to_db(db_csv)
    cur_csv = conn_csv.cursor()

    cur_csv.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn_csv.commit()

    create_prs_table(conn_zuul, cur_zuul, table_name)

    repos = get_repos(org, gitea_token)

    print("Gathering PRs info...")
    for repo in repos:
        get_failed_prs(org, repo, gitea_token, conn_zuul, cur_zuul, table_name)

    update_squad_and_title(cur_csv, conn_zuul, cur_zuul, rtc, table_name)

    cur_csv.close()
    conn_csv.close()
    cur_zuul.close()
    conn_zuul.close()


if __name__ == "__main__":
    org_string = "docs"
    failed_table = "open_prs"
    rtc_table = "repo_title_category"

    main(org_string, failed_table, rtc_table)
    main(f"{org_string}-swiss", f"{failed_table}_swiss", f"{rtc_table}_swiss")


if __name__ == "__main__":
    org_string = "docs"
    failed_table = "failed_zuul_prs"
    rtc_table = "repo_title_category"

    main(org_string, failed_table, rtc_table)
    main(f"{org_string}-swiss", f"{failed_table}_swiss", f"{rtc_table}_swiss")


    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    print(f"Script failed_zuul.py executed in {int(minutes)} minutes {int(seconds)} seconds! Let's go drink some beer :)")

