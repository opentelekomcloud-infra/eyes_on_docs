"""
This script gathers info regarding PRs, which check jobs in zuul has been failed
"""

import json
import logging
import re
import time
from datetime import datetime

import psycopg2
import requests

from classes import Database, EnvVariables

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()

logging.info("-------------------------FAILED PRS SCRIPT IS RUNNING-------------------------")

GITEA_API_ENDPOINT = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)

github_token = env_vars.github_token
github_fallback_token = env_vars.github_fallback_token


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
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error:
        logging.error(
            "Create table: an error occurred while trying to create a table %s in the database: %s", table_name,
            env_vars.db_zuul)


def is_repo_empty(org, repo, gitea_token):
    try:
        commits_resp = session.get(f"{GITEA_API_ENDPOINT}/repos/{org}/{repo}/commits?token={gitea_token}")
        commits_resp.raise_for_status()

        commits_data = json.loads(commits_resp.content.decode())
        if not commits_data:
            return True
        return False
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:  # Conflict error which might mean empty repo, skip this repo to avoid script\
            # hangs
            logging.info("Repo %s is empty, skipping", repo)
            return True
        logging.error("Check repo: an error occurred while trying to get commits for repo %s: %s", repo, e)
        return False
    except requests.exceptions.RequestException as e:
        logging.error("Check repo: an error occurred while trying to get commits for repo %s: %s", repo, e)
        return False


def get_repos(org, gitea_token):
    logging.info("Gathering repos...")
    repos = []
    page = 1
    while True:
        try:
            repos_resp = session.get(f"{GITEA_API_ENDPOINT}/orgs/{org}/repos?page={page}&limit=50&token={gitea_token}")
            repos_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error("Get repos: an error occurred while trying to get repos: %s", e)
            break

        try:
            repos_dict = json.loads(repos_resp.content.decode())
        except json.JSONDecodeError as e:
            logging.error("Get repos: an error occurred while trying to decode JSON: %s", e)
            break

        for repo in repos_dict:
            if not is_repo_empty(org, repo["name"], gitea_token):  # Skipping empty repos
                repos.append(repo["name"])

        link_header = repos_resp.headers.get("Link")
        if link_header is None or "rel=\"next\"" not in link_header:
            break
        page += 1

    logging.info("%s repos have been processed", len(repos))

    return repos


def extract_number_from_body(text):
    try:
        match = re.search(r"#\d+", str(text))
        if match:
            return int(match.group()[1:])
    except ValueError as e:
        logging.error("Extract number from body: an error occurred while converting match group to int: %s", e)
        return None
    return None


def get_f_pr_commits(org, repo, f_pr_number, gitea_token):
    try:
        zuul_url = None
        status = None
        created_at = None
        days_passed = None

        pull_request_resp = session.get(
            f"{GITEA_API_ENDPOINT}/repos/{org}/{repo}/pulls/{f_pr_number}/commits?token={gitea_token}")
        pull_request_resp.raise_for_status()

        f_pr_info = json.loads(pull_request_resp.content.decode("utf-8"))

        if len(f_pr_info) > 0:
            f_commit_sha = f_pr_info[0]["sha"]
            commit_status_resp = session.get(
                f"{GITEA_API_ENDPOINT}/repos/{org}/{repo}/statuses/{f_commit_sha}?token={gitea_token}")
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
        logging.error(
            "Get failed PR commits: an error occurred while trying to get pull requests of %s repo for %s org: \
            %s", repo, org, e)

    return None, None, None, None


def get_failed_prs(org, repo, gitea_token, conn_zuul, cur_zuul, table_name):
    # logging.info(f"Processing {repo}...")  # Debug print, uncomment in case of script hangs
    try:
        if repo != "doc-exports":
            page = 1
            while True:
                # logging.info(f"Fetching PRs for {org} {repo}, page {page}...")  # Debug, uncomment if script hangs
                repo_resp = session.get(
                    f"{GITEA_API_ENDPOINT}/repos/{org}/{repo}/pulls?state=open&page={page}&token=\
                    {gitea_token}")
                pull_requests = []
                if repo_resp.status_code == 200:
                    try:
                        pull_requests = json.loads(repo_resp.content.decode("utf-8"))
                    except json.JSONDecodeError as e:
                        logging.error("Get parent PR: an error occurred while decoding JSON: %s", e)
                    if not pull_requests:
                        break

                    for pull_req in pull_requests:
                        body = pull_req["body"]
                        if body.startswith("This is an automatically created Pull Request"):
                            if pull_req["merged"] is True:
                                continue
                            f_par_pr_num = extract_number_from_body(body)
                            f_pr_number = pull_req["number"]
                            service_name = repo
                            squad = ""
                            title = pull_req["title"]
                            f_pr_url = pull_req["url"]
                            f_pr_state = pull_req["state"]
                            zuul_url, status, created_at, days_passed = get_f_pr_commits(org, repo, f_pr_number,
                                                                                         gitea_token)
                            try:
                                if all(item is not None for item in [zuul_url, status, created_at, days_passed]):
                                    cur_zuul.execute(f"""
                                        INSERT INTO public.{table_name}
                                        ("Service Name", "Failed PR Title", "Failed PR URL", "Squad", "Failed PR State"\
                                        , "Zuul URL", "Zuul Check Status", "Days Passed",  "Parent PR Number")
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                                     (
                                                         service_name, title, f_pr_url, squad, f_pr_state, zuul_url,
                                                         status,
                                                         days_passed, f_par_pr_num)
                                                     )
                                    conn_zuul.commit()
                            except Exception as e:
                                logging.error(
                                    "Failed PRs: an error occurred while inserting into %s table: %s", table_name, e)
                        else:
                            continue
                elif org in ["docs-swiss", "docs"] and repo_resp.status_code != 200:
                    break
                page += 1
    except Exception as e:
        logging.error('Failed PRs: an error occurred:', e)


def update_squad_and_title(conn_zuul, cur_zuul, rtctable, opentable):
    logging.info("Updating squads and titles in %s...", opentable)
    try:
        cur_zuul.execute(f"SELECT * FROM {opentable};")
        failed_prs_rows = cur_zuul.fetchall()

        for row in failed_prs_rows:
            service_name_index = 1
            id_index = 0

            cur_zuul.execute(
                f"""SELECT "Title", "Squad"
                    FROM {rtctable}
                    WHERE "Repository" = %s;""",
                (row[service_name_index],)
            )
            rtc_row = cur_zuul.fetchone()

            if rtc_row:
                cur_zuul.execute(
                    f"""UPDATE {opentable}
                        SET "Service Name" = %s, "Squad" = %s
                        WHERE id = %s;""",
                    (rtc_row[0], rtc_row[1], row[id_index])
                )

            if row[service_name_index] in ('doc-exports', 'docs_on_docs', 'docsportal'):
                cur_zuul.execute(
                    f"""UPDATE {opentable}
                        SET "Squad" = 'Other'
                        WHERE id = %s;""",
                    (row[id_index],)
                )

            conn_zuul.commit()

    except Exception as e:
        logging.error("Error updating squad and title: %s", e)
        conn_zuul.rollback()


def main(org, table_name, rtc):

    conn_zuul = database.connect_to_db(env_vars.db_zuul)
    cur_zuul = conn_zuul.cursor()

    cur_zuul.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn_zuul.commit()

    create_prs_table(conn_zuul, cur_zuul, table_name)

    repos = get_repos(org, env_vars.gitea_token)

    logging.info("Gathering PRs info...")
    for repo in repos:
        get_failed_prs(org, repo, env_vars.gitea_token, conn_zuul, cur_zuul, table_name)

    update_squad_and_title(conn_zuul, cur_zuul, rtc, FAILED_TABLE)

    cur_zuul.close()
    conn_zuul.close()


if __name__ == "__main__":
    ORG_STRING = "docs"
    FAILED_TABLE = "open_prs"
    RTC_TABLE = "repo_title_category"

    main(ORG_STRING, FAILED_TABLE, RTC_TABLE)
    main(f"{ORG_STRING}-swiss", f"{FAILED_TABLE}_swiss", f"{RTC_TABLE}_swiss")

    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info("Script executed in %s minutes %s seconds! Let's go drink some beer :)", int(minutes), int(seconds))
