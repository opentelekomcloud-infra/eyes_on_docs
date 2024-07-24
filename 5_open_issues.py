"""
This script gather info regarding open issues in Gitea and Github
"""

import json
import logging
import re
import time
from datetime import datetime

import psycopg2
import requests
from github import Github

from classes import EnvVariables, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()

logging.error("-------------------------OPEN ISSUES SCRIPT IS RUNNING-------------------------")

GITEA_API_ENDPOINT = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)


def create_open_issues_table(conn, cur, table_name):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Environment" VARCHAR(255),
            "Service Name" VARCHAR(255),
            "Squad" VARCHAR(255),
            "Issue Number" INT,
            "Issue URL" VARCHAR(255),
            "Created by" VARCHAR(255),
            "Created at" TIMESTAMP,
            "Duration" INT,
            "Comments" INT,
            "Assignees" TEXT
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the "
                      "database %s: %s", table_name, env_vars.db_csv, e)


def get_gitea_issues(gitea_token, gitea_org):
    logging.info("Gathering Gitea issues for %s...", gitea_org)
    gitea_issues = []
    page = 1
    while True:
        url = (
            f"{GITEA_API_ENDPOINT}/repos/issues/search?state=open&owner={gitea_org}&page={page}"
            f"&limit=1000&type=issues&token={gitea_token}"
        )
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            if not response.content:
                logging.error("Received an empty response from the server.")
                break

            issues_list = response.json()
            if not issues_list:
                logging.info("No more issues returned by the server.")
                break

            gitea_issues.extend(issues_list)

        except requests.exceptions.RequestException as e:
            logging.error(f"Gitea issues: an error occurred while trying to get Gitea issues for {gitea_org}: {e}")
            break

        link_header = response.headers.get("Link")
        if not link_header or "rel=\"next\"" not in link_header:
            break

        page += 1

    return gitea_issues


def get_github_issues(github_token, repo_names, gh_org):
    logging.info("Gathering Github issues for %s..." % gh_org)
    headers = {"Authorization": f"Bearer {github_token}"}
    github_issues = []
    for repo in repo_names:
        try:
            url = f"https://api.github.com/repos/{gh_org}/{repo}/issues"
            params = {"state": "open", "filter": "all"}
            repos_resp = requests.get(url, timeout=10, headers=headers, params=params)
            repos_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error("Github issues: an error occurred while trying to get Github issues for repo %s "
                          "in %s org: %s", repo, gh_org, e)
            continue

        try:
            issues_dict = json.loads(repos_resp.content.decode())
            github_issues.extend(issues_dict)
        except json.JSONDecodeError as e:
            logging.error("Github issues: an error occurred while trying to decode JSON: %s", e)
            continue

    return github_issues


def get_issues_table(gh_org, gitea_issues, github_issues, cur, conn, table_name):
    logging.info("Posting data to Postgres (%s)...", env_vars.db_csv)
    try:
        for tea in gitea_issues:
            environment = "Gitea"
            service_name = tea['repository']['name']
            squad = ""
            number = tea['number']
            url = tea['html_url']
            if "pulls" in url:
                continue
            url = tea['html_url']
            user = tea['user']['full_name']
            if user == "":
                user = "proposalbot"
            else:
                user = tea['user']['full_name']
            created_at = datetime.strptime(tea['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            now = datetime.utcnow()
            duration = now - created_at
            duration_days = duration.days
            comments = tea['comments']
            if 'assignees' in tea and tea['assignees'] is not None:
                assignees = ', '.join([assignee['login'] for assignee in tea['assignees']])
            else:
                assignees = ''
            cur.execute(f'INSERT INTO {table_name} ("Environment", "Service Name", "Squad", "Issue Number", '
                        f'"Issue URL", "Created by", "Created at", "Duration", "Comments", "Assignees") '
                        f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (environment, service_name, squad, number, url, user, created_at, duration_days, comments,
                         assignees))
            conn.commit()
    except Exception as e:
        logging.error("Issues table: an error occurred while posting data to Postgres: %s", e)
        conn.rollback()

    service_pattern = re.compile(rf"(?<={gh_org}/).([^/]+)")
    for hub in github_issues:
        if 'pull_request' in hub:
            continue

        environment = "Github"
        service_match = None
        for gh in service_pattern.finditer(hub['url']):
            service_match = gh
            break
        if service_match is not None:
            service_name = service_match.group(0).strip()
            squad = ""
            number = hub['number']
            url = hub['html_url']
            user = hub['user']['login']
            created_at = datetime.strptime(hub['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            now = datetime.utcnow()
            duration = now - created_at
            duration_days = duration.days
            comments = hub['comments']
            assignees = ', '.join([assignee['login'] for assignee in hub['assignees']])

            try:
                cur.execute(f'INSERT INTO {table_name} ("Environment", "Service Name", "Squad", "Issue Number", '
                            f'"Issue URL", "Created by", "Created at", "Duration", "Comments", "Assignees") '
                            f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                            (environment, service_name, squad, number, url, user, created_at, duration_days, comments,
                             assignees))
                conn.commit()
            except Exception as e:
                logging.error("Issues table: an error occurred while posting data to table {table_name}: %s", e)
                conn.rollback()


def update_squad_and_title(conn, cur, table_name, rtc):
    logging.info("Updating squads and titles in %s...", table_name)
    try:
        cur.execute(f"SELECT * FROM {table_name};")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                f"""UPDATE {table_name}
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Squad"
                    FROM {rtc} AS rtc
                    WHERE {table_name}."Service Name" = rtc."Repository"
                    AND {table_name}.id = %s;""",
                (row[0],)
            )
            cur.execute(
                f"""UPDATE {table_name}
                    SET "Squad" = 'Other'
                    WHERE {table_name}."Service Name" IN ('doc-exports', 'docs_on_docs', 'docsportal')
                    AND {table_name}.id = %s;""",
                (row[0],)
            )
            conn.commit()

    except Exception as e:
        logging.error("Error updating squad and title for table %s: %s", table_name, e)
        conn.rollback()


def main(org, gh_org, table_name, rtc, token):
    g = Github(token)
    github_org = g.get_organization(gh_org)
    repo_names = [repo.name for repo in github_org.get_repos()]
    logging.info("%s repos have been processed", len(repo_names))

    gitea_issues = get_gitea_issues(env_vars.gitea_token, org)
    github_issues = get_github_issues(env_vars.github_token, repo_names, gh_org)
    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    cur_csv.execute(
        f'''DROP TABLE IF EXISTS {table_name}'''
    )
    conn_csv.commit()

    create_open_issues_table(conn_csv, cur_csv, table_name)
    get_issues_table(org, gitea_issues, github_issues, cur_csv, conn_csv, table_name)
    update_squad_and_title(conn_csv, cur_csv, table_name, rtc)
    conn_csv.close()


if __name__ == '__main__':
    ORG_STRING = "docs"
    GH_ORG_STRING = "opentelekomcloud-docs"
    OPEN_TABLE = "open_issues"
    RTC_TABLE = "repo_title_category"

    DONE = False
    try:
        main(ORG_STRING, GH_ORG_STRING, OPEN_TABLE, RTC_TABLE, env_vars.github_token)
        main(f"{ORG_STRING}-swiss", f"{GH_ORG_STRING}-swiss", f"{OPEN_TABLE}_swiss", f"{RTC_TABLE}_swiss",
             env_vars.github_token)
        DONE = True
    except Exception as e:
        logging.error("An error occurred: %s", e)
        main(ORG_STRING, GH_ORG_STRING, OPEN_TABLE, RTC_TABLE, env_vars.github_fallback_token)
        main(f"{ORG_STRING}-swiss", f"{GH_ORG_STRING}-swiss", f"{OPEN_TABLE}_swiss", f"{RTC_TABLE}_swiss",
             env_vars.github_fallback_token)
        DONE = True
    if DONE:
        logging.info("Github operations successfully done!")

    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info("Script executed in %s minutes %s seconds! Let's go drink some beer :)", int(minutes), int(seconds))
