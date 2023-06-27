import json
import re
import os
import psycopg2
from datetime import datetime
from github import Github
import requests
import time

start_time = time.time()

print("**OPEN ISSUES SCRIPT IS RUNNING**")

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()
session.debug = False

gitea_org = "docs"
github_org = "opentelekomcloud-docs"

gitea_token = os.getenv("GITEA_TOKEN")
github_token = os.getenv("GITHUB_TOKEN")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
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


def connect_to_db():
    print("Connecting to Postgres...")
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
    except psycopg2.Error as e:
        print(f"Connecting to Postgres: an error occurred while trying to connect to the database: {e}")
        return None


def create_open_issues_table(conn, cur):
    try:
        cur.execute(
            '''CREATE TABLE IF NOT EXISTS open_issues (
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
        print(f"Table open_issues has been created successfully")
    except psycopg2.Error as e:
        print(f"Tables creating: an error occurred while trying to create a table open_issues in the database: {e}")


def get_gitea_issues(gitea_token):
    print("Gathering Gitea issues...")
    gitea_issues = []
    page = 1
    while True:
        try:
            repos_resp = requests.get(f"{gitea_api_endpoint}/repos/issues/search?state=open&owner={gitea_org}&page={page}&limit=1000&token={gitea_token}")
            repos_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Gitea issues: an error occurred while trying to get Gitea issues: {e}")
            break

        try:
            issues_dict = json.loads(repos_resp.content.decode())
            for issue in issues_dict:
                gitea_issues.append(issue)
        except json.JSONDecodeError as e:
            print(f"Gitea issues: an error occurred while trying to decode JSON: {e}")
            break

        link_header = repos_resp.headers.get("Link")
        if link_header is None or "rel=\"next\"" not in link_header:
            break
        else:
            page += 1

    return gitea_issues


def get_github_issues(github_token, repo_names):
    print("Gathering Github issues...")
    headers = {"Authorization": f"Bearer {github_token}"}
    github_issues = []
    for repo in repo_names:
        try:
            url = f"https://api.github.com/repos/{github_org}/{repo}/issues"
            params = {"state": "open", "filter": "all"}
            repos_resp = requests.get(url, headers=headers, params=params)
            repos_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Github issues: an error occurred while trying to get Github issues for repo {repo}: {e}")
            continue

        try:
            issues_dict = json.loads(repos_resp.content.decode())
            github_issues.extend(issues_dict)
        except json.JSONDecodeError as e:
            print(f"Github issues: an error occurred while trying to decode JSON: {e}")
            continue

    return github_issues


def get_issues_table(gitea_issues, github_issues, cur, conn):
    print("Posting data to Postgres...")
    try:
        for tea in gitea_issues:
            environment = "Gitea"
            service_name = tea['repository']['name']
            squad = ""
            number = tea['number']
            url = tea['html_url']
            if "pulls" in url:
                continue
            else:
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
            cur.execute('INSERT INTO open_issues ("Environment", "Service Name", "Squad", "Issue Number", "Issue URL", "Created by", "Created at", "Duration", "Comments", "Assignees") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (environment, service_name, squad, number, url, user, created_at, duration_days, comments, assignees))
            conn.commit()
    except Exception as e:
        print(f"Issues table: an error occurred while posting data to Postgres: {e}")
        conn.rollback()

    service_pattern = re.compile(r"(?P<service_name>(?<=\/opentelekomcloud-docs\/)([^\/]+)(?=\/))")
    for hub in github_issues:
        if 'pull_request' in hub:
            continue

        environment = "Github"
        service_match = None
        for gh in service_pattern.finditer(hub['url']):
            service_match = gh
            break
        if service_match is not None:
            service_name = service_match.group("service_name").strip()
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
                cur.execute('INSERT INTO open_issues ("Environment", "Service Name", "Squad", "Issue Number", "Issue URL", "Created by", "Created at", "Duration", "Comments", "Assignees") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                            (environment, service_name, squad, number, url, user, created_at, duration_days, comments, assignees))
                conn.commit()
            except Exception as e:
                print(f"Issues table: an error occurred while posting data to Postgres: {e}")
                conn.rollback()


def update_squad_and_title(conn, cur):
    print("Updating squads and titles...")
    try:
        cur.execute("SELECT * FROM open_issues;")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                """UPDATE open_issues
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Category"
                    FROM repo_title_category AS rtc
                    WHERE open_issues."Service Name" = rtc."Repository"
                    AND open_issues.id = %s;""",
                (row[0],)
            )
            cur.execute(
                """UPDATE open_issues
                    SET "Squad" = 'Other'
                    WHERE open_issues."Service Name" IN ('doc-exports', 'docs_on_docs', 'docsportal')
                    AND open_issues.id = %s;""",
                (row[0],)
            )
            conn.commit()

    except Exception as e:
        print(f"Error updating squad and title: {e}")
        conn.rollback()


def main():
    check_env_variables()
    g = Github(github_token)
    org = g.get_organization("opentelekomcloud-docs")
    repo_names = [repo.name for repo in org.get_repos()]
    print(len(repo_names))

    gitea_issues = get_gitea_issues(gitea_token)
    github_issues = get_github_issues(github_token, repo_names)
    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute(
        f'''DROP TABLE IF EXISTS open_issues'''
    )
    conn.commit()

    create_open_issues_table(conn, cur)
    get_issues_table(gitea_issues, github_issues, cur, conn)
    update_squad_and_title(conn, cur)
    conn.close()

    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    print(f"Script executed in {int(minutes)} minutes {int(seconds)} seconds! Let's go drink some beer :)")


if __name__ == '__main__':
    main()
