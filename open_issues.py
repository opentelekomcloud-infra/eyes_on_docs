import json
import re
import os
import psycopg2
from datetime import datetime
from github import Github
import requests

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


def connect_to_db():
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )


def create_open_issues_table(conn, cur):
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


def get_gitea_issues(gitea_token):
    gitea_issues = []
    page = 1
    while True:
        repos_resp = requests.get(f"{gitea_api_endpoint}/repos/issues/search?state=open&owner={gitea_org}&page={page}&limit=1000&token={gitea_token}")
        if repos_resp.status_code == 200:
            issues_dict = json.loads(repos_resp.content.decode())
            for issue in issues_dict:
                gitea_issues.append(issue)
            link_header = repos_resp.headers.get("Link")
            if link_header is None or "rel=\"next\"" not in link_header:
                break
            else:
                page += 1
        else:
            break
    return gitea_issues


def get_github_issues(github_token, repo_names):
    headers = {"Authorization": f"Bearer {github_token}"}
    github_issues = []
    for repo in repo_names:
        url = f"https://api.github.com/repos/{github_org}/{repo}/issues"
        params = {"state": "open"}
        repos_resp = requests.get(url, headers=headers, params=params)
        print(repos_resp.status_code)
        if repos_resp.status_code == 200:
            issues_dict = json.loads(repos_resp.content.decode())
            github_issues.extend(issues_dict)
    return github_issues


def get_issues_table(gitea_issues, github_issues, cur, conn):
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

    service_pattern = re.compile(r"(?P<service_name>(?<=\/opentelekomcloud-docs\/)([^\/]+)(?=\/))")
    for hub in github_issues:
        if hub:
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

                cur.execute('INSERT INTO open_issues ("Environment", "Service Name", "Squad", "Issue Number", "Issue URL", "Created by", "Created at", "Duration", "Comments", "Assignees") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                            (environment, service_name, squad, number, url, user, created_at, duration_days, comments, assignees))
                conn.commit()


def update_squad_and_title(conn, cur):
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


def main():
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


if __name__ == '__main__':
    main()
