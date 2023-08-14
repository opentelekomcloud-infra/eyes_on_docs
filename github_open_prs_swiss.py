import os
import requests
import json
import csv
import re
import pathlib
import base64
import psycopg2
from github import Github

github_token = os.getenv("GITHUB_TOKEN")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1/repos/docs/"
gitea_token = os.getenv("GITEA_TOKEN")

session = requests.Session()
session.debug = False


def connect_to_db():
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )


def create_prs_table(conn, cur, table_name):
    cur.execute(
        f'''CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        "Parent PR Number" INT,
        "Service Name" VARCHAR(255),
        "Squad" VARCHAR(255),
        "Auto PR URL" VARCHAR(255),
        "Auto PR State" VARCHAR(255),
        "If merged" BOOLEAN,
        "Environment" VARCHAR(255),
        "Parent PR State" VARCHAR(255),
        "Parent PR merged" BOOLEAN
        );'''
    )
    conn.commit()


def gitea_pr_info(parent_pr_name):
    parent_pr_state = None
    parent_pr_merged = None
    pull_request_resp = session.get(f"{gitea_api_endpoint}{parent_pr_name}?token={gitea_token}")
    if pull_request_resp.status_code == 200:
        parent_info = json.loads(pull_request_resp.content.decode("utf-8"))
        parent_pr_state = parent_info["state"]
        parent_pr_merged = parent_info["merged"]

    print("THIS IS STATE", parent_pr_state, "THIS IS MERGED", parent_pr_merged)
    return parent_pr_state, parent_pr_merged


def get_github_open_prs(org, conn, cur):
    for repo in org.get_repos():
        for pr in repo.get_pulls(state='open'):
            if pr.body is not None and 'This is an automatically created Pull Request for changes to' in pr.body:
                pr_number = pr.number
                name_service = pr.base.repo.name
                squad = ""
                match_gitea = re.search(r"(?P<gitea>(?<=under\s).*(?=.))", pr.body)
                gitea_pr_url = match_gitea.group("gitea")
                auto_pr_state = pr.state
                if pr.merged_at is None:
                    merged = False
                else:
                    merged = True
                env = "Github"
                match_url = re.search(r"(?P<pr>(?<=\/docs\/).*(?<!\.))", pr.body)
                parent_api_name = match_url.group("pr")
                print("THIS IS PARENT API NAME", parent_api_name)
                parent_pr_state, parent_pr_merged = gitea_pr_info(parent_api_name)
                print(pr_number, name_service, squad, gitea_pr_url, auto_pr_state, merged, parent_pr_state, parent_pr_merged)
                cur.execute(
                    """
                    INSERT INTO open_prs ("Parent PR Number", "Service Name", "Squad",  "Auto PR URL", "Auto PR State", "If merged", "Environment", "Parent PR State", "Parent PR merged")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (pr_number, name_service, squad, gitea_pr_url, auto_pr_state, merged, env, parent_pr_state, parent_pr_merged)
                )
                conn.commit()


def update_squad_and_title(conn, cur):
    cur.execute("SELECT * FROM open_prs;")
    open_issues_rows = cur.fetchall()

    for row in open_issues_rows:
        cur.execute(
            """UPDATE open_prs
                SET "Service Name" = rtc."Title", "Squad" = rtc."Category"
                FROM repo_title_category AS rtc
                WHERE open_prs."Service Name" = rtc."Repository"
                AND open_prs.id = %s;""",
            (row[0],)
        )
        cur.execute(
            """UPDATE open_prs
                SET "Squad" = 'Other'
                WHERE open_prs."Service Name" IN ('doc-exports', 'docs_on_docs', 'docsportal')
                AND open_prs.id = %s;""",
            (row[0],)
        )
        conn.commit()


def main():
    g = Github(github_token)
    org = g.get_organization("opentelekomcloud-docs")

    conn = connect_to_db()
    cur = conn.cursor()

    # cur.execute("DROP TABLE IF EXISTS open_prs")
    # conn.commit()
    #
    # create_prs_table(conn, cur, "open_prs")

    get_github_open_prs(org, conn, cur)
    update_squad_and_title(conn, cur)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
