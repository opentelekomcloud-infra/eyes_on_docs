"""
This script gathers info about github issues in infra repos, for ecosystem squad
"""

import logging
from datetime import datetime, timedelta

import psycopg2
from github import Github

from config import Database, EnvVariables, Timer, setup_logging

env_vars = EnvVariables()
database = Database(env_vars)

github_token = env_vars.github_token
github_fallback_token = env_vars.github_fallback_token


def create_open_issues_table(conn, cur, table_name):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Repo Name" VARCHAR(255),
            "Issue Number" INT,
            "Issue URL" VARCHAR(255),
            "Created by" VARCHAR(255),
            "Created at" VARCHAR(255),
            "Duration" INT,
            "Comments" INT,
            "Assignees" TEXT
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database \
                        %s: %s", table_name, env_vars.db_csv, e)


def insert_issue_data(conn, cur, table_name, repo, issue):
    assignees = ', '.join(assignee.login for assignee in issue.assignees)
    created_at = issue.created_at.strftime('%Y-%m-%d')
    try:
        cur.execute(
            f"""INSERT INTO {table_name} (
                "Repo Name", "Issue Number",
                "Issue URL", "Created by", "Created at", "Duration", "Comments", "Assignees"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""",
            (
                repo.name,
                issue.number,
                issue.html_url,
                issue.user.login,
                created_at,
                (datetime.now() - issue.created_at).days,
                issue.comments,
                assignees
            )
        )
        conn.commit()
    except psycopg2.Error as e:
        logging.error("Error inserting issue data: %s", e)
        conn.rollback()


def gather_issues(ghorg, conn, cur, table_name):
    logging.info("Gathering issues info...")
    one_year_ago = datetime.now() - timedelta(days=365)
    for repo in ghorg.get_repos():
        if repo.archived or repo.pushed_at < one_year_ago:
            continue
        issues = repo.get_issues(state="open")
        for issue in issues:
            insert_issue_data(conn, cur, table_name, repo, issue)


def main(gorg, table_name, token):
    g = Github(token)
    ghorg = g.get_organization(gorg)
    conn = database.connect_to_db(env_vars.db_csv)
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()

    create_open_issues_table(conn, cur, table_name)
    gather_issues(ghorg, conn, cur, table_name)

    cur.close()
    conn.close()


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------ECOSYSTEM ISSUES SCRIPT IS RUNNING-------------------------")

    GH_ORG_STR = "opentelekomcloud"
    ISSUES_TABLE = "open_issues_eco"

    DONE = False
    try:
        main(GH_ORG_STR, ISSUES_TABLE, github_token)
        DONE = True
    except Exception as e:
        logging.error("Error has been occurred: %s", e)
        main(GH_ORG_STR, ISSUES_TABLE, github_fallback_token)
        DONE = True
    if DONE:
        logging.info("Github operations successfully done!")

    timer.stop()


if __name__ == "__main__":
    run()
