"""
This script provides logic to retrieve info about last date when document was updated
"""

from datetime import datetime
import logging
import os
import shutil
import tempfile
import time
import psycopg2
from github import Github


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()

logging.info("-------------------------LAST COMMIT INFO SCRIPT IS RUNNING-------------------------")

github_token = os.getenv("GITHUB_TOKEN")
github_fallback_token = os.getenv("GITHUB_FALLBACK_TOKEN")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_CSV")  # Here we're using main postgres db since we don't need orphan PRs
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def check_env_variables():
    required_env_vars = [
        "GITHUB_TOKEN", "DB_HOST", "DB_PORT",
        "DB_NAME", "DB_USER", "DB_PASSWORD", "GITEA_TOKEN"
    ]
    for var in required_env_vars:
        if os.getenv(var) is None:
            raise Exception("Missing environment variable: %s", var)


def connect_to_db(db_name):
    logging.info("Connecting to Postgres (%s)...", db_name)
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
    except psycopg2.Error as e:
        logging.error("Connecting to Postgres: an error occurred while trying to connect to the database: %s", e)
        return None


def create_commits_table(conn, cur, table_name):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Service Name" VARCHAR(255),
            "Doc Type" VARCHAR(255),
            "Squad" VARCHAR(255),
            "Last commit at" VARCHAR(255),
            "Days passed" INT,
            "Commit URL" VARCHAR(255)
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      table_name, e)


def get_last_commit_url(github_repo, path):
    logging.debug("%s", path)
    commits = github_repo.get_commits(path=path)
    # logging.debug(f"GITHUB REPO---------------------------------- {github_repo}")
    for commit in commits:
        # logging.debug(f"COMMIT--------------------------------------- {commit}")
        files_changed = commit.files
        if any(file.filename.endswith('.rst') for file in files_changed):
            # logging.debug(f"COMMIT URL AND DATE---------------------------- {commit.html_url} "
            #               f"{commit.commit.author.date}")
            return commit.html_url, commit.commit.author.date  # Return the commit URL and its date
    return None, None


def get_last_commit(org, conn, cur, doctype, string, table_name):
    logging.info("Gathering last commit info for %s...", string)
    exclude_repos = ["docsportal", "doc-exports", "docs_on_docs", ".github", "presentations", "sandbox", "security",
                     "template", "content-delivery-network", "data-admin-service", "resource-template-service"]
    for repo in org.get_repos():

        if repo.name in exclude_repos:
            continue

        tmp_dir = tempfile.mkdtemp()

        try:

            path = doctype
            last_commit_url, last_commit_date = get_last_commit_url(repo, path)
            if last_commit_url and last_commit_date:
                last_commit_url, _ = get_last_commit_url(repo, path)
                formatted_commit_date = last_commit_date.strftime('%Y-%m-%d')
                now = datetime.utcnow()
                duration = now - last_commit_date
                duration_days = duration.days
                if doctype == "umn/source":
                    doc_type = "UMN"
                else:
                    doc_type = "API"
                service_name = repo.name
                cur.execute(
                    f'INSERT INTO {table_name} ("Service Name", "Doc Type", "Last commit at", "Days passed", '
                    f'"Commit URL") VALUES (%s, %s, %s, %s, %s);',
                    (service_name, doc_type, formatted_commit_date, duration_days, last_commit_url,))
                conn.commit()

        except Exception as e:
            logging.error("Last commit: an error occurred while processing repo %s: %s", repo.name, str(e))

        finally:
            shutil.rmtree(tmp_dir)


def update_squad_and_title(conn, cur, table_name, rtc):
    logging.info("Updating squads and titles...")
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
        logging.error("Error updating squad and title: %s", e)
        conn.rollback()


def main(gorg, table_name, rtc, gh_str, token):
    check_env_variables()
    g = Github(token)
    org = g.get_organization(gorg)
    conn = connect_to_db(db_name)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_commits_table(conn, cur, table_name)
    logging.info("Searching for a most recent commit in umn/source...")
    get_last_commit(org, conn, cur, "umn/source", gh_str, table_name)
    logging.info("Searching for a most recent commit in api-ref/source...")
    get_last_commit(org, conn, cur, "api-ref/source", gh_str, table_name)
    update_squad_and_title(conn, cur, table_name, rtc)
    conn.commit()


if __name__ == "__main__":
    GH_ORG_STR = "opentelekomcloud-docs"
    COMMIT_TABLE = "last_update_commit"
    RTC_TABLE = "repo_title_category"

    DONE = False
    try:
        main(GH_ORG_STR, COMMIT_TABLE, RTC_TABLE, GH_ORG_STR, github_token)
        main(f"{GH_ORG_STR}-swiss", f"{COMMIT_TABLE}_swiss", f"{RTC_TABLE}_swiss", f"{GH_ORG_STR}-swiss", github_token)
        DONE = True
    except Exception as e:
        logging.info("Error has been occurred: %s", e)
        main(GH_ORG_STR, COMMIT_TABLE, RTC_TABLE, GH_ORG_STR, github_fallback_token)
        main(f"{GH_ORG_STR}-swiss", f"{COMMIT_TABLE}_swiss", f"{RTC_TABLE}_swiss", f"{GH_ORG_STR}-swiss",
             github_fallback_token)
        DONE = True
    if DONE:
        logging.info("Github operations successfully done!")

    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info("Script executed in %s minutes %s seconds! Let's go drink some beer :)", int(minutes), int(seconds))
