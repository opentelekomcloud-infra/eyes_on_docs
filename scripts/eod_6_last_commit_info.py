"""
This script provides logic to retrieve info about last date when document was updated
"""

import logging
import shutil
import tempfile
from datetime import datetime

import psycopg2
from github import Github
from github.GithubException import GithubException

from config import Database, EnvVariables, Timer, setup_logging

env_vars = EnvVariables()
database = Database(env_vars)


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


def get_last_commit(org, conn, cur, doctype, string, table_name, rtc):
    logging.info("Gathering last commit info for %s...", string)

    try:
        cur.execute(f"SELECT DISTINCT \"Repository\" FROM {rtc} WHERE \"Env\" NOT IN ('public');")
        exclude_repos = [row[0] for row in cur.fetchall()]
    except Exception as e:
        logging.error("Fetching public repos: %s", e)
        return

    for repo in org.get_repos():
        if repo.name in exclude_repos:
            continue

        tmp_dir = tempfile.mkdtemp()

        try:
            path = doctype
            last_commit_url, last_commit_date = get_last_commit_url(repo, path)
            if not last_commit_url or not last_commit_date:
                logging.info("No commits found for %s, skipping.", repo.name)
                continue

            formatted_commit_date = last_commit_date.strftime('%Y-%m-%d')
            now = datetime.utcnow()
            duration_days = (now - last_commit_date).days

            doc_type = "UMN" if doctype == "umn/source" else "API"
            service_name = repo.name

            cur.execute(
                f'INSERT INTO {table_name} ("Service Name", "Doc Type", "Last commit at", "Days passed", "Commit URL") '
                f'VALUES (%s, %s, %s, %s, %s);',
                (service_name, doc_type, formatted_commit_date, duration_days, last_commit_url)
            )

            conn.commit()

        except GithubException as e:
            if e.status == 409:
                logging.warning("Empty repo, skipping: %s", repo.name)
            else:
                logging.error("Last commit: an error occurred while processing repo %s: %s", repo.name, str(e))

        except Exception as e:
            logging.error("Unexpected error processing repo %s: %s", repo.name, str(e))

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


def delete_non_public_repos(conn, cur, table_name):
    cur.execute(
        f'DELETE FROM {table_name} WHERE "Squad" IS NULL;'
    )

    conn.commit()


def main(gorg, table_name, rtc, gh_str, token):
    g = Github(token)
    org = g.get_organization(gorg)
    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()
    cur_csv.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_commits_table(conn_csv, cur_csv, table_name)
    logging.info("Searching for a most recent commit in umn/source...")
    get_last_commit(org, conn_csv, cur_csv, "umn/source", gh_str, table_name, rtc)
    logging.info("Searching for a most recent commit in api-ref/source...")
    get_last_commit(org, conn_csv, cur_csv, "api-ref/source", gh_str, table_name, rtc)
    update_squad_and_title(conn_csv, cur_csv, table_name, rtc)
    delete_non_public_repos(conn_csv, cur_csv, table_name)
    conn_csv.commit()


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------LAST COMMIT INFO SCRIPT IS RUNNING-------------------------")

    GH_ORG_STR = "opentelekomcloud-docs"
    COMMIT_TABLE = "last_update_commit"
    RTC_TABLE = "repo_title_category"

    done = False
    try:
        main(GH_ORG_STR, COMMIT_TABLE, RTC_TABLE, GH_ORG_STR, env_vars.github_token)
        main(f"{GH_ORG_STR}-swiss", f"{COMMIT_TABLE}_swiss", f"{RTC_TABLE}_swiss", f"{GH_ORG_STR}"
                                                                                   f"-swiss", env_vars.github_token)
        done = True
    except Exception as e:
        logging.info("Error has been occurred: %s", e)
        main(GH_ORG_STR, COMMIT_TABLE, RTC_TABLE, GH_ORG_STR, env_vars.github_fallback_token)
        main(f"{GH_ORG_STR}-swiss", f"{COMMIT_TABLE}_swiss", f"{RTC_TABLE}_swiss", f"{GH_ORG_STR}"
                                                                            f"-swiss", env_vars.github_fallback_token)
        done = True
    if done:
        logging.info("Github operations successfully done!")

    timer.stop()


if __name__ == "__main__":
    run()
