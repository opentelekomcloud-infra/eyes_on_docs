import os
import psycopg2
from github import Github
import time
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()

logging.info("-------------------------ECOSYSTEM ISSUES SCRIPT IS RUNNING-------------------------")

github_token = os.getenv("GITHUB_TOKEN")
github_fallback_token = os.getenv("GITHUB_FALLBACK_TOKEN")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_CSV")
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
    logging.info(f"Connecting to Postgres ({db_name})...")
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
    except psycopg2.Error as e:
        logging.error(f"Connecting to Postgres: an error occurred while trying to connect to the database: {e}")
        return None


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
        logging.info(f"Table {table_name} has been created successfully")
    except psycopg2.Error as e:
        logging.error(f"Tables creating: an error occurred while trying to create a table {table_name} in the database {db_name}: {e}")


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
        logging.error(f"Error inserting issue data: {e}")
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
    check_env_variables()
    g = Github(token)

    ghorg = g.get_organization(gorg)
    conn = connect_to_db(db_name)
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()

    create_open_issues_table(conn, cur, table_name)
    gather_issues(ghorg, conn, cur, table_name)

    cur.close()
    conn.close()


if __name__ == "__main__":
    gh_org_str = "opentelekomcloud"
    issues_table = "open_issues_eco"

    done = False
    try:
        main(gh_org_str, issues_table, github_token)
        done = True
    except:
        main(gh_org_str, issues_table, github_fallback_token)
        done = True
    if done:
        logging.info("Github operations successfully done!")

    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info(f"Script executed in {int(minutes)} minutes {int(seconds)} seconds! Let's go drink some beer :)")
