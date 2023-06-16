import os
import shutil
import tempfile
import psycopg2
from git import Repo
from github import Github
from datetime import datetime
import time

start_time = time.time()

print("**LAST COMMIT INFO SCRIPT IS RUNNING**")

github_token = os.getenv("GITHUB_TOKEN")
g = Github(github_token)

org = g.get_organization("opentelekomcloud-docs")

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
        print(f"Table {table_name} has been created successfully")
    except psycopg2.Error as e:
        print(f"Tables creating: an error occurred while trying to create a table {table_name} in the database: {e}")


def get_last_commit_url(github_repo, git_repo, path):
    try:
        last_commit_sha = git_repo.git.log('-1', '--pretty=format:%H', path)
        last_commit_url = f"https://github.com/{github_repo.full_name}/commit/{last_commit_sha}"
        return last_commit_url
    except Github.GithubException.GithubException as e:
        print(f"SHA: an error occurred while getting last commit URL: {e}")

def get_last_commit(cur, conn, doctype):
    print("Gathering last commit info...")
    exclude_repos = ["docsportal", "doc-exports", "docs_on_docs", ".github", "presentations", "sandbox", "security", "template"]
    for repo in org.get_repos():
        if repo.name in exclude_repos:
            continue

        tmp_dir = tempfile.mkdtemp()

        try:
            cloned_repo = Repo.clone_from(repo.clone_url, tmp_dir)

            for path in {doctype}:
                try:
                    last_commit_url = get_last_commit_url(repo, cloned_repo, path)
                    last_commit_str = cloned_repo.git.log('-1', '--pretty=format:%cd', '--date=short', f':(exclude)*conf.py {path}')
                    last_commit = datetime.strptime(last_commit_str, '%Y-%m-%d')  # convert string to datetime
                    now = datetime.utcnow()
                    duration = now - last_commit
                    duration_days = duration.days
                    if doctype == "umn/source":
                        doc_type = "UMN"
                    else:
                        doc_type = "API"
                    service_name = repo.name
                    cur.execute(
                        'INSERT INTO last_update_commit ("Service Name", "Doc Type", "Last commit at", "Days passed", "Commit URL") VALUES (%s, %s, %s, %s, %s);',
                        (service_name, doc_type, last_commit_str, duration_days, last_commit_url,))
                    conn.commit()
                except Exception as e:
                    print(f"Last commit: an error occurred while running git log for path {path}: {str(e)}")

        except Exception as e:
            print(f"Last commit: an error occurred while processing repo {repo.name}: {str(e)}")

        finally:
            shutil.rmtree(tmp_dir)


def update_squad_and_title(conn, cur):
    print("Updating squads and titles...")
    try:
        cur.execute("SELECT * FROM last_update_commit;")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                """UPDATE last_update_commit
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Category"
                    FROM repo_title_category AS rtc
                    WHERE last_update_commit."Service Name" = rtc."Repository"
                    AND last_update_commit.id = %s;""",
                (row[0],)
            )
            cur.execute(
                """UPDATE last_update_commit
                    SET "Squad" = 'Other'
                    WHERE last_update_commit."Service Name" IN ('doc-exports', 'docs_on_docs', 'docsportal')
                    AND last_update_commit.id = %s;""",
                (row[0],)
            )
            conn.commit()

    except Exception as e:
        print(f"Error updating squad and title: {e}")
        conn.rollback()


def main():
    check_env_variables()

    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS last_update_commit")
    create_commits_table(conn, cur, "last_update_commit")
    print(f"Searching for a most recent commit in umn/source...")
    get_last_commit(cur, conn, "umn/source")
    print(f"Searching for a most recent commit in api-ref/source...")
    get_last_commit(cur, conn, "api-ref/source")
    update_squad_and_title(conn, cur)
    conn.commit()
    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    print(f"Script executed in {int(minutes)} minutes {int(seconds)} seconds! Let's go drink some beer :)")


if __name__ == "__main__":
    main()
