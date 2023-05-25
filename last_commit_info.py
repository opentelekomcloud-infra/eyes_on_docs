import os
import shutil
import tempfile
import psycopg2
from git import Repo
from github import Github
from datetime import datetime

github_token = os.getenv("GITHUB_TOKEN")
g = Github(github_token)

org = g.get_organization("opentelekomcloud-docs")

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


def create_commits_table(conn, cur, table_name):
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


def get_last_commit_url(github_repo, git_repo, path):
    last_commit_sha = git_repo.git.log('-1', '--pretty=format:%H', path)
    last_commit_url = f"https://github.com/{github_repo.full_name}/commit/{last_commit_sha}"
    return last_commit_url


def get_last_commit(cur, conn, doctype):
    exclude_repos = ["docsportal", "doc-exports", "docs_on_docs", ".github", "presentations", "sandbox", "security", "template"]
    for repo in org.get_repos():
        if repo.name in exclude_repos:
            continue

        print(f"Processing {repo.name}...")

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
                    print(f"Last commit time in {path}: {last_commit}")
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
                    print(f"Error while running git log for path {path}: {str(e)}")

        except Exception as e:
            print(f"Error while processing repo {repo.name}: {str(e)}")

        finally:
            shutil.rmtree(tmp_dir)


def update_service_names_and_squad(cur, conn):
    cur.execute("SELECT id, \"Service Name\" FROM last_update_commit;")
    last_update_commit_rows = cur.fetchall()

    cur.execute("SELECT \"Repository\", \"Title\", \"Category\" FROM repo_title_category;")
    repo_title_category_rows = cur.fetchall()

    repo_dict = {repo: (title, category) for repo, title, category in repo_title_category_rows}

    for row_id, service_name in last_update_commit_rows:
        if service_name in repo_dict:
            pretty_service_name, squad = repo_dict[service_name]

            cur.execute(
                'UPDATE last_update_commit SET "Service Name" = %s, "Squad" = %s WHERE id = %s;',
                (pretty_service_name, squad, row_id))
            conn.commit()
    print("Update completed.")



def fetch_repo_title_category(cur):
    cur.execute("SELECT * FROM repo_title_category")
    return cur.fetchall()


def main():

    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS last_update_commit")
    create_commits_table(conn, cur, "last_update_commit")
    get_last_commit(cur, conn, "umn/source")
    get_last_commit(cur, conn, "api-ref/source")
    update_service_names_and_squad(cur, conn)
    conn.commit()


if __name__ == "__main__":
    main()
