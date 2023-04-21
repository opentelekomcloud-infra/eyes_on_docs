import os
import requests
import re
import psycopg2
from github import Github

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


def extract_pull_links(cur):
    cur.execute('SELECT "Auto PR URL" FROM orphaned_prs;')
    pull_links = [row[0] for row in cur.fetchall()]
    return pull_links


def get_auto_prs(repo_name, access_token, pull_links):
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.github.com/repos/opentelekomcloud-docs/{repo_name}/pulls"
    params = {"state": "all"}
    response = requests.get(url, headers=headers, params=params)
    auto_prs = []
    for pr in response.json():
        body = pr.get("body")
        if body and any(link in body for link in pull_links):
            auto_prs.append(pr)
    return auto_prs


def add_github_columns(cur, conn):
    cur.execute(
        '''
        ALTER TABLE orphaned_prs
        ADD COLUMN IF NOT EXISTS "Github PR State" VARCHAR(255),
        ADD COLUMN IF NOT EXISTS "Github PR Merged" BOOLEAN;
        '''
    )
    conn.commit()


def update_orphaned_prs(cur, conn, rows, auto_prs):
    for row in rows:
        pr_id, pull_link = row
        gitea_repo_name = re.search(r"/docs/(.+?)/", pull_link).group(1)

        matching_pr = None
        for pr in auto_prs:
            github_repo_name = pr["base"]["repo"]["name"]
            if gitea_repo_name == github_repo_name:
                matching_pr = pr
                break
        if matching_pr:
            state = matching_pr["state"]
            if matching_pr["merged_at"] is None:
                merged = False
            else:
                merged = True

            cur.execute(
                'UPDATE orphaned_prs SET "Github PR State" = %s, "Github PR Merged" = %s WHERE id = %s;',
                (state, merged, pr_id)
            )
        else:
            continue

    conn.commit()


def main():
    g = Github(github_token)

    org = g.get_organization("opentelekomcloud-docs")
    repo_names = [repo.name for repo in org.get_repos()]

    conn = connect_to_db()
    cur = conn.cursor()

    pull_links = extract_pull_links(cur)

    auto_prs = []
    for repo_name in repo_names:
        auto_prs += get_auto_prs(repo_name, github_token, pull_links)

    add_github_columns(cur, conn)

    cur.execute('SELECT id, "Auto PR URL" FROM orphaned_prs;')
    rows = cur.fetchall()

    update_orphaned_prs(cur, conn, rows, auto_prs)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
