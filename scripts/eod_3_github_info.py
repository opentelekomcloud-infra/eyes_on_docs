"""
This script retrieves info about parent PRs on Github
"""

import logging
import re

import requests
from github import Github

from config import Database, EnvVariables, setup_logging, Timer

env_vars = EnvVariables()
database = Database(env_vars)


def extract_pull_links(cur, table_name):
    logging.info("Extracting links...")
    try:
        cur.execute(f'SELECT "Auto PR URL" FROM {table_name};')
        pull_links = [row[0] for row in cur.fetchall()]
        return pull_links
    except Exception as e:
        logging.info("Extracting pull links: an error occurred while extracting pull links from %s: %s",
                     table_name, str(e))
        return []


def get_auto_prs(gh_string, repo_name, access_token, pull_links):
    auto_prs = []
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.github.com/repos/{gh_string}/{repo_name}/pulls"
    params = {"state": "all"}
    try:
        response = requests.get(url, timeout=10, headers=headers, params=params)
        response.raise_for_status()
        for pr in response.json():
            body = pr.get("body")
            if body and any(link in body for link in pull_links):
                auto_prs.append(pr)
    except requests.exceptions.RequestException as e:
        logging.info("Get PRs: an error occurred while trying to get pull requests: %s", e)
    return auto_prs


def add_github_columns(cur, conn, table_name):
    logging.info("Add info to the Postgres (%s)...", table_name)
    try:
        cur.execute(
            f'''
            ALTER TABLE {table_name}
            ADD COLUMN IF NOT EXISTS "Github PR State" VARCHAR(255),
            ADD COLUMN IF NOT EXISTS "Github PR Merged" BOOLEAN;
            '''
        )
        conn.commit()
    except requests.exceptions.RequestException as e:
        logging.info("Add new column: an error occurred while trying to addidng info to the {table_name}: %s", e)


def update_orphaned_prs(org_str, cur, conn, rows, auto_prs, table_name):
    logging.info("Processing orphaned PRs for %s...", org_str)
    for row in rows:
        pr_id, pull_link = row
        gitea_repo_name = re.search(rf"/{org_str}/(.+?)/", pull_link).group(1)
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
            try:
                cur.execute(
                    f'UPDATE {table_name} SET "Github PR State" = %s, "Github PR Merged" = %s WHERE id = %s;',
                    (state, merged, pr_id)
                )
            except Exception as e:
                logging.info("Orphanes: an error occurred while updating orphaned PRs in the %s table: %s",
                             table_name, str(e))

        else:
            continue

    conn.commit()


def main(org, gorg, table_name, token):
    g = Github(token)

    ghorg = g.get_organization(gorg)
    repo_names = [repo.name for repo in ghorg.get_repos()]
    conn_orph = database.connect_to_db(env_vars.db_orph)
    cur_orph = conn_orph.cursor()

    pull_links = extract_pull_links(cur_orph, table_name)

    auto_prs = []
    logging.info("Gathering PRs info...")
    for repo_name in repo_names:
        auto_prs += get_auto_prs(gorg, repo_name, env_vars.github_token, pull_links)

    add_github_columns(cur_orph, conn_orph, table_name)

    cur_orph.execute(f'SELECT id, "Auto PR URL" FROM {table_name};')
    rows = cur_orph.fetchall()

    update_orphaned_prs(org, cur_orph, conn_orph, rows, auto_prs, table_name)

    cur_orph.close()
    conn_orph.close()


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------GITHUB INFO SCRIPT IS RUNNING-------------------------")

    ORG_STRING = "docs"
    GH_ORG_STR = "opentelekomcloud-docs"
    ORPH_TABLE = "open_prs"

    DONE = False
    try:
        main(ORG_STRING, GH_ORG_STR, ORPH_TABLE, env_vars.github_token)
        main(f"{ORG_STRING}-swiss", f"{GH_ORG_STR}-swiss", f"{ORPH_TABLE}_swiss", env_vars.github_token)
        DONE = True
    except Exception as e:
        logging.info(f"Error has been occurred: {e}")
        main(ORG_STRING, GH_ORG_STR, ORPH_TABLE, env_vars.github_fallback_token)
        main(f"{ORG_STRING}-swiss", f"{GH_ORG_STR}-swiss", f"{ORPH_TABLE}_swiss", env_vars.github_fallback_token)
        DONE = True
    if DONE:
        logging.info("Github operations successfully done!")

    timer.stop()


if __name__ == "__main__":
    run()
