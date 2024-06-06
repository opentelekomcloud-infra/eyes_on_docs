"""
This script gather and process info about dependent PRs, its parents and store it in postgres table
"""

import csv
import json
import logging
import os
import pathlib
import re
import time

import psycopg2
import requests
from github import Github

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_time = time.time()

logging.info("-------------------------OPEN PRs SCRIPT IS RUNNING-------------------------")

GITEA_API_ENDPOINT = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()
gitea_token = os.getenv("GITEA_TOKEN")
github_token = os.getenv("GITHUB_TOKEN")
github_fallback_token = os.getenv("GITHUB_FALLBACK_TOKEN")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_csv = os.getenv("DB_CSV")  # main postgres db, where open PRs tables for both public and hybrid clouds are stored
db_orph = os.getenv("DB_ORPH")  # dedicated db for orphans PRs (for both clouds) tables
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def check_env_variables():
    required_env_vars = [
        "GITHUB_TOKEN", "DB_HOST", "DB_PORT",
        "DB_CSV", "DB_ORPH", "DB_USER", "DB_PASSWORD", "GITEA_TOKEN"
    ]
    for var in required_env_vars:
        if os.getenv(var) is None:
            raise Exception(f"Missing environment variable: {var}")


def csv_erase(filenames):
    try:
        for filename in filenames:
            file_path = pathlib.Path(filename)
            if file_path.exists():
                file_path.unlink()
                logging.info("CSV %s has been deleted", filename)
            else:
                continue
    except Exception as e:
        logging.error("CSV erase: error has been occured: %s", e)


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


def create_prs_table(conn_csv, cur_csv, table_name):
    try:
        cur_csv.execute(
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
        conn_csv.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      table_name, e)


def get_repos(org, cur_csv, gitea_token, rtc_table):
    repos = []

    try:
        cur_csv.execute(f"SELECT DISTINCT \"Title\" FROM {rtc_table} WHERE \"Env\" IN ('internal', 'hidden');")
        exclude_repos = [row[0] for row in cur_csv.fetchall()]
    except Exception as e:
        logging.error("Fetching exclude repos for internal services: %s", e)
        return repos

    page = 1

    while True:
        try:
            repos_resp = session.get(f"{GITEA_API_ENDPOINT}/orgs/{org}/repos?page={page}&limit=50&token={gitea_token}")
            repos_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error("Get repos: an error occurred while trying to get repos: %s", e)
            break

        try:
            repos_dict = json.loads(repos_resp.content.decode())
        except json.JSONDecodeError as e:
            logging.error("JSON decode: an error occurred while trying to decode JSON: %s", e)
            break

        for repo in repos_dict:
            if repo["archived"] or repo["name"] in exclude_repos:
                continue
            repos.append(repo["name"])

        link_header = repos_resp.headers.get("Link")
        if link_header is None or "rel=\"next\"" not in link_header:
            break
        page += 1

    logging.info("%s repos have been processed", len(repos))
    return repos


def check_pull_requests_exist(org, repo):
    try:
        initial_resp = session.get(f"{GITEA_API_ENDPOINT}/repos/{org}/{repo}/pulls?state=all&limit=1&token="
                                   f"{gitea_token}")
        initial_resp.raise_for_status()
        pulls = json.loads(initial_resp.content.decode())
        if not pulls:
            logging.info("No pull requests found in %s. Skipping.", repo)
            return False
        return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.info("No repository or pull requests found in %s (404 error). Skipping.", repo)
            return False
        logging.error("Error checking pull requests in %s: %s", repo, e)
        return False
    except requests.exceptions.RequestException as e:
        logging.error("Unexpected error when checking pull requests in %s: %s", repo, e)
        return False


def get_parent_pr(org, repo):
    if not check_pull_requests_exist(org, repo):
        return
    try:
        path = pathlib.Path("proposalbot_prs.csv")
        if path.exists() is False:
            with open("proposalbot_prs.csv", "w", encoding="utf-8") as csv_2:
                csv_writer = csv.writer(csv_2)
                csv_writer.writerow(["Parent PR number", "Service Name", "Auto PR URL", "Auto PR State", "If merged",
                                     "Environment"])
        else:
            with open("proposalbot_prs.csv", "a", encoding="utf-8") as csv_2:
                csv_writer = csv.writer(csv_2)

                if repo not in {'doc-exports', 'dsf'}:
                    page = 1
                    while True:
                        try:
                            repo_resp = session.get(f"{GITEA_API_ENDPOINT}/repos/{org}/{repo}/pulls?state=all&page={page}"
                                                    f"&limit=1000&token={gitea_token}")
                            repo_resp.raise_for_status()
                        except requests.exceptions.RequestException as e:
                            logging.error("Error occurred while trying to get repo pull requests: %s", e)
                            break

                        try:
                            pull_request = json.loads(repo_resp.content.decode())
                        except json.JSONDecodeError as e:
                            logging.error("Error occurred while trying to decode JSON: %s", e)
                            break

                        dependency_pull_requests = []

                        for pr in pull_request:
                            dependency_pull_requests.append(pr)

                        for pull_req in dependency_pull_requests:
                            body = pull_req["body"]
                            if body.startswith("This is an automatically created Pull Request"):
                                if pull_req["state"] == "closed" and pull_req["merged"] is False:
                                    continue
                                parent_pr = extract_number_from_body(body)
                                service = repo
                                auto_url = pull_req["url"]
                                auto_state = pull_req["state"]
                                if_merged = pull_req["merged"]
                                env = "Gitea"
                                try:
                                    csv_writer.writerow([parent_pr, service, auto_url, auto_state, if_merged, env])
                                except csv.Error as e:
                                    logging.error("Error occurred while trying to write to CSV file: %s", e)
                                    break
                        link_header = repo_resp.headers.get("Link")
                        if link_header is None or "rel=\"next\"" not in link_header:
                            break
                        page += 1

    except IOError as e:
        logging.error("Proposalbot_prs.csv: an error occurred while trying to open or write to CSV file: %s", e)
        return

    try:
        csv_2.close()

    except IOError as e:
        logging.error("Error occurred while trying to close CSV file: %s", e)


def extract_number_from_body(text):
    try:
        match = re.search(r"#\d+", str(text))
        if match:
            return int(match.group()[1:])
    except ValueError as e:
        logging.error("An error occurred while converting match group to int: %s", e)
        return None
    except re.error as e:
        logging.error("An error occurred while searching text: %s", e)
        return None
    return None


def get_pull_requests(org, repo):
    logging.info("Gathering Gitea's child PRs...")
    states = ["open", "closed"]
    pull_requests = []
    try:
        csv_file = open("doc_exports_prs.csv", "a", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Parent PR index", "Parent PR title", "Parent PR URL", "Parent PR state", "If merged"])
    except IOError as e:
        logging.error("Child PRs: an error occurred while opening or writing to the CSV file: %s", e)
        return pull_requests

    for state in states:
        page = 1
        while True:
            try:
                pull_requests_resp = session.get(f"{GITEA_API_ENDPOINT}/repos/{org}/{repo}/pulls?state={state}&page="
                                                 f"{page}&limit=50&token={gitea_token}")
                pull_requests_resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error("Child PRs: an error occurred while trying to get pull requests of %s repo: %s",
                              repo, e)
                break

            try:
                pull_requests = json.loads(pull_requests_resp.content.decode("utf-8"))
            except json.JSONDecodeError as e:
                logging.error("Child PRs: an error occurred while trying to decode JSON: %s", e)
                break

            for pr in pull_requests:
                index = pr["number"]
                title = pr["title"]
                url = pr["url"]
                state = pr["state"]
                if_merged = pr["merged"]
                try:
                    csv_writer.writerow([index, title, url, state, if_merged])
                except csv.Error as e:
                    logging.error("Child PRs: an error occurred while trying to write to CSV file: %s", e)
                    break

            link_header = pull_requests_resp.headers.get("Link")
            if link_header is None or "rel=\"next\"" not in link_header:
                break
            page += 1
    try:
        csv_file.close()
    except IOError as e:
        logging.error("Child PRs: n error occurred while trying to close CSV file: %s", e)

    return pull_requests


def fetch_repo_title_category(cur_csv, rtctable):
    logging.info("Fetching RTC table %s...", rtctable)
    try:
        cur_csv.execute(f"SELECT * FROM {rtctable}")
        return cur_csv.fetchall()
    except Exception as e:
        logging.error("Fetch: an error occurred while trying to fetch data from the table: %s", e)
        return None


def update_service_titles(cur_csv, rtctable):
    logging.info("Updating service titles using %s..", rtctable)
    try:
        repo_title_category = fetch_repo_title_category(cur_csv, rtctable)
    except Exception as e:
        logging.error("Titles: an error occurred while fetching repo title category: %s", e)
        return

    try:
        with open("proposalbot_prs.csv", "r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            rows = list(reader)
            header = rows.pop(0)
            for row in rows:
                for repo, title in [(r[1], r[2]) for r in repo_title_category]:
                    if repo == row[1]:
                        title_index = header.index("Service Name")
                        row[title_index] = title
    except IOError as e:
        logging.error("Titles: an error occurred while reading the file: %s", e)
        return
    except Exception as e:
        logging.error("Titles: an unexpected error occurred: %s", e)
        return

    try:
        with open("proposalbot_prs.csv", "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)
    except IOError as e:
        logging.error("Titles: an error occurred while writing to the file: %s", e)
        return
    except Exception as e:
        logging.error("Titles: an unexpected error occurred: %s", e)
        return


def add_squad_column(cur_csv, rtctable):
    logging.info("Add 'Squad' column into csv file...")
    try:
        repo_title_category = fetch_repo_title_category(cur_csv, rtctable)
    except Exception as e:
        logging.error("Squad column: an error occurred while fetching repo title category: %s", e)
        return

    try:
        with open("proposalbot_prs.csv", "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            header = rows.pop(0)
            header.insert(2, "Squad")
            for row in rows:
                name_service = row[1]
                for repo, title, squad in [(r[1], r[2], r[4]) for r in repo_title_category]:
                    if title == name_service:
                        row.insert(2, squad)
    except IOError as e:
        logging.error("Squad column: an error occurred while reading the file: %s", e)
        return
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
        return

    try:
        with open("proposalbot_prs.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
    except IOError as e:
        logging.error("Squad column: an error occurred while writing to the file: %s", e)
        return
    except Exception as e:
        logging.error("Squad column: an unexpected error occurred: %s", e)
        return


def compare_csv_files(conn_csv, cur_csv, conn_orph, cur_orph, opentable):
    logging.info("Gathering open and orphaned PRs...")
    try:
        doc_exports_prs = []
        proposalbot_prs = []

        with open("proposalbot_prs.csv", "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                proposalbot_prs.append(row)

        with open("doc_exports_prs.csv", "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                doc_exports_prs.append(row)

    except IOError as e:
        logging.error("Open and orphans for %s: an error occurred while trying to read the file: %s", opentable, e)
        return

    orphaned = []
    open_prs = []
    for pr1 in proposalbot_prs:
        for pr2 in doc_exports_prs:
            if pr1[0] == pr2[0] and pr1[4] != pr2[3]:
                if pr1 not in orphaned:
                    pr1.extend([pr2[3], pr2[4]])
                    orphaned.append(pr1)
                    try:
                        cur_orph.execute(f"""
                            INSERT INTO public.{opentable}
                            ("Parent PR Number", "Service Name", "Squad", "Auto PR URL", "Auto PR State", "If merged",
                             "Environment", "Parent PR State", "Parent PR merged")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, tuple(pr1))
                        conn_orph.commit()
                    except Exception as e:
                        logging.error("Open and orphans for ORPHANS and %s: an error occurred while inserting into "
                                      "the orphaned_prs table: %s", opentable, e)

            elif pr1[0] == pr2[0] and pr1[4] == pr2[3] == "open":
                if pr1 not in open_prs:
                    pr1.extend([pr2[3], pr2[4]])
                    open_prs.append(pr1)
                    try:
                        cur_csv.execute(f"""
                            INSERT INTO public.{opentable}
                            ("Parent PR Number", "Service Name", "Squad",  "Auto PR URL", "Auto PR State", "If merged",
                             "Environment", "Parent PR State", "Parent PR merged")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, tuple(pr1))
                        conn_csv.commit()
                    except Exception as e:
                        logging.error("Open and orphans for OPEN and %s: an error occurred while inserting"
                                      " into the open_prs table: %s", opentable, e)


def gitea_pr_info(org, parent_pr_name):
    parent_pr_num = None
    parent_pr_state = None
    parent_pr_merged = None
    pull_request_resp = session.get(f"{GITEA_API_ENDPOINT}/repos/{org}/{parent_pr_name}?token={gitea_token}")
    if pull_request_resp.status_code == 200:
        parent_info = json.loads(pull_request_resp.content.decode("utf-8"))
        parent_pr_num = parent_info.get("number")
        parent_pr_state = parent_info.get("state")
        parent_pr_merged = parent_info.get("merged")

    return parent_pr_num, parent_pr_state, parent_pr_merged


def get_github_open_prs(github_org, conn_csv, cur_csv, opentable, string):
    logging.info("Gathering Github open PRs for %s...", string)

    if not github_org or not conn_csv or not cur_csv:
        logging.error("Github PRs: error: Invalid input parameters.")
        return

    try:
        for repo in github_org.get_repos():
            for pr in repo.get_pulls(state='open'):
                if pr.body is not None and 'This is an automatically created Pull Request for changes to' in pr.body:
                    name_service = pr.base.repo.name
                    squad = ""
                    github_pr_url = pr.html_url
                    auto_pr_state = pr.state
                    if pr.merged_at is None:
                        merged = False
                    else:
                        merged = True
                    env = "Github"
                    match_url = re.search(rf"(?<={string})/.*(?=.)", pr.body)
                    if match_url:
                        parent_api_name = match_url.group(0)
                        parent_pr_num, parent_pr_state, parent_pr_merged = gitea_pr_info(parent_api_name, string)
                        cur_csv.execute(
                            f"""
                            INSERT INTO {opentable} ("Parent PR Number", "Service Name", "Squad",  "Auto PR URL",
                             "Auto PR State", "If merged", "Environment", "Parent PR State", "Parent PR merged")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (parent_pr_num, name_service, squad, github_pr_url, auto_pr_state, merged, env,
                             parent_pr_state, parent_pr_merged)
                        )
                        conn_csv.commit()
    except Exception as e:
        logging.error('Github PRs: an error occurred:', e)


def update_squad_and_title(cursors, conns, rtctable, opentable):
    logging.info("Updating squads and titles...")
    for cur in cursors:
        try:
            cur.execute(f"SELECT * FROM {opentable};")
            open_issues_rows = cur.fetchall()

            for row in open_issues_rows:
                cur.execute(
                    f"""UPDATE {opentable}
                        SET "Service Name" = rtc."Title", "Squad" = rtc."Squad"
                        FROM {rtctable} AS rtc
                        WHERE {opentable}."Service Name" = rtc."Repository"
                        AND {opentable}.id = %s;""",
                    (row[0],)
                )
                cur.execute(
                    f"""UPDATE {opentable}
                        SET "Squad" = 'Other'
                        WHERE {opentable}."Service Name" IN ('doc-exports', 'docs_on_docs', 'docsportal')
                        AND {opentable}.id = %s;""",
                    (row[0],)
                )
                for conn in conns:
                    conn.commit()

        except Exception as e:
            logging.error("Error updating squad and title: %s", e)


def main(org, gh_org, rtctable, opentable, string, token):
    check_env_variables()
    csv_erase(["proposalbot_prs.csv", "doc_exports_prs.csv", "orphaned_prs.csv"])

    conn_csv = connect_to_db(db_csv)
    cur_csv = conn_csv.cursor()
    conn_orph = connect_to_db(db_orph)
    cur_orph = conn_orph.cursor()
    g = Github(token)
    github_org = g.get_organization(gh_org)

    cur_csv.execute(f"DROP TABLE IF EXISTS {opentable}")
    conn_csv.commit()

    cursors = [cur_csv, cur_orph]
    conns = [conn_csv, conn_orph]

    create_prs_table(conn_csv, cur_csv, opentable)

    repos = get_repos(org, cur_csv, gitea_token, rtctable)
    logging.info("Gathering parent PRs...")
    for repo in repos:
        get_parent_pr(org, repo)
    get_pull_requests(org, "doc-exports")

    update_service_titles(cur_csv, rtctable)
    add_squad_column(cur_csv, rtctable)

    cur_orph.execute(f"DROP TABLE IF EXISTS {opentable}")
    conn_orph.commit()
    create_prs_table(conn_orph, cur_orph, opentable)
    compare_csv_files(conn_csv, cur_csv, conn_orph, cur_orph, opentable)

    get_github_open_prs(github_org, conn_csv, cur_csv, opentable, string)

    update_squad_and_title(cursors, conns, rtctable, opentable)

    for conn in conns:
        conn.close()


if __name__ == "__main__":
    RTC_TABLE = "repo_title_category"
    OPEN_TABLE = "open_prs"
    ORG_STRING = "docs"
    GH_ORG_STRING = "opentelekomcloud-docs"

    DONE = False

    try:
        main(ORG_STRING, GH_ORG_STRING, RTC_TABLE, OPEN_TABLE, ORG_STRING, github_token)
        main(f"{ORG_STRING}-swiss", f"{GH_ORG_STRING}-swiss", f"{RTC_TABLE}_swiss", f"{OPEN_TABLE}_swiss",
             f"{ORG_STRING}-swiss", github_token)
        DONE = True
    except Exception as e:
        logging.info("Error has been occurred: %s", e)
        main(ORG_STRING, GH_ORG_STRING, RTC_TABLE, OPEN_TABLE, ORG_STRING, github_fallback_token)
        main(f"{ORG_STRING}-swiss", f"{GH_ORG_STRING}-swiss", f"{RTC_TABLE}_swiss", f"{OPEN_TABLE}_swiss",
             f"{ORG_STRING}-swiss", github_fallback_token)
        DONE = True
    if DONE:
        logging.info("Github operations successfully done!")

    csv_erase(["proposalbot_prs.csv", "doc_exports_prs.csv", "orphaned_prs.csv"])
    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info("Script executed in %s minutes %s seconds! Let's go drink some beer :)", int(minutes), int(seconds))
