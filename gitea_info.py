import os
import requests
import json
import csv
import re
import pathlib
import psycopg2
from github import Github
import time

start_time = time.time()

print("**OPEN PRs SCRIPT IS RUNNING**")

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()
session.debug = False
gitea_token = os.getenv("GITEA_TOKEN")
github_token = os.getenv("GITHUB_TOKEN")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_csv = os.getenv("DB_CSV")
db_orph = os.getenv("DB_ORPH")
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


def csv_erase():
    try:
        proposalbot = pathlib.Path("proposalbot_prs.csv")
        docexports = pathlib.Path("doc_exports_prs.csv")
        orphaned = pathlib.Path("orphaned_prs.csv")
        if proposalbot.exists() is True:
            proposalbot.unlink()
        if docexports.exists() is True:
            docexports.unlink()
        if orphaned.exists() is True:
            orphaned.unlink()
        print("CSV erased")
    except Exception as e:
        print(f"CSV erasing: an error occurred while trying to delete csv files: {e}")


def connect_to_db(db_name):
    print(f"Connecting to Postgres ({db_name})...")
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
        print(f"Table {table_name} has been created successfully")
    except psycopg2.Error as e:
        print(f"Tables creating: an error occurred while trying to create a table {table_name} in the database: {e}")


def get_repos(org, gitea_token):
    repos = []
    page = 1
    while True:
        try:
            repos_resp = session.get(f"{gitea_api_endpoint}/orgs/{org}/repos?page={page}&limit=50&token={gitea_token}")
            repos_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Get repos: an error occurred while trying to get repos: {e}")
            break

        try:
            repos_dict = json.loads(repos_resp.content.decode())
        except json.JSONDecodeError as e:
            print(f"JSON decode: an error occurred while trying to decode JSON: {e}")
            break

        for repo in repos_dict:
            repos.append(repo["name"])

        link_header = repos_resp.headers.get("Link")
        if link_header is None or "rel=\"next\"" not in link_header:
            break
        else:
            page += 1

    print(len(repos), "repos has been processed")

    return repos


def get_parent_pr(org, repo):
    try:
        path = pathlib.Path("proposalbot_prs.csv")
        if path.exists() is False:
            csv_2 = open("proposalbot_prs.csv", "w")
            csv_writer = csv.writer(csv_2)
            csv_writer.writerow(["Parent PR number", "Service Name", "Auto PR URL", "Auto PR State", "If merged", "Environment"])
        else:
            csv_2 = open("proposalbot_prs.csv", "a")
            csv_writer = csv.writer(csv_2)
    except IOError as e:
        print(f"Proposalbot_prs.csv: an error occurred while trying to open or write to CSV file: {e}")
        return
    if repo != "doc-exports" and repo != "dsf":
        page = 1
        while True:
            try:
                repo_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state=all&page={page}&limit=1000&token={gitea_token}")
                repo_resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error occurred while trying to get repo pull requests: {e}")
                break

            try:
                pull_request = json.loads(repo_resp.content.decode())
            except json.JSONDecodeError as e:
                print(f"Error occurred while trying to decode JSON: {e}")
                break

            dependency_pull_requests = []

            for pr in pull_request:
                dependency_pull_requests.append(pr)

            for pull_req in dependency_pull_requests:
                body = pull_req["body"]
                if body.startswith("This is an automatically created Pull Request"):
                    if pull_req["state"] == "closed" and pull_req["merged"] is False:
                        continue
                    else:
                        parent_pr = extract_number_from_body(body)
                        service = repo
                        auto_url = pull_req["url"]
                        auto_state = pull_req["state"]
                        if_merged = pull_req["merged"]
                        env = "Gitea"
                        try:
                            csv_writer.writerow([parent_pr, service, auto_url, auto_state, if_merged, env])
                        except csv.Error as e:
                            print(f"Error occurred while trying to write to CSV file: {e}")
                            break
            link_header = repo_resp.headers.get("Link")
            if link_header is None or "rel=\"next\"" not in link_header:
                break
            else:
                page += 1
    try:
        csv_2.close()

    except IOError as e:
        print(f"Error occurred while trying to close CSV file: {e}")


def extract_number_from_body(text):
    try:
        match = re.search(r"#\d+", str(text))
        if match:
            return int(match.group()[1:])
    except ValueError as e:
        print(f"An error occurred while converting match group to int: {e}")
        return None
    except re.error as e:
        print(f"An error occurred while searching text: {e}")
        return None
    return None


def get_pull_requests(org, repo):
    print("Gathering Gitea's child PRs...")
    states = ["open", "closed"]
    pull_requests = []
    try:
        csv_file = open("doc_exports_prs.csv", "a", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Parent PR index", "Parent PR title", "Parent PR URL", "Parent PR state", "If merged"])
    except IOError as e:
        print(f"Child PRs: an error occurred while opening or writing to the CSV file: {e}")
        return pull_requests

    for state in states:
        page = 1
        while True:
            try:
                pull_requests_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state={state}&page={page}&limit=50&token={gitea_token}")
                pull_requests_resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Child PRs: an error occurred while trying to get pull requests of {repo} repo: {e}")
                break

            try:
                pull_requests = json.loads(pull_requests_resp.content.decode("utf-8"))
            except json.JSONDecodeError as e:
                print(f"Child PRs: an error occurred while trying to decode JSON: {e}")
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
                    print(f"Child PRs: an error occurred while trying to write to CSV file: {e}")
                    break

            link_header = pull_requests_resp.headers.get("Link")
            if link_header is None or "rel=\"next\"" not in link_header:
                break
            else:
                page += 1
    try:
        csv_file.close()
    except IOError as e:
        print(f"Child PRs: n error occurred while trying to close CSV file: {e}")

    return pull_requests


def fetch_repo_title_category(cur_csv, rtctable):
    print(f"Fetching RTC table {rtctable}...")
    try:
        cur_csv.execute(f"SELECT * FROM {rtctable}")
        return cur_csv.fetchall()
    except Exception as e:
        print(f"Fetch: an error occurred while trying to fetch data from the table: {e}")
        return None


def update_service_titles(cur_csv, rtctable):
    print(f"Updating service titles using {rtctable}..")
    try:
        repo_title_category = fetch_repo_title_category(cur_csv, rtctable)
    except Exception as e:
        print(f"Titles: an error occurred while fetching repo title category: {e}")
        return

    try:
        with open("proposalbot_prs.csv", "r", newline="") as file:
            reader = csv.reader(file)
            rows = list(reader)
            header = rows.pop(0)
            for i, row in enumerate(rows):
                for (repo_id, repo, title, category, stype) in repo_title_category:
                    if repo == row[1]:
                        title_index = header.index("Service Name")
                        row[title_index] = title
    except IOError as e:
        print(f"Titles: an error occurred while reading the file: {e}")
        return
    except Exception as e:
        print(f"Titles: an unexpected error occurred: {e}")
        return

    try:
        with open("proposalbot_prs.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)
    except IOError as e:
        print(f"Titles: an error occurred while writing to the file: {e}")
        return
    except Exception as e:
        print(f"Titles: an unexpected error occurred: {e}")
        return


def add_squad_column(cur_csv, rtctable):
    print("Add 'Squad' column into csv file...")
    try:
        repo_title_category = fetch_repo_title_category(cur_csv, rtctable)
    except Exception as e:
        print(f"Squad column: an error occurred while fetching repo title category: {e}")
        return

    try:
        with open("proposalbot_prs.csv", "r", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            header = rows.pop(0)
            header.insert(2, "Squad")
            for row in rows:
                name_service = row[1]
                for (repo_id, repo, title, category, stype) in repo_title_category:
                    if title == name_service:
                        row.insert(2, category)
    except IOError as e:
        print(f"Squad column: an error occurred while reading the file: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return

    try:
        with open("proposalbot_prs.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
    except IOError as e:
        print(f"Squad column: an error occurred while writing to the file: {e}")
        return
    except Exception as e:
        print(f"Squad column: an unexpected error occurred: {e}")
        return


def compare_csv_files(conn_csv, cur_csv, conn_orph, cur_orph, opentable):
    print("Gathering open and orphaned PRs...")
    try:
        doc_exports_prs = []
        proposalbot_prs = []

        with open("proposalbot_prs.csv", "r") as f:
            reader = csv.reader(f)
            for row in reader:
                proposalbot_prs.append(row)

        with open("doc_exports_prs.csv", "r") as f:
            reader = csv.reader(f)
            for row in reader:
                doc_exports_prs.append(row)

    except IOError as e:
        print(f"Open and orphans: an error occurred while trying to read the file: {e}")
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
                            ("Parent PR Number", "Service Name", "Squad", "Auto PR URL", "Auto PR State", "If merged", "Environment", "Parent PR State", "Parent PR merged")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, tuple(pr1))
                        conn_orph.commit()
                    except Exception as e:
                        print(f"Open and orphans: an error occurred while inserting into the orphaned_prs table: {e}")

            elif pr1[0] == pr2[0] and pr1[4] == pr2[3] == "open":
                if pr1 not in open_prs:
                    pr1.extend([pr2[3], pr2[4]])
                    open_prs.append(pr1)
                    try:
                        cur_csv.execute(f"""
                            INSERT INTO public.{opentable}
                            ("Parent PR Number", "Service Name", "Squad",  "Auto PR URL", "Auto PR State", "If merged", "Environment", "Parent PR State", "Parent PR merged")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, tuple(pr1))
                        conn_csv.commit()
                    except Exception as e:
                        print(f"Open and orphans: an error occurred while inserting into the open_prs table: {e}")


def gitea_pr_info(org, parent_pr_name):
    parent_pr_num = None
    parent_pr_state = None
    parent_pr_merged = None
    pull_request_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{parent_pr_name}?token={gitea_token}")
    if pull_request_resp.status_code == 200:
        parent_info = json.loads(pull_request_resp.content.decode("utf-8"))
        parent_pr_num = parent_info.get("number")
        parent_pr_state = parent_info.get("state")
        parent_pr_merged = parent_info.get("merged")

    return parent_pr_num, parent_pr_state, parent_pr_merged


def get_github_open_prs(github_org, conn_csv, cur_csv, opentable, string):
    print(f"Gathering Github open PRs for {string}...")

    if not github_org or not conn_csv or not cur_csv:
        print("Github PRs: error: Invalid input parameters.")
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
                            INSERT INTO {opentable} ("Parent PR Number", "Service Name", "Squad",  "Auto PR URL", "Auto PR State", "If merged", "Environment", "Parent PR State", "Parent PR merged")
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            (parent_pr_num, name_service, squad, github_pr_url, auto_pr_state, merged, env, parent_pr_state, parent_pr_merged)
                        )
                        conn_csv.commit()
    except Exception as e:
        print('Github PRs: an error occurred:', e)


def update_squad_and_title(conn_csv, cur_csv, rtctable, opentable):
    print("Updating squads and titles...")
    try:
        cur_csv.execute("SELECT * FROM open_prs;")
        open_issues_rows = cur_csv.fetchall()

        for row in open_issues_rows:
            cur_csv.execute(
                f"""UPDATE {opentable}
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Category"
                    FROM {rtctable} AS rtc
                    WHERE {opentable}."Service Name" = rtc."Repository"
                    AND {opentable}.id = %s;""",
                (row[0],)
            )
            cur_csv.execute(
                f"""UPDATE {opentable}
                    SET "Squad" = 'Other'
                    WHERE {opentable}."Service Name" IN ('doc-exports', 'docs_on_docs', 'docsportal')
                    AND {opentable}.id = %s;""",
                (row[0],)
            )
            conn_csv.commit()

    except Exception as e:
        print(f"Error updating squad and title: {e}")
        conn_csv.rollback()


def main(org, gh_org, rtctable, opentable, string):
    check_env_variables()
    csv_erase()

    conn_csv = connect_to_db(db_csv)
    cur_csv = conn_csv.cursor()

    g = Github(github_token)
    github_org = g.get_organization(gh_org)

    cur_csv.execute(f"DROP TABLE IF EXISTS {opentable}")
    conn_csv.commit()

    create_prs_table(conn_csv, cur_csv, opentable)

    repos = get_repos(org, gitea_token)
    print("Gathering parent PRs...")
    for repo in repos:
        get_parent_pr(org, repo)

    get_pull_requests(org, "doc-exports")

    update_service_titles(cur_csv, rtctable)
    add_squad_column(cur_csv, rtctable)

    conn_orph = connect_to_db(db_orph)
    cur_orph = conn_orph.cursor()

    cur_orph.execute(f"DROP TABLE IF EXISTS {opentable}")
    conn_orph.commit()

    create_prs_table(conn_orph, cur_orph, opentable)
    compare_csv_files(conn_csv, cur_csv, conn_orph, cur_orph, opentable)

    csv_erase()

    get_github_open_prs(github_org, conn_csv, cur_csv, opentable, string)
    update_squad_and_title(conn_csv, cur_csv, rtctable, opentable)

    cur_csv.close()
    conn_csv.close()

    cur_orph.close()
    conn_orph.close()

    end_time = time.time()
    execution_time = end_time - start_time
    minutes, seconds = divmod(execution_time, 60)
    print(f"Script executed in {int(minutes)} minutes {int(seconds)} seconds! Let's go drink some beer :)")


if __name__ == "__main__":
    rtc_table = "repo_title_category"
    open_table = "open_prs"
    org_string = "docs"
    main("docs", "opentelekomcloud-docs", rtc_table, open_table, org_string)
    main("docs-swiss", "opentelekomcloud-docs-swiss", f"{rtc_table}_swiss", f"{open_table}_swiss", f"{org_string}-swiss")
