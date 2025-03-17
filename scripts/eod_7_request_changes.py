import json
import logging
import re
from datetime import datetime

import psycopg2
import requests

from config import Database, EnvVariables, Timer, setup_logging

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)


def create_prs_table(conn, cur, table_name):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "PR Number" VARCHAR(255),
            "Service Name" VARCHAR(255),
            "Squad" VARCHAR(255),
            "PR URL" VARCHAR(255),
            "Days passed" INT,
            "Reviewer" VARCHAR(255),
            "Parent PR Status" VARCHAR(255)
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      table_name, e)


def get_repos(cur, rtc):
    repos = []

    try:
        cur.execute(f"SELECT DISTINCT \"Repository\" FROM {rtc} WHERE \"Env\" IN ('public', 'tech');")
        repos = [row[0] for row in cur.fetchall()]
        if not repos:
            logging.info("No repositories found.")
    except Exception as e:
        logging.error("Fetching repos: %s", e)

    return repos


def get_pr_number(org, repo):
    page = 1
    pr_details = []
    while True:
        try:
            repo_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state=open&page={page}"
                                    f"&limit=1000&token={env_vars.gitea_token}")
            repo_resp.raise_for_status()
            pull_requests = json.loads(repo_resp.content.decode())
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.info("No repository or pull requests found in %s (404 error). Skipping.", repo)
                return []
            else:
                logging.error("Error checking pull requests in %s: %s", repo, e)
                return []

        except json.JSONDecodeError as e:
            logging.error("Error occurred while trying to decode JSON: %s", e)
            break

        if not pull_requests:
            break

        for pr in pull_requests:
            pr_details.append({'pr_number': pr['number']})

        link_header = repo_resp.headers.get("Link")
        if link_header is None or "rel=\"next\"" not in link_header:
            break
        else:
            page += 1

    return pr_details


def convert_iso_to_datetime(iso_str):
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))


def process_pr_reviews(org, repo, pr_number, changes_tab, conn_csv, cur_csv):
    reviews = []
    try:
        reviews_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/reviews?token="
                                   f"{env_vars.gitea_token}")
        reviews_resp.raise_for_status()
        reviews = json.loads(reviews_resp.content.decode())
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.info("No reviews found for PR %s in %s (404 error). Skipping.", pr_number, repo)
            return False
    except requests.exceptions.RequestException as e:
        logging.error("Error occurred while trying to get PR %s reviews: %s", pr_number, e)
        return
    except json.JSONDecodeError as e:
        logging.error("Error occurred while trying to decode JSON: %s", e)
        return

    final_review = reviews[-1] if reviews else None
    if final_review and final_review['state'] == "REQUEST_CHANGES":
        last_review_date_str = final_review["updated_at"]
        last_review_date = convert_iso_to_datetime(last_review_date_str)
        reviewer_login = final_review['user']['login']

        get_last_commit(org, repo, pr_number, reviewer_login, last_review_date, changes_tab, conn_csv, cur_csv)


def get_last_commit(org, repo, pr_number, reviewer_login, last_review_date, changes_tab, conn_csv, cur_csv):
    try:
        commits_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/commits?token="
                                   f"{env_vars.gitea_token}")
        commits_resp.raise_for_status()
        commits = json.loads(commits_resp.content.decode())
    except requests.exceptions.RequestException as e:
        logging.error("Error occurred while trying to get commits for PR %s: %s", pr_number, e)
        return None
    except json.JSONDecodeError as e:
        logging.error("Error occurred while trying to decode JSON: %s", e)
        return None

    commit = commits[0] if commits else None
    if commit:
        commit_date_str = commit["commit"]["committer"]["date"]
        commit_date = convert_iso_to_datetime(commit_date_str)
        commit_author_info = commit.get("author")
        commit_author = commit_author_info.get("login") if commit_author_info else None
        if commit_author != reviewer_login and commit_date < last_review_date:
            insert_data_postgres(org, repo, pr_number, conn_csv, cur_csv, last_review_date, changes_tab)
        elif commit_author != reviewer_login and commit_date > last_review_date:
            insert_data_postgres(org, repo, pr_number, conn_csv, cur_csv, commit_date, "our_side_problem")

    return None


def insert_data_postgres(org, repo, pr_number, conn, cur, activity_date, changes_tab):
    try:
        filtered_reviews_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/"
                                            f"reviews?token={env_vars.gitea_token}")
        filtered_reviews_resp.raise_for_status()
        filtered_reviews = json.loads(filtered_reviews_resp.content.decode())
    except requests.exceptions.RequestException as e:
        logging.error("Error occurred while trying to get PR %s reviews: %s", pr_number, e)
        return
    except json.JSONDecodeError as e:
        logging.error("Error occurred while trying to decode JSON: %s", e)
        return

    if not filtered_reviews:
        logging.info("No reviews found for PR %s in %s.", pr_number, repo)
        return
    final_review = filtered_reviews[-1] if filtered_reviews else None
    pr_url = final_review["pull_request_url"]
    last_activity_date = activity_date.date() if isinstance(activity_date, datetime) else activity_date
    current_date = datetime.utcnow().date()
    days_since_last_activity = (current_date - last_activity_date).days
    reviewer_name = final_review['user']['full_name']

    cur.execute(f"""
        INSERT INTO {changes_tab} ("PR Number", "Service Name", "Squad", "PR URL", "Days passed", "Reviewer",
        "Parent PR Status")
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (pr_number, repo, '', pr_url, days_since_last_activity, reviewer_name, 'No changes requested'))
    conn.commit()


def parent_pr_changes_check(cur, conn, org, changes_tab):
    try:
        cur.execute(f"SELECT \"PR Number\", \"Service Name\" FROM {changes_tab}")
        records = cur.fetchall()
        repo_pr_dict = {record[1]: record[0] for record in records}
    except Exception as e:
        logging.error("Fetching PR numbers: %s", e)
        return

    for repo, pr_number in repo_pr_dict.items():
        try:
            pr_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}?token="
                                  f"{env_vars.gitea_token}")
            pr_resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.info("No repository or pull requests found in %s (404 error). Skipping.", repo)
                return False
            else:
                logging.error("Error checking pull requests in %s: %s", repo, e)
                return False
        except requests.exceptions.RequestException as e:
            logging.error("Error occurred while trying to get PR %s in repo %s: %s", pr_number, repo, e)
            break

        try:
            parent_pr = json.loads(pr_resp.content.decode())
        except json.JSONDecodeError as e:
            logging.error("Error occurred while trying to decode JSON: %s", e)
            break

        body = parent_pr["body"]
        if body.startswith("This is an automatically created Pull Request"):
            match_repo = re.search(r"(?<=/).+(?=#)", str(body))
            repo_name = match_repo.group(0)
            parent_pr_number = extract_number_from_body(body)
            parent_reviews = []
            try:
                parent_reviews_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo_name}/pulls/"
                                                  f"{parent_pr_number}/reviews?token={env_vars.gitea_token}")
                parent_reviews_resp.raise_for_status()
                parent_reviews = json.loads(parent_reviews_resp.content.decode())
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logging.info("No reviews found for PR %s in %s (404 error). Skipping.", parent_pr_number,
                                 repo_name)
                    return False
            except requests.exceptions.RequestException as e:
                logging.error("Error occurred while trying to get PR %s reviews: %s", parent_pr_number, e)
                return
            except json.JSONDecodeError as e:
                logging.error("Error occurred while trying to decode JSON: %s", e)
                return

            final_review = parent_reviews[-1] if parent_reviews else None
            if final_review and final_review['state'] == "REQUEST_CHANGES":
                try:
                    cur.execute(f"""
                        UPDATE {changes_tab} SET "Parent PR Status" = 'CHANGES REQUESTED'
                        WHERE "PR Number" = %s AND "Service Name" = %s;
                    """, (pr_number, repo))
                    conn.commit()
                    logging.info("Updated PR %s in %s to CHANGES REQUESTED.", pr_number, repo)
                except psycopg2.Error as e:
                    logging.error("Error updating database: %s", e)


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


def update_squad_and_title(cur, conn, rtc, changes_tab):
    logging.info("Updating squads and titles...")
    try:
        cur.execute(f"SELECT * FROM {changes_tab};")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                f"""UPDATE {changes_tab}
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Squad"
                    FROM {rtc} AS rtc
                    WHERE {changes_tab}."Service Name" = rtc."Repository"
                    AND {changes_tab}.id = %s;""",
                (row[0],)
            )
            cur.execute(
                f"""UPDATE {changes_tab}
                    SET "Parent PR Status" = 'CHANGES REQUESTED'
                    WHERE {changes_tab}."Service Name" IN ('doc-exports', 'docs_on_docs', 'docsportal')
                    AND {changes_tab}.id = %s;""",
                (row[0],)
            )
        conn.commit()

    except Exception as e:
        logging.error("Error updating squad and title: %s", e)


def main(org, rtc, changes_tab):
    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()
    cur_csv.execute(f"DROP TABLE IF EXISTS {changes_tab}")
    conn_csv.commit()

    create_prs_table(conn_csv, cur_csv, changes_tab)

    repos = get_repos(cur_csv, rtc)

    logging.info("Gathering PRs where changes has been requested...")

    for repo in repos:
        prs = get_pr_number(org, repo)
        for pr_info in prs:
            pr_number = pr_info['pr_number']
            process_pr_reviews(org, repo, pr_number, changes_tab, conn_csv, cur_csv)

    parent_pr_changes_check(cur_csv, conn_csv, org, changes_tab)
    parent_pr_changes_check(cur_csv, conn_csv, org, "our_side_problem")
    update_squad_and_title(cur_csv, conn_csv, rtc, changes_tab)


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------REQUEST CHANGES SCRIPT IS RUNNING-------------------------")

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    org_string = "docs"
    rtc_table = "repo_title_category"
    changes_table = "requested_changes"

    done = False

    cur_csv.execute("DROP TABLE IF EXISTS our_side_problem")
    create_prs_table(conn_csv, cur_csv, "our_side_problem")

    conn_csv.commit()

    main(org_string, rtc_table, changes_table)
    main(f"{org_string}-swiss", f"{rtc_table}_swiss", f"{changes_table}_swiss")

    update_squad_and_title(cur_csv, conn_csv, rtc_table, "our_side_problem")

    if done:
        logging.info("Search successfully finish!")

    cur_csv.close()
    conn_csv.close()

    timer.stop()


if __name__ == "__main__":
    run()
