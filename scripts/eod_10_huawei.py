import json
import logging
import re

import psycopg2
import requests

from config import Database, EnvVariables, Timer, setup_logging

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)


def create_prs_table(conn, cur, huawei):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {huawei} (
            id SERIAL PRIMARY KEY,
            "PR Number" VARCHAR(255),
            "Service Name" VARCHAR(255),
            "Squad" VARCHAR(255),
            "PR URL" VARCHAR(255),
            "Days passed" INT,
            "Reviewer" VARCHAR(255),
            "Label" VARCHAR(255),
            "Huawei comment" VARCHAR(255)
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", huawei)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      huawei, e)


def get_requested_prs(cur, changes):
    requested_prs = []

    try:
        cur.execute(f'''SELECT DISTINCT "PR URL", "Days passed", "Reviewer"
                        FROM {changes}
                        WHERE "Parent PR Status" = 'CHANGES REQUESTED' AND "Squad" NOT IN ('Huawei')
                        AND "Days passed" > 3;''')
        requested_prs = cur.fetchall()

        if not requested_prs:
            logging.info("No repositories found.")
    except Exception as e:
        logging.error("Fetching repos: %s", e)

    # print("REQUESTED PRS FROM CHANGES TAB__________________________________", type(requested_prs), len(requested_prs))
    return requested_prs


def parse_pr_url(requested_prs, org):
    parsed_prs = []

    for pr_url, days_passed, reviewer in requested_prs:
        try:
            repo_match = re.search(rf"(?<={org}\/)(.*?)(?=\/pulls)", str(pr_url))
            number_match = re.search(r"(?<=pulls\/)(\d+)", str(pr_url))

            if repo_match and number_match:
                repo = repo_match.group()
                pr_number = int(number_match.group())

                parsed_prs.append({"pr_number": pr_number, "repo": repo, "pr_url": pr_url,
                                   "days_passed": days_passed, "reviewer": reviewer})

        except (ValueError, re.error) as e:
            logging.error("Error parsing PR URL %s: %s", pr_url, e)
    # print("PR LIST_________________________________", len(parsed_prs))

    return parsed_prs


def get_analyzed_prs(org, parsed_prs):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }
    analyzed_prs = []

    for pr in parsed_prs:
        pr_number = pr["pr_number"]
        repo = pr["repo"]
        pr_url = pr["pr_url"]
        days_passed = pr["days_passed"]
        reviewer = pr["reviewer"]

        try:
            labels_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}", headers=headers)
            labels_resp.raise_for_status()
            pr_data = labels_resp.json()

            labels = pr_data.get("labels", [])
            has_analyzed_label = any(pr_label["name"] == "analyzed" for pr_label in labels)

            if has_analyzed_label:
                label = "Analyzed"
            else:
                label = "Not labeled"
            analyzed_prs.append({"pr_number": pr_number, "repo": repo,  "pr_url": pr_url, "days_passed": days_passed,
                                 "reviewer": reviewer, "pr_label": label})
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.info("No reviews found for PR %s in %s (404 error). Skipping.", pr_number, repo)
            continue
        except requests.exceptions.RequestException as e:
            logging.error("Error occurred while trying to get PR %s reviews: %s", pr_number, e)
            continue
        except json.JSONDecodeError as e:
            logging.error("Error occurred while trying to decode JSON: %s", e)
            continue
    # print("ANALYZED PRS_________________________________", len(analyzed_prs))

    return analyzed_prs


def search_comments(org, analyzed_prs):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }
    comments = []
    for pr in analyzed_prs:
        pr_number = pr["pr_number"]
        repo = pr["repo"]
        pr_url = pr["pr_url"]
        days_passed = pr["days_passed"]
        reviewer = pr["reviewer"]
        pr_label = pr["pr_label"]

        try:
            reviews_resp = session.get(
                f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/reviews",
                headers=headers
            )
            reviews_resp.raise_for_status()
            reviews_data = reviews_resp.json()

            latest_review = reviews_data[-1]
            comments_count = latest_review.get("comments_count", 0)
            review_id = latest_review["id"]

            if comments_count == 0:
                print(f"PR {pr_number} in {repo} has {comments_count} comments in review {review_id}")
            else:
                print(f"PR {pr_number} in {repo} has {comments_count} comments in review {review_id}")
            comments.append(
                {"pr_number": pr_number, "repo": repo, "pr_url": pr_url, "days_passed": days_passed,
                 "pr_label": pr_label, "reviewer": reviewer, "review_id": review_id, "comments_count": comments_count})

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.info("No reviews found for PR %s in %s (404 error). Skipping.", pr_number, repo)
            continue
        except requests.exceptions.RequestException as e:
            logging.error("Error occurred while trying to get PR %s reviews: %s", pr_number, e)
            continue
        except json.JSONDecodeError as e:
            logging.error("Error occurred while trying to decode JSON: %s", e)
            continue
    # print("COMMENTS__________________________", comments)
    return comments


def get_review_comments_info(org, comments):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }
    comments_list = []
    for i in comments:
        pr_number = i["pr_number"]
        repo = i["repo"]
        pr_url = i["pr_url"]
        days_passed = i["days_passed"]
        pr_label = i["pr_label"]
        reviewer = i["reviewer"]
        review_id = i["review_id"]
        comments_count = i["comments_count"]
        try:
            comments_resp = session.get(
                f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/reviews/{review_id}/comments",
                headers=headers
            )
            comments_resp.raise_for_status()
            comments_data = comments_resp.json()
            if comments_count == 0:
                huawei_comment = "Not commented"
            else:
                latest_comment = comments_data[-1]
                latest_comment_author = latest_comment["user"]["full_name"]

                if latest_comment_author != reviewer:
                    print(f"Latest comment in {pr_number} in {repo} for {review_id} by Huawei {latest_comment_author}")
                    huawei_comment = "Commented"
                else:
                    print(f"Latest comment in {pr_number} in {repo} for {review_id} by review author {reviewer}")
                    huawei_comment = "Not commented"
            comments_list.append({"pr_number": pr_number, "repo": repo, "pr_url": pr_url, "days_passed": days_passed,
                                  "reviewer": reviewer, "pr_label": pr_label, "huawei_comment": huawei_comment})

        except requests.exceptions.RequestException as e:
            logging.error("Error occurred while trying to get PR %s reviews: %s", pr_number, e)
        # print("COMMENTS LIST-----------------", len(comments_list), comments_list[-1])
    return comments_list


def insert_analyzed_prs(conn, cur, huawei, analyzed_prs):
    try:
        for pr in analyzed_prs:
            cur.execute(
                f'''INSERT INTO {huawei} ("PR Number", "Service Name", "PR URL", "Days passed", "Label", "Reviewer",
                "Huawei comment")
                VALUES (%s, %s, %s, %s, %s, %s, %s);''',
                (pr["pr_number"], pr["repo"], pr["pr_url"], pr["days_passed"], pr["pr_label"], pr["reviewer"],
                 pr["huawei_comment"])
            )
        conn.commit()
        logging.info("Inserted %d analyzed PRs into %s", len(analyzed_prs), huawei)
    except psycopg2.Error as e:
        logging.error("Error inserting analyzed PRs: %s", e)


def update_squad_and_title(cur, conn, rtc, huawei_tab):
    logging.info("Updating squads and titles...")
    try:
        cur.execute(f"SELECT * FROM {huawei_tab};")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                f"""UPDATE {huawei_tab}
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Squad"
                    FROM {rtc} AS rtc
                    WHERE {huawei_tab}."Service Name" = rtc."Repository"
                    AND {huawei_tab}.id = %s;""",
                (row[0],)
            )

        conn.commit()

    except Exception as e:
        logging.error("Error updating squad and title: %s", e)


def main(conn, cur, org, rtc, changes_tab, huawei_tab):
    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()
    cur_csv.execute(f"DROP TABLE IF EXISTS {huawei_tab}")
    conn_csv.commit()

    create_prs_table(conn_csv, cur_csv, huawei_tab)

    requested_prs = get_requested_prs(cur_csv, changes_tab)
    logging.info("Looking for labels in requested changes PRs...")
    parsed_prs = parse_pr_url(requested_prs, org)
    analyzed_prs = get_analyzed_prs(org, parsed_prs)
    comments = search_comments(org, analyzed_prs)
    comments_list = get_review_comments_info(org, comments)

    insert_analyzed_prs(conn, cur, huawei_tab, comments_list)
    update_squad_and_title(cur_csv, conn_csv, rtc, huawei_tab)


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------HUAWEI SCRIPT IS RUNNING-------------------------")

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    org_string = "docs"
    rtc_table = "repo_title_category"
    changes_table = "requested_changes"
    huawei_label_table = "huawei_label"

    done = False

    conn_csv.commit()

    main(conn_csv, cur_csv, org_string, rtc_table, changes_table, huawei_label_table)
    main(conn_csv, cur_csv, f"{org_string}-swiss", f"{rtc_table}_swiss", f"{changes_table}_swiss",
         f"{huawei_label_table}_swiss")

    if done:
        logging.info("Search successfully finish!")

    cur_csv.close()
    conn_csv.close()

    timer.stop()


if __name__ == "__main__":
    run()
