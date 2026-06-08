import logging
import psycopg2
import requests
import re

from config import Database, EnvVariables, Timer, setup_logging

session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)

gitea_api_endpoint = env_vars.base_gitea_url


def create_missing_child_prs_table(conn, cur, missing_child_prs):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {missing_child_prs} (
            id SERIAL PRIMARY KEY,
            "PR Number" INT,
            "Service Name" VARCHAR(255),
            "Squad" VARCHAR(255),
            "PR URL" VARCHAR(255),
            "If Child" VARCHAR(255),
            "Head SHA" VARCHAR(255)
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", missing_child_prs)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      missing_child_prs, e)


def get_open_prs(org, repo):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }

    open_prs = []
    page = 1
    per_page = 50

    try:
        while True:
            prs_resp = session.get(
                f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls",
                headers=headers,
                params={
                    "state": "open",
                    "page": page,
                    "limit": per_page
                }
            )
            prs_resp.raise_for_status()
            prs_data = prs_resp.json()

            if not prs_data:
                break

            open_prs.extend(prs_data)
            page += 1

            logging.info("Fetched page %s, got %s PRs", page - 1, len(prs_data))

        logging.info("Total open PRs found: %s", len(open_prs))

    except requests.exceptions.RequestException as e:
        logging.error("Error occurred while fetching open PRs: %s", e)

    return open_prs


def collect_prs_with_file_info(org, repo, open_prs):
    prs_info = []

    for pr in open_prs:
        pr_number = pr.get("number")
        pr_url = pr.get("html_url")
        head_sha = pr.get("head", {}).get("sha")

        logging.info("Checking PR #%s — files and timeline...", pr_number)

        service_name = None
        has_required_files = False
        has_child_timeline = False

        try:
            headers = {"Authorization": f"token {env_vars.gitea_token}"}
            resp = session.get(
                f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/files",
                headers=headers,
                timeout=60,
            )
            resp.raise_for_status()
            files_data = resp.json()
            filenames = [f["filename"] for f in files_data]

            has_all_meta = any("ALL_META.TXT.json" in f for f in filenames)
            has_class = any("CLASS.TXT.json" in f for f in filenames)
            has_required_files = has_all_meta and has_class

            for name in filenames:
                if name.startswith("docs/"):
                    parts = name.split("/")
                    if len(parts) > 2:
                        service_name = parts[1]
                        break

        except Exception as e:
            logging.error("Error fetching files for PR #%s: %s", pr_number, e)

        if has_required_files:
            has_child_timeline = has_child_pr(org, repo, pr_number)

        has_child = has_required_files and has_child_timeline

        prs_info.append({
            "pr_number": pr_number,
            "service_name": service_name or "Unknown",
            "pr_url": pr_url,
            "if_child": "Yes" if has_child else "No",
            "head_sha": head_sha or "Unknown"
        })

        logging.info(
            "PR #%s: files=%s, timeline=%s → If Child=%s",
            pr_number,
            "Yes" if has_required_files else "No",
            "Yes" if has_child_timeline else "No",
            "Yes" if has_child else "No"
        )

    logging.info("Collected %s PRs total", len(prs_info))
    return prs_info


def has_child_pr(org, repo, pr_number):
    headers = {"Authorization": f"token {env_vars.gitea_token}"}
    try:
        resp = session.get(
            f"{gitea_api_endpoint}/repos/{org}/{repo}/issues/{pr_number}/timeline",
            headers=headers,
            timeout=60
        )
        resp.raise_for_status()
        events = resp.json()

        for event in events:
            if event.get("type") != "pull_ref" or not event.get("ref_issue"):
                continue

            ref_issue = event["ref_issue"]
            body = ref_issue.get("body", "") or ""

            if not body.startswith("This is an automatically created Pull Request"):
                continue

            m = re.search(r"#(\d+)", body)
            if not m:
                continue

            referenced_number = int(m.group(1))
            if referenced_number == pr_number:
                logging.info("Timeline: PR #%s has pull_ref confirming child creation.", pr_number)
                return True
            else:
                logging.info("Timeline: PR #%s has pull_ref but references PR #%s.", pr_number, referenced_number)
                return False

        return False

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logging.info("Timeline not found for PR #%s (404).", pr_number)
            return False
        logging.error("HTTP error when checking timeline for PR #%s: %s", pr_number, e)
        return False
    except Exception as e:
        logging.error("Error checking timeline for PR #%s: %s", pr_number, e)
        return False


def update_child_status_from_timeline(conn, cur, org, repo, table_name):
    logging.info("Checking timeline for PRs with If Child = 'No'...")

    try:
        cur.execute(f'''SELECT "PR Number" FROM {table_name} WHERE "If Child" = 'No';''')
        prs_to_check = [row[0] for row in cur.fetchall()]
        logging.info("Found %s PRs to verify via timeline.", len(prs_to_check))

        for pr_number in prs_to_check:
            if has_child_pr(org, repo, pr_number):
                cur.execute(
                    f'''UPDATE {table_name}
                        SET "If Child" = 'Yes'
                        WHERE "PR Number" = %s;''',
                    (pr_number,)
                )
                conn.commit()
                logging.info("PR #%s: updated to Yes (child confirmed).", pr_number)
            else:
                logging.info("PR #%s: still No (no valid pull_ref found).", pr_number)

    except Exception as e:
        logging.error("Error during timeline check/update: %s", e)


def insert_prs(conn, cur, table_name, prs_info):
    try:
        for pr in prs_info:
            cur.execute(
                f'''INSERT INTO {table_name}
                ("PR Number", "Service Name", "PR URL", "If Child", "Head SHA")
                VALUES (%s, %s, %s, %s, %s);''',
                (pr["pr_number"], pr["service_name"], pr["pr_url"],
                 pr["if_child"], pr["head_sha"])
            )
        conn.commit()
        logging.info("Inserted %s PRs into %s", len(prs_info), table_name)
    except psycopg2.Error as e:
        logging.error("Error inserting PRs: %s", e)


def update_squad_info(cur, conn, rtc_table, target_table):
    logging.info("Updating squad information...")
    try:
        cur.execute(f"SELECT id, \"Service Name\" FROM {target_table};")
        rows = cur.fetchall()

        for row in rows:
            row_id = row[0]

            cur.execute(
                f"""UPDATE {target_table}
                    SET "Squad" = rtc."Squad",
                        "Service Name" = rtc."Title"
                    FROM {rtc_table} AS rtc
                    WHERE {target_table}."Service Name" = rtc."Service Type"
                    AND {target_table}.id = %s;""",
                (row_id,)
            )

        conn.commit()
        logging.info("Squad information updated successfully")

    except Exception as e:
        logging.error("Error updating squad information: %s", e)


def main(internal_org, rtc_table, missing_child_prs_table):
    repo = "doc-exports"

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    cur_csv.execute(f"DROP TABLE IF EXISTS {missing_child_prs_table}")
    conn_csv.commit()

    create_missing_child_prs_table(conn_csv, cur_csv, missing_child_prs_table)

    logging.info("Fetching open PRs from %s/%s...", internal_org, repo)
    open_prs = get_open_prs(internal_org, repo)

    if not open_prs:
        logging.warning("No open PRs found!")
        cur_csv.close()
        conn_csv.close()
        return

    logging.info("Collecting PRs and checking files...")
    prs_info = collect_prs_with_file_info(internal_org, repo, open_prs)

    insert_prs(conn_csv, cur_csv, missing_child_prs_table, prs_info)

    update_child_status_from_timeline(conn_csv, cur_csv, internal_org, repo, missing_child_prs_table)

    update_squad_info(cur_csv, conn_csv, rtc_table, missing_child_prs_table)

    logging.info("Successfully completed processing for %s!", internal_org)

    cur_csv.close()
    conn_csv.close()


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-------------------------MISSING CHILD PRS SCRIPT IS RUNNING-------------------------")

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    BASE_RTC_TABLE = "repo_title_category"
    BASE_MISSING_CHILD_TABLE = "missing_child_prs"

    cur_csv.execute('SELECT "Env Name", "Table Suffix", "Internal Org" FROM environments;')
    environments = cur_csv.fetchall()

    cur_csv.close()
    conn_csv.close()

    for env in environments:
        env_name = env[0]
        table_suffix = env[1]
        internal_org = env[2]

        rtc_table = f"{BASE_RTC_TABLE}{table_suffix}"
        missing_child_table = f"{BASE_MISSING_CHILD_TABLE}{table_suffix}"

        logging.info("Processing environment: %s, organization: %s, table: %s",
                     env_name, internal_org, missing_child_table)

        try:
            main(internal_org, rtc_table, missing_child_table)
        except Exception as e:
            logging.error("Error processing environment %s: %s", env_name, e)
            logging.info("Continuing with next environment...")
            continue

    logging.info("All environments processed successfully!")

    timer.stop()


if __name__ == "__main__":
    run()
