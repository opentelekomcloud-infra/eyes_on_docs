""" This script uses async logic for faster evaluation"""

import asyncio
import logging
import os
from datetime import datetime
from typing import List, Dict

import aiohttp # type: ignore
import psycopg2
import psycopg2.extras

from config import Database, EnvVariables, Timer, setup_logging

# Async conf
MAX_CONCURRENT_REQUESTS = 20
BATCH_SIZE = 100
REQUEST_DELAY = 0.1  # Rate limiting delay
REQUEST_TIMEOUT = 30

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
env_vars = EnvVariables()
database = Database(env_vars)

TEXT_EXTENSIONS = {".py", ".js", ".ts", ".json", ".yaml", ".yml", ".md", ".rst", ".txt", ".sh", ".ini", ".conf"}
LABELS = {"on hold", "new_service", "broken_pr_huawei", "broken_pr_eco"}


class OptimizedAPIClient:
    def __init__(self):
        self.session = None
        self.headers = {"Authorization": f"token {env_vars.gitea_token}"}
        self.semaphore = None

    async def __aenter__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=20)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self.headers
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_with_rate_limit(self, url: str) -> dict:
        async with self.semaphore:
            for attempt in range(3):  # Retry до 3 раз
                try:
                    await asyncio.sleep(REQUEST_DELAY)  # Rate limiting
                    async with self.session.get(url) as response:
                        if response.status == 429:  # Rate limit exceeded
                            wait_time = 2 ** attempt
                            logging.warning(f"Rate limit hit, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                        response.raise_for_status()
                        return await response.json()
                except Exception as e:
                    if attempt == 2:  # Последняя попытка
                        logging.error(f"Failed to fetch {url}: {e}")
                        return {}
                    await asyncio.sleep(2 ** attempt)
        return {}

    async def fetch_text_with_rate_limit(self, url: str) -> str:
        async with self.semaphore:
            for attempt in range(3):
                try:
                    await asyncio.sleep(REQUEST_DELAY)
                    async with self.session.get(url) as response:
                        if response.status == 429:
                            wait_time = 2 ** attempt
                            logging.warning(f"Rate limit hit, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                        response.raise_for_status()
                        return await response.text()
                except Exception as e:
                    if attempt == 2:
                        logging.error(f"Failed to fetch text {url}: {e}")
                        return ""
                    await asyncio.sleep(2 ** attempt)
        return ""


async def gather_prs_async(org: str, repos: List[str], client: OptimizedAPIClient) -> List[Dict]:
    all_prs = []

    async def fetch_repo_prs(repo: str):
        url = f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state=open&page=1"
        prs_data = await client.fetch_with_rate_limit(url)

        repo_prs = []
        for pr in prs_data:
            body = pr.get("body", "")
            pr_labels = pr.get("labels", [])
            label_names = {label["name"] for label in pr_labels}

            if LABELS & label_names:
                continue

            if body.startswith("This is an automatically created Pull Request"):
                created_at = datetime.fromisoformat(pr.get("created_at").replace('Z', '+00:00')).date()
                days_passed = (datetime.utcnow().date() - created_at).days

                repo_prs.append({
                    "number": pr.get("number"),
                    "repo": repo,
                    "url": pr.get("url"),
                    "days_passed": days_passed,
                    "files_count": pr.get("changed_files", 0)
                })
        return repo_prs

    tasks = [fetch_repo_prs(repo) for repo in repos]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, list):
            all_prs.extend(result)
        else:
            logging.error(f"Error fetching repo PRs: {result}")

    return all_prs


async def get_pr_files_async(org: str, prs: List[Dict], client: OptimizedAPIClient) -> List[Dict]:
    async def fetch_pr_files(pr: Dict):
        repo = pr["repo"]
        pr_number = pr["number"]
        page = 1
        files = []

        while True:
            url = f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls/{pr_number}/files?page={page}"
            files_data = await client.fetch_with_rate_limit(url)

            if not files_data:
                break

            for file in files_data:
                if file["status"] == "deleted":
                    continue

                file_url = file["raw_url"]
                _, ext = os.path.splitext(file_url)

                files.append({
                    "repo": repo,
                    "pr_number": pr_number,
                    "file_url": file_url,
                    "is_text": ext.lower() in TEXT_EXTENSIONS
                })

            if len(files_data) < 50:
                break
            page += 1

        return files

    tasks = [fetch_pr_files(pr) for pr in prs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_files = []
    for result in results:
        if isinstance(result, list):
            all_files.extend(result)
        else:
            logging.error(f"Error fetching PR files: {result}")

    return all_files


async def count_lines_async(files: List[Dict], client: OptimizedAPIClient) -> List[Dict]:

    async def count_file_lines(file: Dict):
        if not file["is_text"]:
            file["lines_count"] = 1
            return file

        content = await client.fetch_text_with_rate_limit(file["file_url"])
        file["lines_count"] = len(content.splitlines()) if content else 0
        return file

    text_files = [f for f in files if f["is_text"]]
    non_text_files = [f for f in files if not f["is_text"]]

    for file in non_text_files:
        file["lines_count"] = 1

    processed_files = []
    for i in range(0, len(text_files), BATCH_SIZE):
        batch = text_files[i:i + BATCH_SIZE]
        tasks = [count_file_lines(file) for file in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in batch_results:
            if isinstance(result, dict):
                processed_files.append(result)
            else:
                logging.error(f"Error counting lines: {result}")

    return processed_files + non_text_files


def batch_insert_to_db(conn, cur, data: List[Dict], table: str, columns: List[str]):
    if not data:
        return

    values = []
    for item in data:
        values.append(tuple(item.get(col, None) for col in columns))

    escaped_columns = [f'"{col}"' for col in columns]
    query = f"INSERT INTO {table} ({', '.join(escaped_columns)}) VALUES %s"

    try:
        psycopg2.extras.execute_values(cur, query, values, page_size=BATCH_SIZE)
        conn.commit()
        logging.info(f"Inserted {len(values)} records into {table}")
    except Exception as e:
        logging.error(f"Error batch inserting into {table}: {e}")
        conn.rollback()


async def main_async(org: str, rtc: str, fil_lin_tab: str, temp_tab: str):
    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    try:
        cur_csv.execute(f"DROP TABLE IF EXISTS {fil_lin_tab}")
        cur_csv.execute(f"DROP TABLE IF EXISTS {temp_tab}")
        conn_csv.commit()

        create_temp_table(conn_csv, cur_csv, temp_tab)
        create_prs_table(conn_csv, cur_csv, fil_lin_tab)

        repos = get_repos(cur_csv, rtc)
        logging.info(f"Processing {len(repos)} repositories...")

        async with OptimizedAPIClient() as client:
            logging.info("Gathering PRs...")
            all_prs = await gather_prs_async(org, repos, client)
            logging.info(f"Found {len(all_prs)} PRs")

            if not all_prs:
                logging.info("No PRs found")
                return

            pr_columns = ["PR Number", "Service Name", "PR URL", "Days passed", "Files count", "Lines count"]
            pr_data = []
            for pr in all_prs:
                pr_data.append({
                    "PR Number": pr["number"],
                    "Service Name": pr["repo"],
                    "PR URL": pr["url"],
                    "Days passed": pr["days_passed"],
                    "Files count": pr["files_count"],
                    "Lines count": 0
                })

            batch_insert_to_db(conn_csv, cur_csv, pr_data, fil_lin_tab, pr_columns)

            logging.info("Gathering PR files...")
            all_files = await get_pr_files_async(org, all_prs, client)
            logging.info(f"Found {len(all_files)} files")

            logging.info("Counting lines in files...")
            processed_files = await count_lines_async(all_files, client)

            temp_columns = ["repo", "pr_number", "file_url", "lines_count"]
            temp_data = []
            for file in processed_files:
                temp_data.append({
                    "repo": file["repo"],
                    "pr_number": file["pr_number"],
                    "file_url": file["file_url"],
                    "lines_count": file["lines_count"]
                })

            batch_insert_to_db(conn_csv, cur_csv, temp_data, temp_tab, temp_columns)

            aggregate_lines_count(conn_csv, cur_csv, temp_tab, fil_lin_tab)
            update_squad_and_title(conn_csv, cur_csv, rtc, fil_lin_tab)

    finally:
        cur_csv.close()
        conn_csv.close()


def run():
    timer = Timer()
    timer.start()

    setup_logging()
    logging.info("-----ASYNC HUAWEI FILES AND LINES SCRIPT IS RUNNING-----")

    org_string = "docs"
    rtc_table = "repo_title_category"
    files_lines_table = "huawei_files_lines"
    temp_table = "temp_huawei_files_lines"

    asyncio.run(main_async(org_string, rtc_table, files_lines_table, temp_table))
    asyncio.run(main_async(f"{org_string}-swiss", f"{rtc_table}_swiss",
                           f"{files_lines_table}_swiss", f"{temp_table}_swiss"))

    timer.stop()
    logging.info("Async Huawei filles-lines script completed successfully!")


def create_prs_table(conn, cur, table_name):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "PR Number" INT,
            "Service Name" VARCHAR(255),
            "Squad" VARCHAR(255),
            "PR URL" VARCHAR(255),
            "Days passed" INT,
            "Files count" INT,
            "Lines count" INT
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", table_name)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      table_name, e)


def create_temp_table(conn, cur, temp_tab):
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {temp_tab} (
            id SERIAL PRIMARY KEY,
            repo VARCHAR(255),
            pr_number INT,
            file_url TEXT,
            lines_count INT
            );'''
        )
        conn.commit()
        logging.info("Table %s has been created successfully", temp_tab)
    except psycopg2.Error as e:
        logging.error("Tables creating: an error occurred while trying to create a table %s in the database: %s",
                      temp_tab, e)


def get_repos(cur, rtc):
    repos = []
    try:
        cur.execute(f"SELECT DISTINCT \"Repository\" FROM {rtc} WHERE \"Env\" IN ('public');")
        repos = [row[0] for row in cur.fetchall()]
        if not repos:
            logging.info("No repositories found.")
    except Exception as e:
        logging.error("Fetching repos: %s", e)
    return repos


def aggregate_lines_count(conn, cur, temp_tab, fil_lin_tab):
    cur.execute(f"""
        UPDATE {fil_lin_tab} AS main
        SET "Lines count" = subquery.total_lines
        FROM (
            SELECT pr_number, repo, SUM(lines_count) AS total_lines
            FROM {temp_tab}
            GROUP BY pr_number, repo
        ) AS subquery
        WHERE main."PR Number" = subquery.pr_number
          AND main."Service Name" = subquery.repo;
    """)
    conn.commit()
    logging.info("Updated line counts in PR table")


def update_squad_and_title(conn, cur, rtc, fil_lin_tab):
    logging.info("Updating squads and titles...")
    try:
        cur.execute(f"SELECT * FROM {fil_lin_tab};")
        open_issues_rows = cur.fetchall()

        for row in open_issues_rows:
            cur.execute(
                f"""UPDATE {fil_lin_tab}
                    SET "Service Name" = rtc."Title", "Squad" = rtc."Squad"
                    FROM {rtc} AS rtc
                    WHERE {fil_lin_tab}."Service Name" = rtc."Repository"
                    AND {fil_lin_tab}.id = %s;""",
                (row[0],)
            )
        conn.commit()
    except Exception as e:
        logging.error("Error updating squad and title: %s", e)


if __name__ == "__main__":
    run()
