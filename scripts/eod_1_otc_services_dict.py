"""
This script gather and process info about all services in OTC, both public and hybrid, process it and store in
service postgres tables, to match repo names, service full names and its squads
"""

import base64
import json
import logging

import psycopg2
import requests
import yaml

from config import Database, EnvVariables, Timer, setup_logging

BASE_URL = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
session = requests.Session()

env_vars = EnvVariables()
database = Database(env_vars)
gitea_token = env_vars.gitea_token


def create_rtc_table(conn_csv, cur_csv, table_name):
    logging.info("Creating new service table %s...", table_name)
    try:
        cur_csv.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Repository" VARCHAR(255),
            "Title" VARCHAR(255),
            "Category" VARCHAR(255),
            "Squad" VARCHAR(255),
            "Env" VARCHAR(255)
            );'''
        )
        conn_csv.commit()
    except Exception as e:
        logging.error("RTC: an error occurred while trying to create a table: %s", e)
        return


def create_doc_table(conn_csv, cur_csv, table_name):
    logging.info("Creating new doc table %s...", table_name)
    try:
        cur_csv.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Service Type" VARCHAR(255),
            "Title" VARCHAR(255),
            "Document Type" VARCHAR(255),
            "Link" VARCHAR(255)
            );'''
        )
        conn_csv.commit()
    except Exception as e:
        logging.error("Doc Table: an error occurred while trying to create a table: %s", e)


def get_pretty_category_names(base_dir, category_dir):
    response = requests.get(f"{BASE_URL}{category_dir}", timeout=10)
    response.raise_for_status()
    all_files = [item['path'] for item in response.json() if item['type'] == 'file']

    category_mapping = {}

    for file_path in all_files:
        if file_path.endswith('.yaml'):
            response = requests.get(f"{BASE_URL}{base_dir}{file_path}", timeout=10)
            response.raise_for_status()

            file_content_base64 = response.json()['content']
            file_content = base64.b64decode(file_content_base64).decode('utf-8')

            data_dict = yaml.safe_load(file_content)
            category_mapping[data_dict['name']] = data_dict['title']

    return category_mapping


def get_service_categories(base_dir, category_dir, services_dir, target_cloud=None):
    pretty_names = get_pretty_category_names(base_dir, category_dir)

    response = requests.get(f"{BASE_URL}{services_dir}", timeout=10)
    response.raise_for_status()
    all_files = [item['path'] for item in response.json() if item['type'] == 'file']

    all_data = []

    for file_path in all_files:
        if file_path.endswith('.yaml'):
            response = requests.get(f"{BASE_URL}{base_dir}{file_path}", timeout=10)
            response.raise_for_status()

            file_content_base64 = response.json()['content']
            file_content = base64.b64decode(file_content_base64).decode('utf-8')

            data_dict = yaml.safe_load(file_content)

            cloud_envs = data_dict.get("cloud_environments", [])

            if target_cloud:
                available_clouds = [env.get('name') for env in cloud_envs]
                if target_cloud not in available_clouds:
                    continue

                target_env = next((env for env in cloud_envs if env.get('name') == target_cloud), None)
                if target_env:
                    visibility = target_env.get('visibility', 'public')
                else:
                    continue
            else:
                if cloud_envs:
                    visibility = cloud_envs[0].get('visibility', 'public')
                else:
                    visibility = 'public'

            data_dict['target_visibility'] = visibility

            technical_name = data_dict.get('service_category')
            data_dict['service_category'] = pretty_names.get(technical_name, technical_name)
            teams = data_dict.get('teams', [])
            if teams:
                squad_name = teams[0].get('name', '')
                data_dict['squad'] = squad_name
            else:
                data_dict['squad'] = ''

            all_data.append(data_dict)

    return all_data


def get_docs_info(base_dir, doc_dir):
    response = requests.get(f"{BASE_URL}{doc_dir}", timeout=10)
    response.raise_for_status()
    all_files = [item['path'] for item in response.json() if item['type'] == 'file']

    all_data = []

    for file_path in all_files:
        if file_path.endswith('.yaml'):
            response = requests.get(f"{BASE_URL}{base_dir}{file_path}", timeout=10)
            response.raise_for_status()

            file_content_base64 = response.json()['content']
            file_content = base64.b64decode(file_content_base64).decode('utf-8')

            data_dict = yaml.safe_load(file_content)
            all_data.append(data_dict)

    return all_data


def get_tech_repos(cur_csv, rtc_table):
    headers = {
        "Authorization": f"token {env_vars.gitea_token}"
    }
    tech_repos = []

    try:
        cur_csv.execute(f"SELECT DISTINCT \"Repository\" FROM {rtc_table} WHERE \"Env\" IN ('internal', 'hidden',"
                        f"'public');")
        exclude_repos = [row[0] for row in cur_csv.fetchall()]

    except Exception as e:
        logging.error("Fetching exclude repos for internal services: %s", e)
        return exclude_repos

    max_pages = 50
    page = 1

    if rtc_table == "repo_title_category":
        org = "docs"
    else:
        org = "docs-swiss"
    while True:
        try:
            repos_resp = session.get(f"{BASE_URL}/orgs/{org}/repos?page={page}&limit=50", headers=headers)
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
            tech_repos.append(repo["name"])

        if page > max_pages:
            logging.warning("Reached maximum page limit for docs")
            break

        link_header = repos_resp.headers.get("Link")
        if link_header is None or "rel=\"next\"" not in link_header:
            break
        page += 1
    logging.info("%s repos have been processed", len(tech_repos))
    return tech_repos


def insert_services_data(item, conn_csv, cur_csv, table_name):
    if not isinstance(item, dict):
        logging.error("Unexpected data type: %s, value: %s", type(item), item)
        return

    insert_query = f"""INSERT INTO {table_name} ("Repository", "Title", "Category", "Squad", "Env")
                      VALUES (%s, %s, %s, %s, %s);"""

    repository = item.get("service_uri")
    title = item.get("service_title")
    category = item.get("service_category")
    squad = item.get("squad")

    senv = item.get("target_visibility")

    cur_csv.execute(insert_query, (repository, title, category, squad, senv))
    conn_csv.commit()


def insert_tech_repos_data(conn_csv, cur_csv, tech_repo, table_name):
    insert_query = f"""INSERT INTO {table_name} ("Repository", "Title", "Category", "Squad", "Env")
                      VALUES (%s, %s, %s, %s, %s);"""

    repository = tech_repo
    title = tech_repo
    if tech_repo in ("doc-exports", "doc-convertor", "docsportal"):
        category = "Docs"
        squad = "Huawei"
    else:
        category = "Tech"
        squad = "Tech"
    senv = "tech"

    cur_csv.execute(insert_query, (repository, title, category, squad, senv))
    conn_csv.commit()


def get_squad_description(styring_url):
    response = requests.get(styring_url, timeout=10)
    response.raise_for_status()

    file_content_base64 = response.json()['content']
    file_content = base64.b64decode(file_content_base64).decode('utf-8')

    data = yaml.safe_load(file_content)

    return {item['slug']: item['description'] for item in data['teams']}


def update_squad_title(conn, styring_url, table_name):
    descriptions = get_squad_description(styring_url)

    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT \"Squad\" FROM {table_name};")
    squads = cur.fetchall()

    for (squad,) in squads:
        description = descriptions.get(squad)
        if description:
            cur.execute(f"UPDATE {table_name} SET \"Squad\" = %s WHERE \"Squad\" = %s;", (description, squad))

    conn.commit()
    cur.close()


def insert_docs_data(item, conn_csv, cur_csv, table_name):
    if not isinstance(item, dict):
        logging.error("Unexpected data type: %s, value: %s", type(item), item)
        return

    insert_query = f"""INSERT INTO {table_name} ("Service Type", "Title", "Document Type", "Link")
                      VALUES (%s, %s, %s, %s);"""

    stype = item.get("service_type")
    title = item.get("title")
    dtype = item.get("type")
    link = item.get("link") + "source"

    cur_csv.execute(insert_query, (stype, title, dtype, link))
    conn_csv.commit()


def add_obsolete_services(conn_csv, cur_csv, rtc_table):
    data_to_insert = [
        {"service_uri": "content-delivery-network", "service_title": "Content Delivery Network", "service_category":
            "Other", "service_type": "cdn", "squad": "Other", "target_visibility": "hidden"}
    ]

    for item in data_to_insert:
        insert_services_data(item, conn_csv, cur_csv, rtc_table)


def copy_rtc(cur_csv, cursors, conns, rtctable):
    logging.info("Start copy %s to other DBs...", rtctable)
    try:
        cur_csv.execute(f"SELECT * FROM {rtctable};")
    except psycopg2.Error as e:
        logging.error("Error fetching data from %s: %s", rtctable, e)
        return

    rows = cur_csv.fetchall()
    columns = [desc[0] for desc in cur_csv.description]
    columns_quoted = [f'"{col}"' for col in columns]
    for conn, cur in zip(conns, cursors):
        try:
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {rtctable} (
            {', '.join(['%s text' % col for col in columns_quoted])}
            );
            """)
            for row in rows:
                placeholders = ', '.join(['%s'] * len(row))
                cur.execute(f"INSERT INTO {rtctable} VALUES ({placeholders});", row)
            conn.commit()
        except psycopg2.Error as e:
            logging.error("Error copying data to %s in target DB: %s", rtctable, e)
            conn.rollback()


def main(base_dir, rtctable, doctable, styring_path, target_cloud=None):
    services_dir = f"{base_dir}otc_metadata/data/services"
    category_dir = f"{base_dir}otc_metadata/data/service_categories"
    doc_dir = f"{base_dir}otc_metadata/data/documents"
    styring_url = f"{BASE_URL}{styring_path}{env_vars.gitea_token}"

    conn_orph = database.connect_to_db(env_vars.db_orph)
    cur_orph = conn_orph.cursor()

    conn_zuul = database.connect_to_db(env_vars.db_zuul)
    cur_zuul = conn_zuul.cursor()

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()

    conns = [conn_orph, conn_zuul]
    cursors = [cur_orph, cur_zuul]

    cur_csv.execute(f"DROP TABLE IF EXISTS {rtctable}, {doctable}")
    conn_csv.commit()
    for conn, cur in zip(conns, cursors):
        cur.execute(f"DROP TABLE IF EXISTS {rtctable}, {doctable}")
        conn.commit()

    all_data = get_service_categories(base_dir, category_dir, services_dir, target_cloud)
    create_rtc_table(conn_csv, cur_csv, rtctable)
    for data in all_data:
        insert_services_data(data, conn_csv, cur_csv, rtctable)

    update_squad_title(conn_csv, styring_url, rtctable)

    create_doc_table(conn_csv, cur_csv, doctable)
    all_doc_data = get_docs_info(base_dir, doc_dir)
    for doc_data in all_doc_data:
        insert_docs_data(doc_data, conn_csv, cur_csv, doctable)

    tech_repos = get_tech_repos(cur_csv, rtctable)
    print("TECH REPOS________________________________________", rtctable, tech_repos)
    for tech_repo in tech_repos:
        insert_tech_repos_data(conn_csv, cur_csv, tech_repo, rtctable)
    copy_rtc(cur_csv, cursors, conns, rtctable)

    for conn in conns:
        conn.close()
    conn_csv.close()


def run():
    timer = Timer()
    timer.start()

    setup_logging()

    logging.info("-------------------------OTC SERVICES DICT SCRIPT IS RUNNING-------------------------")

    BASE_DIR_UNIFIED = "/repos/infra/otc-metadata-rework/contents/"
    STYRING_URL_REGULAR = "/repos/infra/gitstyring/contents/data/github/orgs/opentelekomcloud-docs/data.yaml?token="
    STYRING_URL_SWISS = "/repos/infra/gitstyring/contents/data/github/orgs/opentelekomcloud-docs-swiss/data.yaml?token="
    BASE_RTC_TABLE = "repo_title_category"
    BASE_DOC_TABLE = "doc_types"

    main(BASE_DIR_UNIFIED, BASE_RTC_TABLE, BASE_DOC_TABLE, STYRING_URL_REGULAR, target_cloud="eu_de")

    main(BASE_DIR_UNIFIED, f"{BASE_RTC_TABLE}_swiss", f"{BASE_DOC_TABLE}_swiss", STYRING_URL_SWISS,
         target_cloud="swiss")

    conn_csv = database.connect_to_db(env_vars.db_csv)
    cur_csv = conn_csv.cursor()
    add_obsolete_services(conn_csv, cur_csv, BASE_RTC_TABLE)

    conn_csv.commit()
    conn_csv.close()

    timer.stop()


if __name__ == "__main__":
    run()
