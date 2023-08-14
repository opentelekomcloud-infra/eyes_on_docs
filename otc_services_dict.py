import os
import requests
import yaml
import base64
import psycopg2

BASE_URL = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"

TOKEN = os.getenv("GITEA_TOKEN")
headers = {
    "Authorization": f"token {TOKEN}"
}

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_CSV")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


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


def create_rtc_table(conn, cur, table_name):
    print(f"Creating new service table {table_name}...")
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Repository" VARCHAR(255),
            "Title" VARCHAR(255),
            "Category" VARCHAR(255),
            "Type" VARCHAR(255)
            );'''
        )
        conn.commit()
    except Exception as e:
        print(f"RTC: an error occurred while trying to create a table: {e}")
        return


def create_doc_table(conn, cur, table_name):
    print(f"Creating new doc table {table_name}...")
    try:
        cur.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Service Type" VARCHAR(255),
            "Title" VARCHAR(255),
            "Document Type" VARCHAR(255),
            "Link" VARCHAR(255)
            );'''
        )
        conn.commit()
    except Exception as e:
        print(f"Doc Table: an error occurred while trying to create a table: {e}")


def get_pretty_category_names(base_dir, category_dir):
    """Retrieve pretty names for categories."""
    response = requests.get(f"{BASE_URL}{category_dir}", headers=headers)
    response.raise_for_status()
    all_files = [item['path'] for item in response.json() if item['type'] == 'file']

    category_mapping = {}

    for file_path in all_files:
        if file_path.endswith('.yaml'):
            response = requests.get(f"{BASE_URL}{base_dir}{file_path}", headers=headers)
            response.raise_for_status()

            file_content_base64 = response.json()['content']
            file_content = base64.b64decode(file_content_base64).decode('utf-8')

            data_dict = yaml.safe_load(file_content)
            category_mapping[data_dict['name']] = data_dict['title']

    return category_mapping


def get_service_categories(base_dir, category_dir, services_dir):
    """Fetch all .yaml files from a Gitea directory and return their data after mapping to pretty category names."""
    pretty_names = get_pretty_category_names(base_dir, category_dir)

    response = requests.get(f"{BASE_URL}{services_dir}", headers=headers)
    response.raise_for_status()
    all_files = [item['path'] for item in response.json() if item['type'] == 'file']

    all_data = []

    for file_path in all_files:
        if file_path.endswith('.yaml'):
            response = requests.get(f"{BASE_URL}{base_dir}{file_path}", headers=headers)
            response.raise_for_status()

            file_content_base64 = response.json()['content']
            file_content = base64.b64decode(file_content_base64).decode('utf-8')

            data_dict = yaml.safe_load(file_content)
            technical_name = data_dict.get('service_category')
            data_dict['service_category'] = pretty_names.get(technical_name, technical_name)

            all_data.append(data_dict)

    return all_data


def get_docs_info(base_dir, doc_dir):
    """Fetch all .yaml files from a Gitea directory and return their data."""
    response = requests.get(f"{BASE_URL}{doc_dir}", headers=headers)
    response.raise_for_status()
    all_files = [item['path'] for item in response.json() if item['type'] == 'file']

    all_data = []

    for file_path in all_files:
        if file_path.endswith('.yaml'):
            response = requests.get(f"{BASE_URL}{base_dir}{file_path}", headers=headers)
            response.raise_for_status()

            file_content_base64 = response.json()['content']
            file_content = base64.b64decode(file_content_base64).decode('utf-8')

            data_dict = yaml.safe_load(file_content)
            all_data.append(data_dict)

    return all_data


def insert_services_data(item, conn, cur, table_name):
    """Insert gathered services metadata and its categories into Postgres table."""
    if not isinstance(item, dict):
        print(f"Unexpected data type: {type(item)}, value: {item}")
        return

    insert_query = f"""INSERT INTO {table_name} ("Repository", "Title", "Category", "Type")
                      VALUES (%s, %s, %s, %s);"""

    repository = item.get("service_uri")
    title = item.get("service_title")
    category = item.get("service_category")
    stype = item.get("service_type")

    cur.execute(insert_query, (repository, title, category, stype))

    conn.commit()


def insert_docs_data(item, conn, cur, table_name):
    """Insert gathered docs metadata and its types into Postgres table."""
    if not isinstance(item, dict):
        print(f"Unexpected data type: {type(item)}, value: {item}")
        return

    insert_query = f"""INSERT INTO {table_name} ("Service Type", "Title", "Document Type", "Link")
                      VALUES (%s, %s, %s, %s);"""

    stype = item.get("service_type")
    title = item.get("title")
    dtype = item.get("type")
    link = item.get("link") + "source"

    cur.execute(insert_query, (stype, title, dtype, link))
    conn.commit()


def add_obsolete_services(conn, cur):
    """Add obsolete services and its categories to the Postgres table for public cloud"""
    data_to_insert = [
        {"service_uri": "content-delivery-network", "service_title": "Content Delivery Network", "service_category": "Other", "service_type": "cdn"},
        {"service_uri": "data-admin-service", "service_title": "Data Admin Service", "service_category": "Other", "service_type": "das"}
    ]

    for item in data_to_insert:
        insert_services_data(item, conn, cur, "repo_title_category")


def main(base_dir, rtc_table, doc_table):
    cur.execute(f"DROP TABLE IF EXISTS {rtc_table}, {doc_table}")
    conn.commit()

    services_dir = f"{base_dir}otc_metadata/data/services"
    category_dir = f"{base_dir}otc_metadata/data/service_categories"
    doc_dir = f"{base_dir}otc_metadata/data/documents"

    all_data = get_service_categories(base_dir, category_dir, services_dir)

    create_rtc_table(conn, cur, rtc_table)

    for data in all_data:
        insert_services_data(data, conn, cur, rtc_table)

    create_doc_table(conn, cur, doc_table)
    all_doc_data = get_docs_info(base_dir, doc_dir)
    for doc_data in all_doc_data:
        insert_docs_data(doc_data, conn, cur, doc_table)


if __name__ == "__main__":
    conn = connect_to_db(db_name)
    cur = conn.cursor()
    base_dir_swiss = "/repos/infra/otc-metadata-swiss/contents/"
    base_dir_regular = "/repos/infra/otc-metadata/contents/"
    base_rtc_table = "repo_title_category"
    base_doc_table = "doc_types"

    main(base_dir_swiss, f"{base_rtc_table}_swiss", f"{base_doc_table}_swiss")
    main(base_dir_regular, base_rtc_table, base_doc_table)
    add_obsolete_services(conn, cur)

    cur.close()
    conn.close()
