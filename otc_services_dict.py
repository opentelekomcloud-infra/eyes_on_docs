import os
import requests
import yaml
import base64
import psycopg2

BASE_URL = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"

gitea_token = os.getenv("GITEA_TOKEN")
headers = {
    "Authorization": f"token {gitea_token}"
}

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_csv = os.getenv("DB_CSV")
db_orph = os.getenv("DB_ORPH")
db_zuul = os.getenv("DB_ZUUL")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def connect_to_db(db):
    print(f"Connecting to Postgres ({db})...")
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db,
            user=db_user,
            password=db_password
        )
    except psycopg2.Error as e:
        print(f"Connecting to Postgres: an error occurred while trying to connect to the database: {e}")
        return None


def create_rtc_table(conn_csv, cur_csv, table_name):
    print(f"Creating new service table {table_name}...")
    try:
        cur_csv.execute(
            f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            "Repository" VARCHAR(255),
            "Title" VARCHAR(255),
            "Category" VARCHAR(255),
            "Type" VARCHAR(255)
            );'''
        )
        conn_csv.commit()
    except Exception as e:
        print(f"RTC: an error occurred while trying to create a table: {e}")
        return


def create_doc_table(conn_csv, cur_csv, table_name):
    print(f"Creating new doc table {table_name}...")
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
        print(f"Doc Table: an error occurred while trying to create a table: {e}")


def get_pretty_category_names(base_dir, category_dir):
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


def insert_services_data(item, conn_csv, cur_csv, table_name):
    if not isinstance(item, dict):
        print(f"Unexpected data type: {type(item)}, value: {item}")
        return

    insert_query = f"""INSERT INTO {table_name} ("Repository", "Title", "Category", "Type")
                      VALUES (%s, %s, %s, %s);"""

    repository = item.get("service_uri")
    title = item.get("service_title")
    category = item.get("service_category")
    stype = item.get("service_type")

    cur_csv.execute(insert_query, (repository, title, category, stype))

    conn_csv.commit()


def insert_docs_data(item, conn_csv, cur_csv, table_name):
    if not isinstance(item, dict):
        print(f"Unexpected data type: {type(item)}, value: {item}")
        return

    insert_query = f"""INSERT INTO {table_name} ("Service Type", "Title", "Document Type", "Link")
                      VALUES (%s, %s, %s, %s);"""

    stype = item.get("service_type")
    title = item.get("title")
    dtype = item.get("type")
    link = item.get("link") + "source"

    cur_csv.execute(insert_query, (stype, title, dtype, link))
    conn_csv.commit()


def add_obsolete_services(conn_csv, cur_csv):
    data_to_insert = [
        {"service_uri": "content-delivery-network", "service_title": "Content Delivery Network", "service_category": "Other", "service_type": "cdn"},
        {"service_uri": "data-admin-service", "service_title": "Data Admin Service", "service_category": "Other", "service_type": "das"}
    ]

    for item in data_to_insert:
        insert_services_data(item, conn_csv, cur_csv, "repo_title_category")


def copy_rtc(cur_csv, cursors, conns, rtctable):
    print(f"Start copy {rtctable} to other DBs...")
    try:
        cur_csv.execute(f"SELECT * FROM {rtctable};")
    except psycopg2.Error as e:
        print(f"Error fetching data from {rtctable}: {e}")
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
            print(f"Error copying data to {rtctable} in target DB: {e}")
            conn.rollback()



def main(base_dir, rtctable, doctable):
    services_dir = f"{base_dir}otc_metadata/data/services"
    category_dir = f"{base_dir}otc_metadata/data/service_categories"
    doc_dir = f"{base_dir}otc_metadata/data/documents"

    conn_orph = connect_to_db(db_orph)
    cur_orph = conn_orph.cursor()

    conn_zuul = connect_to_db(db_zuul)
    cur_zuul = conn_zuul.cursor()

    conn_csv = connect_to_db(db_csv)
    cur_csv = conn_csv.cursor()

    conns = [conn_orph, conn_zuul]
    cursors = [cur_orph, cur_zuul]

    cur_csv.execute(f"DROP TABLE IF EXISTS {rtctable}, {doctable}")
    conn_csv.commit()
    for conn, cur in zip(conns, cursors):
        cur.execute(f"DROP TABLE IF EXISTS {rtctable}, {doctable}")
        conn.commit()

    all_data = get_service_categories(base_dir, category_dir, services_dir)
    create_rtc_table(conn_csv, cur_csv, rtctable)
    for data in all_data:
        insert_services_data(data, conn_csv, cur_csv, rtctable)

    create_doc_table(conn_csv, cur_csv, doctable)
    all_doc_data = get_docs_info(base_dir, doc_dir)
    for doc_data in all_doc_data:
        insert_docs_data(doc_data, conn_csv, cur_csv, doctable)

    copy_rtc(cur_csv, cursors, conns, rtctable)

    for conn in conns:
        conn.close()
    conn_csv.close()


if __name__ == "__main__":
    base_dir_swiss = "/repos/infra/otc-metadata-swiss/contents/"
    base_dir_regular = "/repos/infra/otc-metadata/contents/"
    base_rtc_table = "repo_title_category"
    base_doc_table = "doc_types"

    main(base_dir_regular, base_rtc_table, base_doc_table)
    main(base_dir_swiss, f"{base_rtc_table}_swiss", f"{base_doc_table}_swiss")
    conn_csv = connect_to_db(db_csv)
    cur_csv = conn_csv.cursor()
    add_obsolete_services(conn_csv, cur_csv)
    conn_csv.commit()
    conn_csv.close()
