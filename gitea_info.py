import os
import requests
import json
import csv
import re
import pathlib
import base64
import psycopg2

gitea_api_endpoint = "https://gitea.eco.tsi-dev.otc-service.com/api/v1"
yaml_url = "https://gitea.eco.tsi-dev.otc-service.com/api/v1/repos/infra/otc-metadata/contents/%2Fotc_metadata%2Fdata%2Fservices.yaml?token="
session = requests.Session()
session.debug = False
org = "docs"
gitea_token = os.getenv("GITEA_TOKEN")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def csv_erase():
    proposalbot = pathlib.Path("proposalbot_prs.csv")
    docexports = pathlib.Path("doc_exports_prs.csv")
    orphaned = pathlib.Path("orphaned_prs.csv")
    if proposalbot.exists() is True:
        proposalbot.unlink()
    if docexports.exists() is True:
        docexports.unlink()
    if orphaned.exists() is True:
        orphaned.unlink()


def connect_to_db():
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )


def get_repos(org, gitea_token):
    repos = []
    page = 1
    while True:
        repos_resp = session.get(f"{gitea_api_endpoint}/orgs/{org}/repos?page={page}&limit=50&token={gitea_token}")
        if repos_resp.status_code == 200:
            repos_dict = json.loads(repos_resp.content.decode())
            for repo in repos_dict:
                repos.append(repo["name"])
            link_header = repos_resp.headers.get("Link")
            if link_header is None or "rel=\"next\"" not in link_header:
                break
            else:
                page += 1
        else:
            break
    return repos


def get_pull_requests(repo):
    states = ["open", "closed"]
    pull_requests = []
    csv_file = open("doc_exports_prs.csv", "a", newline="")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Parent PR index", "Parent PR title", "Parent PR URL", "Parent PR state", "If merged"])
    for state in states:
        page = 1
        while True:
            pull_requests_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state={state}&page={page}&limit=50&token={gitea_token}")
            if pull_requests_resp.status_code == 200:
                pull_requests = json.loads(pull_requests_resp.content.decode("utf-8"))
                for pr in pull_requests:
                    index = pr["number"]
                    title = pr["title"]
                    url = pr["url"]
                    state = pr["state"]
                    if_merged = pr["merged"]
                    csv_writer.writerow([index, title, url, state, if_merged])
                link_header = pull_requests_resp.headers.get("Link")
                if link_header is None or "rel=\"next\"" not in link_header:
                    break
                else:
                    page += 1
            else:
                break
    csv_file.close()
    return pull_requests


def get_parent_pr(repo):
    path = pathlib.Path("proposalbot_prs.csv")
    if path.exists() is False:
        csv_2 = open("proposalbot_prs.csv", "w")
        csv_writer = csv.writer(csv_2)
        csv_writer.writerow(["Parent PR number", "Name service", "Auto PR URL", "Auto PR State", "If merged"])
    else:
        csv_2 = open("proposalbot_prs.csv", "a")
        csv_writer = csv.writer(csv_2)

    if repo != "doc-exports":
        page = 1
        while True:
            repo_resp = session.get(f"{gitea_api_endpoint}/repos/{org}/{repo}/pulls?state=all&page={page}&limit=1000&token={gitea_token}")
            if repo_resp.status_code == 200:
                pull_request = json.loads(repo_resp.content.decode())

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
                            csv_writer.writerow([parent_pr, service, auto_url, auto_state, if_merged])

                link_header = repo_resp.headers.get("Link")
                if link_header is None or "rel=\"next\"" not in link_header:
                    break
                else:
                    page += 1
            else:
                break
    csv_2.close()


def extract_number_from_body(text):
    match = re.search(r"#\d+", text)
    if match:
        return int(match.group()[1:])
    return None


def create_prs_table(conn, cur, table_name):
    cur.execute(
        f'''CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        "Parent PR Number" INT,
        "Name Service" VARCHAR(255),
        "Squad" VARCHAR(255),
        "Auto PR URL" VARCHAR(255),
        "Auto PR State" VARCHAR(255),
        "If merged" BOOLEAN,
        "Parent PR State" VARCHAR(255),
        "Parent PR merged" BOOLEAN
        );'''
    )
    conn.commit()


def compare_csv_files(conn, cur):
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
    orphaned = []
    open_prs = []
    for pr1 in proposalbot_prs:
        for pr2 in doc_exports_prs:
            if pr1[0] == pr2[0] and pr1[4] != pr2[3]:
                if pr1 not in orphaned:
                    pr1.extend([pr2[3], pr2[4]])
                    orphaned.append(pr1)
                    cur.execute("""
                        INSERT INTO public.orphaned_prs
                        ("Parent PR Number", "Name Service", "Squad", "Auto PR URL", "Auto PR State", "If merged", "Parent PR State", "Parent PR merged")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, tuple(pr1))
                    conn.commit()
            elif pr1[0] == pr2[0] and pr1[4] == pr2[3] == "open":
                if pr1 not in open_prs:
                    pr1.extend([pr2[3], pr2[4]])
                    open_prs.append(pr1)
                    cur.execute("""
                        INSERT INTO public.open_prs
                        ("Parent PR Number", "Name Service", "Squad",  "Auto PR URL", "Auto PR State", "If merged", "Parent PR State", "Parent PR merged")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, tuple(pr1))
                    conn.commit()
    cur.close()
    conn.close()


def prepare_yaml(yaml_url, gitea_token):
    yaml_resp = session.get(f"{yaml_url}{gitea_token}")
    if yaml_resp.status_code == 200:
        yaml_dict = json.loads(yaml_resp.content.decode())
        content = yaml_dict["content"]
        raw = base64.b64decode(content)
        decode = (raw.decode())
        replace = decode.replace("\t", " " * 4).replace("- ", "  ").replace("repo: docs/", "").replace("repo: opentelekomcloud-docs/", "").replace("documents: ", "").replace("services:\n", "MARKER\n")
        services = re.sub("\s+teams|\s+environment|\s+type|\s+permission|\s+internal|\s+public|-rw|-ro\n|read|gitea|github|\s+write|[\s\S]*?(?=service_categories)|:", "", replace)
        return services
    else:
        print(f"Error retrieving services YAML: {yaml_resp.status_code} {yaml_resp.reason}")
        return None


def update_services_csv(yaml_services):
    pretty = re.search("[\s\S]*?(?=repositories)", yaml_services).group()
    pretty_services = {}
    pattern = re.compile(r"name\s*(?P<name>\S+\n)\s+title\s*(?P<title>[^\n]+)")
    matches = pattern.finditer(pretty)

    for match in matches:
        name = match.group("name")
        title = match.group("title")
        pretty_services[name] = title
    pretty_categories = {key.rstrip(): value for key, value in pretty_services.items()}
    return pretty_categories


def replace_category_names(yaml_services):
    pretty_categories = {
                        "bigdata-ai": "Big Data & AI",
                        "compute": "Compute",
                        "database": "Database",
                        "eco": "Eco",
                        "container": "Container",
                        "orchestration": "Orchestration",
                        "storage": "Storage",
                        "network": "Network",
                        "dashboard": "Dashboard",
                        "security-services": "Security Services"
                        }
    pattern = re.compile(r"^[\s\S]*?^MARKER\n", re.MULTILINE)
    cutted = re.sub(pattern, "", yaml_services)
    category_pattern = re.compile(r"(?<=name\sdocs-)(?P<cat>.*)")

    serv_string = cutted
    for match in category_pattern.finditer(cutted):
        category = match.group("cat").rstrip()
        if category in pretty_categories:
            serv_string = serv_string.replace(category, pretty_categories[category])

    return serv_string


def update_service_titles(cur):
    repo_title_category = fetch_repo_title_category(cur)

    with open("proposalbot_prs.csv", "r", newline="") as file:
        reader = csv.reader(file)
        rows = list(reader)
        header = rows.pop(0)
        for i, row in enumerate(rows):
            for (repo_id, repo, title, category) in repo_title_category:
                if repo == row[1]:
                    title_index = header.index("Name service")
                    row[title_index] = title

    with open("proposalbot_prs.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)


def add_squad_column(cur):
    repo_title_category = fetch_repo_title_category(cur)

    with open("proposalbot_prs.csv", "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
        header = rows.pop(0)
        header.insert(2, "Squad")
        for row in rows:
            name_service = row[1]
            for (repo_id, repo, title, category) in repo_title_category:
                if title == name_service:
                    row.insert(2, category)

    with open("proposalbot_prs.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def service_tuple(serv_string):
    repo_title_category = {}
    blocks = re.split("repositories", serv_string)
    for block in blocks:
        category_pattern = re.compile(r"(?<=name\sdocs-)(?P<cat>.*)")
        cat = category_pattern.search(block)
        if cat:
            category = cat.group("cat").strip()
        else:
            continue

        title_pattern = re.compile(r"service_title\s*(?P<title>[^\n]+)")
        tit = title_pattern.search(block)
        if tit:
            title = tit.group("title").strip()
        else:
            continue

        repo_pattern = re.compile(r"(?P<reposit>(?<=^\s{8}).*)")
        rep = repo_pattern.search(block)
        if rep:
            repository = rep.group("reposit").strip()
            repo_title_category[repository] = (title, category)
        else:
            continue
    repo_title_category["content-delivery-network"] = ("Content Delivery Network", "Other")
    repo_title_category["data-admin-service"] = ("Data Admin Service", "Other")

    return repo_title_category


def create_rtc_table(conn, cur, repo_title_category):
    cur.execute(
        f'''CREATE TABLE IF NOT EXISTS repo_title_category (
        id SERIAL PRIMARY KEY,
        "Repository" VARCHAR(255),
        "Title" VARCHAR(255),
        "Category" VARCHAR(255)
        );'''
    )
    conn.commit()
    for repo, (title, category) in repo_title_category.items():
        cur.execute(
            """
            INSERT INTO repo_title_category ("Repository", "Title", "Category")
            VALUES (%s, %s, %s);
            """,
            (repo, title, category)
        )
    conn.commit()


def fetch_repo_title_category(cur):
    cur.execute("SELECT * FROM repo_title_category")
    return cur.fetchall()


def main():
    csv_erase()

    conn = connect_to_db()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS open_prs, orphaned_prs, repo_title_category")
    conn.commit()

    create_prs_table(conn, cur, "open_prs")
    create_prs_table(conn, cur, "orphaned_prs")

    repos = get_repos(org, gitea_token)
    for repo in repos:
        get_parent_pr(repo)

    get_pull_requests("doc-exports")

    yaml_services = prepare_yaml(yaml_url, gitea_token)
    serv_string = replace_category_names(yaml_services)
    repo_title_category = service_tuple(serv_string)
    rtc_dict = {}
    for key, value in repo_title_category.items():
        new_key = key.lower().replace(" ", "-").replace("-services", "").replace("-ro read\n", "")
        rtc_dict[new_key] = value

    repo_title_category = rtc_dict
    create_rtc_table(conn, cur, repo_title_category)

    update_service_titles(cur)
    add_squad_column(cur)
    compare_csv_files(conn, cur)

    csv_erase()


if __name__ == "__main__":
    main()
