grafana-docs-monitoring
=======================
Scripts gathering HC3.0 PRs, issues and docs info from Github and Gitea. Scripts are works both for OTC and Swiss clouds in a one run. Data store in two Postgres databases: **_CSV_** is for a service tables, open PRs, failed Zuul PRs, issues and commits updates, **_ORPH_** is a dedicated DB special for orphan PRs

1) **otc_services_dict.py:** service script gathering metadata for service, its full names, categories and types. Should be run first, since all of the following scripts are relay on it in terms of repo names, service titles and squad names.
2) **gitea_info.py:** this script is using for open & orphan PRs data collecting
3) **github_info.py:** add info regarding child PRs in Github
4) **failed_zuul.py:** collecting info about PRs which checks in Zuul has been failed
5) **open_issues.py:** this script gather info regarding all open issues both from Gitea and Gitnub
6) **last_commit_info.py:** this script gather and calculate date of the last update of a prospective doc for all the services and types of docs (user manual, API reference and so on)

Postgres database names, table names, Gitea & Github organization names and access tokens are store in environment variables.
