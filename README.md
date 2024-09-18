Eyes-On-Docs
============
************
Scripts gathering HC3.0 PRs, issues and docs info from Github and Gitea. Scripts are works both for OTC and Swiss clouds in a one run. Data store in three Postgres databases: **_CSV_** is for a service tables and open PRs, failed Zuul PRs, issues and commits updates, **_ORPH_** is a dedicated DB special for orphan PRs

1) **eod_1_otc_services_dict.py:** service script gathering metadata for service, its full names, categories and types. 
   Should be run first, since all of the following scripts are relay on it in terms of repo names, service titles and squad names.
2) **eod_2_gitea_info.py:** this script is using for open & orphan PRs data collecting
3) **eod_3_github_info.py:** add info regarding child PRs in Github
4) **eod_4_failed_zuul.py:** collecting info about PRs which checks in Zuul has been failed
5) **eod_5_open_issues.py:** this script gather info regarding all open issues both from Gitea and Gitnub
6) **eod_6_last_commit_info.py:** this script gather and calculate date of the last update of a prospective doc for all 
   the services and types of docs (user manual, API reference and so on)
7) **eod_7_request_changes.py:** this script gather info about PRs where changes has been requested, from both 
   sides: OTC squads and upstream team, which is generating documentation
8) **eod_8_ecosystem_issue.py:** script for gathering issues for Eco Squad only
9) **eod_9_scheduler.py:** this script checking postgres for orphans, unattended issues and outdated docs, and send 
   notifications to Zulip, via OTC Bot. Runs as cronjob (see 'system-config' repo)
Postgres database names, table names, Gitea & Github organization names and access tokens are store in environment variables.

Notification schedule
---------------------
*********************
We have 3 types of alerts: Orphaned PRs Count, Unattended Issues and Outdated Documentation Release Dates.\
**Orphaned PRs Count** checks comes every day.\
**Unattended Issues** appears after issues remain unassigned for 7 days.\
**Outdated Documentation Release Date** has more co,plex schedule:
First alert triggers once 3 weeks prior 365 days threshold; second alert triggers once 2 weeks prior, third alert will trigger 1 week prior reaching 365 days threshold. After breaching 1-year threshold, notifications will be delivered daily.

All notifications are delivering to a corresponding channel at 11:00 in a morning. 

Notification anatomy
--------------------
********************
This is how typical notification looks like. Since notifications do not differ in their structure, we can give an example of one of them.

![Notification anatomy](https://github.com/opentelekomcloud-infra/eyes_on_docs/blob/documentation/alert_anatomy.PNG)

**Alert name:** name of an alert. Could be *Orphaned PRs Alert,* *Outdated Documents Alert* or *Open Issues Alert.*\
**Alert summary:** what happened, what is this alert about.\
**Squad and Service names, Zone name:** squad and service name to which alert is dedicated to. Zone name could be *Public* for common OTC and *Hybrid* for Swiss Cloud.\
**Direct link to problematic resource:** clickable link points to a resource which state triggert alert - specific commit, or PR or issue.\
**Link leads to Grafana dashboard:** URL points to Grafana dashboard contains briefly info regarding issues, PRs or documents.\
