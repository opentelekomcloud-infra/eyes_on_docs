"""
This script is an entry point for all other modules included in Eyes-on-Docs
"""

import argparse

from scripts import eod_1_otc_services_dict, eod_2_gitea_info, eod_3_github_info, eod_4_failed_zuul, \
    eod_5_open_issues, eod_6_last_commit_info, eod_7_request_changes, eod_8_ecosystem_issues, eod_9_scheduler


def main():
    parser = argparse.ArgumentParser(description="Eyes-on-Docs scripts run")
    parser.add_argument('--eod1', action='store_true', help='OTC services dict')
    parser.add_argument('--eod2', action='store_true', help='Gitea info')
    parser.add_argument('--eod3', action='store_true', help='Github info')
    parser.add_argument('--eod4', action='store_true', help='Failed Zuul')
    parser.add_argument('--eod5', action='store_true', help='Open issues')
    parser.add_argument('--eod6', action='store_true', help='Last commit info')
    parser.add_argument('--eod7', action='store_true', help='Request changes')
    parser.add_argument('--eod8', action='store_true', help='Ecosystem issues')
    parser.add_argument('--eod9', action='store_true', help='Scheduler')
    args = parser.parse_args()

    if args.eod1:
        eod_1_otc_services_dict.run()
    if args.eod2:
        eod_2_gitea_info.run()
    if args.eod3:
        eod_3_github_info.run()
    if args.eod4:
        eod_4_failed_zuul.run()
    if args.eod5:
        eod_5_open_issues.run()
    if args.eod6:
        eod_6_last_commit_info.run()
    if args.eod7:
        eod_7_request_changes.run()
    if args.eod8:
        eod_8_ecosystem_issues.run()
    if args.eod9:
        eod_9_scheduler.run()


if __name__ == "__main__":
    main()
