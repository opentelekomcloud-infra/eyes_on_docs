"""
This script contains data classes for code reusing
"""

import os
import logging
import psycopg2


class EnvVariables:
    required_env_vars = [
        "DB_HOST", "DB_PORT", "DB_CSV", "DB_USER", "DB_ORPH", "DB_ZUUL", "DB_PASSWORD", "GITEA_TOKEN", "GITHUB_TOKEN", "GITHUB_FALLBACK_TOKEN"
    ]

    def __init__(self):
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT")
        self.db_csv = os.getenv("DB_CSV")  # main postgres db, open PRs tables for public and hybrid clouds are stored
        self.db_user = os.getenv("DB_USER")
        self.db_orph = os.getenv("DB_ORPH")
        self.db_zuul = os.getenv("DB_ZUUL")
        self.db_password = os.getenv("DB_PASSWORD")
        self.gitea_token = os.getenv("GITEA_TOKEN")
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.github_fallback_token = os.getenv("GITHUB_FALLBACK_TOKEN")
        self.check_env_variables()

    def check_env_variables(self):
        for var in self.required_env_vars:
            if os.getenv(var) is None:
                raise Exception("Missing environment variable: %s" % var)


class Database:
    def __init__(self, env):
        self.db_host = env.db_host
        self.db_port = env.db_port
        self.db_user = env.db_user
        self.db_password = env.db_password

    def connect_to_db(self, db_name):
        logging.info("Connecting to Postgres (%s)...", db_name)
        try:
            return psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=db_name,
                user=self.db_user,
                password=self.db_password
            )
        except psycopg2.Error as e:
            logging.error("Connecting to Postgres: an error occurred while trying to connect to the database: %s", e)
            return None
