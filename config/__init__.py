import logging

from .classes import Database, EnvVariables, Timer


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


__all__ = ['EnvVariables', 'Database', 'Timer']
