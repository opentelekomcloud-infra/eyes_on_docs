from .classes import EnvVariables, Database, Timer
import logging


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


__all__ = ['EnvVariables', 'Database', 'Timer']
