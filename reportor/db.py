import os
import sqlalchemy as sa

from reportor.config import load_config

def db_from_config(config_name, config=None):
    if config is None and 'REPORTOR_CREDS' in os.environ:
        config = load_config(os.environ['REPORTOR_CREDS'])
    url = config.get("db", config_name)
    return sa.create_engine(url)
