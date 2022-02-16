import configparser
import psycopg2
import pytest


config = configparser.ConfigParser()
config.read('dwh.cfg')
cluster = config['CLUSTER']


@pytest.fixture(scope='module')
def connection_handler():
    conn = psycopg2.connect(
        f"host={cluster['HOST']} dbname={cluster['DB_NAME']} user={cluster['DB_USER']} password={cluster['DB_PASSWORD']}")
    cur = conn.cursor()
    yield cur
    conn.close()
