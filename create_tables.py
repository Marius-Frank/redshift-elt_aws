import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries, drop_schema_queries


def drop_schemas(cur, conn):
    """
    Drop schemas
    INPUT:
      - cur: pyscopg2 instance
      - coonn: database cursor
    """
    for query in drop_schema_queries:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    """
    Drop/Delete tables
    INPUT:
      - cur: pyscopg2 instance
      - coonn: database cursor
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables
    INPUT:
      - cur: pyscopg2 instance
      - coonn: database cursor
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def create_schemas(cur, conn):
    """
    Create schemas
    INPUT:
      - cur: pyscopg2 instance
      - coonn: database cursor
    """
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    create_schemas(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()