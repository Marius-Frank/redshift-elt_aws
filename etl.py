import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

b_load = False
b_transform = True

def load_staging_tables(cur, conn):
    """
    Load data from S3 bucket into staging tables
    INPUT:
      - cur: pyscopg2 instance
      - coonn: database cursor
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform data from staging tables into analytics tables.
    Remove duplicates and NULL values
    INPUT:
      - cur: pyscopg2 instance
      - coonn: database cursor
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    if b_load:
        load_staging_tables(cur, conn)
    if b_transform:
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()