import configparser
import psycopg2
from dwh_pkg.sql_queries import create_table_queries, drop_table_queries, create_schema_str,set_search_path
import boto3


def drop_tables(cur, conn):
    """
    Execute drop table queries.
    :param cur: cursor
    :param conn: connection
    :return:None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Execute create table queries
    :param cur: cursor
    :param conn: connection
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to the redsift and create tables.
    :return:None
    """
    try:
        config = configparser.ConfigParser()
        config.read('../../dwh.cfg')
        print(*config['CLUSTER'].values())


        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()