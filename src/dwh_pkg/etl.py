import configparser
import psycopg2
from dwh_pkg.sql_queries import copy_table_queries, insert_table_queries
import time




def load_staging_tables(cur, conn):
    """
    Copy data from S3 to redshift staging tables.
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()



def insert_tables(cur, conn):
    """ Execute insert queries that insert data from staging tables to  target tables.
    :return:  None"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()




def main():
    """
    Connect to the db, copy data from s3 to staging table and then to target redshift table.
    :return:None
    """
    try:
        config = configparser.ConfigParser()
        config.read('../../dwh.cfg')
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        load_start= time.perf_counter()
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        conn.close()
        load_end = time.perf_counter()
        print(f"Load in  {load_end - load_start:0.4f} seconds or {(load_end - load_start)/60:0.2f} minutes")
    except Exception as e:
        print (e)


if __name__ == "__main__":
    main()