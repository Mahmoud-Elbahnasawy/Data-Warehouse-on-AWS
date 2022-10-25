import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    INPUTS:
    CURSOR 
    CONNECTION
    OUTPUT:
    LOADS DATA INTO STAGING TABLES AFTER IT IS COPIED FROM S3 BUCKET
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    INPUT :
    CURSOR
    CONNECTION
    OUTPUT:
    INSERT DATA INTO SCHEMA FIVE TABLES (songplay,users,artist,time,song) 
    FROM TWO SATGING TABLES THAT HAD BEEN LOADED BY
    THE load_staging_tables FUNCTION
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    THIS FUNCTION RUNS THE load_staging_tables AND insert_tables FUNCTIONS
    AFTER CONNECTING TO DATABASE
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


#RUN MAIN FUNCTION IF WE WERE IN THE CURRENT FILE
if __name__ == "__main__":
    main()