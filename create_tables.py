import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    INPUT:
    CURSOR 
    CONNECTION
    OUTPUT:
    DELETE TABLES IF THEY EXIST
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    INPUT:
    CURSOR 
    CONNECTION
    OUTPUT:
    CREATES TABLES IF THEY DON'T EXIST
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function connects to database and executes the drop_tables and create_tables functions 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

#RUN MAIN FUNCTION IF WE WERE IN THE CURRENT FILE
if __name__ == "__main__":
    main()