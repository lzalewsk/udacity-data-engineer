import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads staging tables from S3 bucket using the queries in `copy_table_queries` list.
    Parameters:
        cur:  DB connection cursor.
        conn: Connection handler to DB. Used for transaction commit.
    Returns:
        None   
    """
    
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform data from staging tables into facts and diamension tables using the queries in `insert_table_queries` list.
    Parameters:
        cur:  DB connection cursor.
        conn: Connection handler to DB. Used for transaction commit.
    Returns:
        None     
    """
    
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()