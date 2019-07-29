import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, copy_table, insert_table


def load_staging_tables(cur, conn):
    """Iterate over staging table list
    and execute staging table queries"""
    i=0
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Copy Completed: ' + copy_table[i] )
            i+=1
        except Exception as e:
            print(e)


def insert_tables(cur, conn):
    """Iterate over insert table list
    and execute insert table queries"""
    i=0
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Insert Completed: ' + insert_table[i])
            i+=1
        except Exception as e:
            print(e)


def main():
    #Get parameters needed to connect to redshift cluser
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY = config.get('AWS','key')
    SECRET = config.get('AWS','secret')

    DWH_DB=config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")
    DWH_ROLE_ARN = config.get("DWH", "DWH_ROLE_ARN")
    DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")


    #connect to cluster
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
    conn_string

    #connect to postgres instance
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #Execute loading of data in staging tables
    load_staging_tables(cur, conn)

    #Extract data from staging data to populate tables
    insert_tables(cur, conn)

    #close connection
    conn.close()


if __name__ == "__main__":
    main()
