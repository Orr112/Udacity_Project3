import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_list, drop_list


def drop_tables(cur, conn):
    """Iterate over drop table list
    and execute drop table queries"""
    i=0
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('Query Executed: ' + drop_list[i] )
        i+=1

def create_tables(cur, conn):
    """Iterate over create table list
    and execute create table queries"""
    i=0
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print('Query Executed: ' + create_list[i] )
        i+=1

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

    #Drop existing tables
    drop_tables(cur, conn)

    #Create tables
    create_tables(cur, conn)

    #Drop connection
    conn.close()


if __name__ == "__main__":
    main()
