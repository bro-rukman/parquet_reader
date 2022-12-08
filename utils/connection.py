import psycopg2 as pg
import sqlalchemy
import os

database = "huawei"
user = "postgres"
password = "password"
host = "localhost"
port = "5432"

pg_conn = pg.connect(database=database, user=user,
                     password=password, host=host, port=port)
connections_sqlalchemy = sqlalchemy.create_engine(
    "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))


def connection_psycopg2():
    pg_conn = pg.connect(database="huawei", user="postgres",
                         password="password", host="localhost", port=5432)
    return pg_conn


def get_column():
    data_cols = []
    sql = "select column_name from information_schema.columns where table_name = 'data_sample' and table_schema = 'public'"
    try:
        pg_conn.autocommit = True
    except:
        print("Failed to connect database !")
    with pg_conn.cursor() as curr:
        try:
            curr.execute(sql)
            column_names = [desc[0] for desc in curr.fetchall()]
            for i in column_names:
                data_cols.append(i)
        except:
            print("No data")
    pg_conn.commit()
    pg_conn.close()
    return data_cols
