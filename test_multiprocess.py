from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from threading import Thread
import numpy as np
import os
import psycopg2.extras
import psycopg2 as pg
import time
from multiprocessing.pool import ThreadPool
from utils.connection import get_column

path = "/Users/endangrukmana/Downloads/parquet/20220925"
dir_list = os.listdir(path)
data_cols = get_column()
pg_connection = pg.connect(database="huawei",
                           user="postgres",
                           password="password",
                           host="localhost",
                           port="5432")
files = []
for file in dir_list:
    check = file.split('.')
    if check[len(check)-1] == 'parquet':
        files.append(f'{path}/{file}')


def insert(data):
    # data.to_sql(name="data_4g_huawei", con=connections, schema='public',
    #             if_exists='append', index=False, method='multi')
    select = "SELECT COUNT(*) FROM data_4g_huawei;"
    with pg_connection.cursor() as cursor:
        cursor.execute(select)
        return pg_connection.commit()
    # tuples = [tuple(x) for x in data.to_numpy()]
    # new_tuples = [
    #     tuple(None if isinstance(i, float) and math.isnan(i) else i for i in t)
    #     for t in tuples
    # ]
    # cols = ','.join(list(data.columns))
    # format_new = "INSERT INTO data_4g_huawei ({}) VALUES %s"
    # format_new1 = format_new.format(cols)
    # cursor = koneksi.cursor()
    # pg.extras.execute_values(
    #     cursor, format_new1, new_tuples, template=None, page_size=100)
    # koneksi.commit()
    # koneksi.close()

    # # query = "INSERT INTO %s(%s) VALUES %%s" % ('data_4g_huawei', cols)
    # # extras.execute_values(cursor, query.as_string(cursor), new_tuples)
    # # conn.commit()
    # # conn.close()
    # sql = "INSERT INTO data_4g_huawei ({}) VALUES {}"
    # # values = []
    # # for index, row in data.iterrows():
    # #     value = []
    # #     for key in data.keys():
    # #         if pd.isna(row[key]):
    # #             value.append("{}".format('NULL'))
    # #         else:
    # #             value.append("{}".format(row[key]))

    # #     values.append(','.join(value))
    # # format_sql = sql.format(','.join(cols))

    # format_sql = sql.format(cols, new_tuples)
    # print(format_sql)
    # conn.autocommit = True
    # curr = conn.cursor()
    # curr.execute(sql)
    # conn.commit()
    # conn.close()
    # print('query generated')


def execute_data(data):
    if len(data) > 0:
        df_columns = list(data)
        columns = ','.join(f'"{w}"' for w in df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_statement = "INSERT INTO data_sample ({}) {}".format(
            columns, values)
        curr = pg_connection.cursor()
        psycopg2.extras.execute_batch(
            curr, insert_statement, data.values)
    pg_connection.commit()


def script(file):
    try:
        read_data = pd.read_parquet(file, engine='auto')
        read_data = read_data[data_cols]
        data_clean = read_data.replace({np.nan: None})
        # if len(data_clean) > 0:
        #     df_columns = list(data_clean)
        #     columns = ','.join(f'"{w}"' for w in df_columns)
        #     values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        #     insert_statement = "INSERT INTO data_sample ({}) {}".format(
        #         columns, values)
        #     curr = pg_connection.cursor()
        #     psycopg2.extras.execute_batch(
        #         curr, insert_statement, data_clean.values)
        # pg_connection.commit()
        # execute_data(data_clean)
        del data_clean
    except:
        print("Failed file: {}".format(file))


class multi_threading:
    def __init__(self, file):
        self.file = file

    def insert_data(data):
        if len(data) > 0:
            df_columns = list(data)
            columns = ','.join(f'"{w}"' for w in df_columns)
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
            insert_statement = "INSERT INTO data_sample ({}) {}".format(
                columns, values)
            curr = pg_connection.cursor()
            psycopg2.extras.execute_batch(
                curr, insert_statement, data.values)    
        pg_connection.commit()

    def parsing_file(self):
        try:
            read_data = pd.read_parquet(self.file, engine='auto')
            read_data = read_data[data_cols]
            data_clean = read_data.replace({np.nan: None})
            # if len(data_clean) > 0:
            #     df_columns = list(data_clean)
            #     columns = ','.join(f'"{w}"' for w in df_columns)
            #     values = "VALUES({})".format(
            #         ",".join(["%s" for _ in df_columns]))
            #     insert_statement = "INSERT INTO data_sample ({}) {}".format(
            #         columns, values)
            #     curr = pg_connection.cursor()
            #     psycopg2.extras.execute_batch(
            #         curr, insert_statement, data_clean.values)
            # pg_connection.commit()
            # self.insert_data(data_clean)
            del data_clean
            print("Succeeded : {}".format(self.file))
        except:
            print("Failed : {}".format(self.file))

    def run(self):
        self.parsing_file()


if __name__ == "__main__":
    start = time.time()
    # for file in files:
    #     multi_threading(file).run()

    # thread_list = []
    # for file in files:
    #     t = Thread(target=multi_threading(file).run)
    #     t.start()
    #     thread_list.append(t)
    # for thread in thread_list:
    #     thread.join()

    with ThreadPoolExecutor(10) as executor:
        t_res = executor.map(script, files)
    # with ThreadPool(10) as pool:
    #     pool.map(script, files, chunksize=1)
    print("Time consumed {}".format(time.time()-start))
