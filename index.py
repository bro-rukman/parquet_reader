import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import Pool
from threading import Thread, current_thread
import pandas as pd
from queue import Queue
import os
import time
import itertools
from concurrent.futures import ThreadPoolExecutor
from utils.connection import get_column, connections

path = "/Users/endangrukmana/Downloads/parquet/example"
dir_list = os.listdir(path)
column_based = get_column()


def insert_into_sql(file):
    t = time.time()
    try:
        read_data = pd.read_parquet(file, engine='auto')
        df_clean = read_data[column_based]
        df_clean.to_sql(name="data_4g_huawei", con=connections, schema='public',
                        if_exists='append', index=False, method='multi')
        del df_clean
        print(file+" Success to execute")
    except:
        print(file+" Failed to execute")
    print(time.time()-t)


def execute_sql_with_mp(lock, file):
    num_cpus = mp.cpu_count()-1
    lock.acquire()
    insert_into_sql(file)
    lock.release()


def iterable_part(data):
    start = time.time()
    df = pd.DataFrame()
    for file_x in data:
        try:
            data_f = pd.read_parquet(file_x, engine='auto')
            df = pd.concat([df, data_f], ignore_index=True)
            del data_f
        except:
            print(file_x+" Gagal")
    df = df[column_based]
    df.to_sql(name="data_4g_huawei", con=connections, schema='public',
              if_exists='append', index=False, method='multi')
    print("End time:{}".format(time.time()-start))


def worker(q):
    value = q.get()
    print(current_thread().name, value)
    # insert_into_sql(value)
    q.task_done()


def mainQueue():
    start = time.time()
    files = []
    for file in dir_list:
        check = file.split('.')
        if check[len(check)-1] == 'parquet':
            files.append(
                f'{path}/{file}')
    q = Queue(10)
    for file in files:
        thread = Thread(target=worker, args=(q,))
        thread.daemon = True
        thread.start()
    for proc in files:
        q.put(proc)
    q.join()

    print("Elapsed Time: %s" % (time.time() - start))


def threadPools():
    start = time.time()
    files = []
    for file in dir_list:
        check = file.split('.')
        if check[len(check)-1] == 'parquet':
            files.append(
                f'{path}/{file}')
    with ThreadPoolExecutor() as pool:
        for file in files:
            pool.map(insert_into_sql, file)
        print("Elapsed Time: %s" % (time.time() - start))


if __name__ == "__main__":
    # mainQueue()
    threadPools()
