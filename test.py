import os
import time
import pandas as pd
from multiprocessing import Process, Value, Array, Lock
from utils.connection import get_column, connections
path = "/Users/endangrukmana/Downloads/parquet/example"
dir_list = os.listdir(path)
column_based = get_column()


def iterable_part(files_iterate, lock):
    print(files_iterate)
    start = time.time()
    df = pd.DataFrame()
    for file_x in files_iterate:
        with lock:
            try:
                data_f = pd.read_parquet(file_x, engine='auto')
                df = pd.concat([df, data_f], ignore_index=True)
                del data_f
            except:
                print(file+" Gagal")
    df = df[column_based]
    df.to_sql(name="data_4g_huawei", con=connections, schema='public',
              if_exists='append', index=False, method='multi')
    print("End time:{}".format(time.time()-start))


if __name__ == "__main__":
    files = []
    for file in dir_list:
        check = file.split('.')
        if check[len(check)-1] == 'parquet':
            files.append(
                f'{path}/{file}')
    splitedSize = 5
    file_splited = [files[x:x+splitedSize]
                    for x in range(0, len(files), splitedSize)]
    lock = Lock()
    p1 = Process(target=iterable_part, args=(files, lock))
    p2 = Process(target=iterable_part, args=(files, lock))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    # processes = []
    # num_process = os.cpu_count()-1

    # for i in range(num_process):
    #     process = Process(target=square_number)
    #     processes.append(process)
    # for process in processes:
    #     process.start()
    # for process in processes:
    #     process.join()
