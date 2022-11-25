import pandas as pd
import numpy as np
import os
import psycopg2.extras
import psycopg2 as pg
import sqlalchemy
import math
import time
import threading
import multiprocessing as mp
from multiprocessing import Process
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from utils.connection import get_column, conn, connections

target_path = '/Users/endangrukmana/Downloads/Frontend/parquet_py/'
path = "/Users/endangrukmana/Downloads/parquet/example"
file_name_target = "result.csv"
if not os.path.exists(target_path):
    os.mkdir(target_path)
fullname = os.path.join(target_path, file_name_target)
dir_list = os.listdir(path)
data_cols = get_column()

dir_list = os.listdir(path)
files = []
for file in dir_list:
    check = file.split('.')
    if check[len(check)-1] == 'parquet':
        files.append(f'{path}/{file}')
files_split = np.array_split(files, 10)
if not os.path.isfile(fullname):
    for idx, arr_split in enumerate(files_split):
        if idx == 0:
            data_show = pd.DataFrame()
            for files_path in arr_split:
                try:
                    data_f = pd.read_parquet(files_path, engine='auto')
                    data_show = pd.concat(
                        [data_show, data_f], ignore_index=True)
                except:
                    print(files_path + " Failed")
            data_show.to_csv(fullname, mode='a', index=False, sep=",")
            print("Split 1 done")
        else:
            for files_path_next in arr_split:
                try:
                    data_f_next = pd.read_parquet(
                        files_path_next, engine='auto')
                    data_f_next.to_csv(fullname, mode='a',
                                       index=False, sep=",", header=False)
                except:
                    print(files_path_next + " Failed")
else:
    print("File "+fullname + " is already exist")
