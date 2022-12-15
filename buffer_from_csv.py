import pandas as pd
import numpy as np
import os
import psycopg2 as pg
import time
from io import StringIO
from utils.connection import get_column, connection_psycopg2

path = "/Users/endangrukmana/Downloads/parquet/example"
dir_list = os.listdir(path)
data_cols = get_column()
pg_connection = connection_psycopg2()
if __name__ == "__main__":
    start = time.time()
    files = []
    for file in dir_list:
        check = file.split('.')
        if check[len(check)-1] == 'parquet':
            files.append(f'{path}/{file}')
# split per 10 file get time 51 second for 20 files
    splitSize = 10
    file_splited = [files[x:x+splitSize]
                    for x in range(0, len(files), splitSize)]
    for file in file_splited:
        sio = StringIO()
        for file_exe in file:
            try:
                read_data = pd.read_parquet(file_exe, engine='auto')
                read_data = read_data[data_cols]
                sio.write(read_data.to_csv(
                    index=None, header=None, mode="a", na_rep="NULL"))
            except:
                print("error {}".format(file_exe))
        # strQuery = "COPY data_sample  FROM STDIN ( FORMAT 'csv', HEADER false )"
        sio.seek(0)
        with pg_connection.cursor() as curr:
            #     #     curr.copy_expert(strQuery, sio)
            curr.copy_from(file=sio, columns=data_cols,
                           table="data_sample", sep=",", null="NULL")
            pg_connection.commit()
        sio.truncate(0)
# not splitted get 70 second for 20 files
    # for file in files:
    #     sio = StringIO()
    #     try:
    #         read_data = pd.read_parquet(file, engine='auto')
    #         read_data = read_data[data_cols]
    #         sio.write(read_data.to_csv(
    #             index=None, header=None, mode="a", na_rep="NULL"))
    #     except:
    #         print("error {}".format(file))
    #     sio.seek(0)
    #     # strQuery = "COPY data_sample  FROM STDIN ( FORMAT 'csv', HEADER false )"
    #     with pg_connection.cursor() as curr:
    #         #     #     curr.copy_expert(strQuery, sio)
    #         curr.copy_from(file=sio, columns=data_cols,
    #                        table="data_sample", sep=",", null="NULL")
    #         pg_connection.commit()
    #     sio.truncate(0)

    print("Time consumed {}".format(time.time()-start))
