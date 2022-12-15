import pandas as pd
import numpy as np
import os
import time
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pyarrow as pa
from utils.connection import get_column, connection_psycopg2
data_cols = get_column()
connection = connection_psycopg2()
target_path = '/Users/endangrukmana/Downloads/Frontend/parquet_py/'
path = "/Users/endangrukmana/Downloads/parquet/example"
file_name_target = "result.csv"
fullname = os.path.join(target_path, file_name_target)

if __name__ == "__main__":
    start = time.time()
    files = []
    dir_list = os.listdir(path)
    for file in dir_list:
        check = file.split('.')
        if check[len(check)-1] == 'parquet':
            files.append(f'{path}/{file}')

# not splitted for 20 file get 44 second
    for file in files:
        if os.path.isfile(fullname):
            os.remove(fullname)
        try:
            df = pd.read_parquet(file, engine="auto")
            df = df[data_cols]
            df = df.replace({np.nan: None})
            df.to_csv(fullname, index=False, header=False, sep=",")
            sqlQuery = "COPY data_sample FROM STDIN DELIMITER ',' CSV"
            with open(fullname) as f:
                with connection.cursor() as curr:
                    curr.copy_expert(sqlQuery, f)
                connection.commit()
            del df
        except:
            print(f'Error {file}')
# split per 10 file get second for 20 file
    # splitSize = 10
    # file_splited = [files[x:x+splitSize]
    #                 for x in range(0, len(files), splitSize)]
    # for file_new in file_splited:
    #     if os.path.isfile(fullname):
    #         os.remove(fullname)
    #     for file_x in file_new:
    #         try:
    #             df = pd.read_parquet(file_x, engine="auto")
    #             df = df[data_cols]
    #             df = df.replace({np.nan: None})
    #             df.to_csv(fullname, index=False,
    #                       mode='a', header=False, sep=",")
    #         except:
    #             print(f'{file_x} error !')
    #     try:
    #         cursor = connection.cursor()
    #         sqlQuery = "COPY data_sample FROM STDIN DELIMITER ',' CSV"
    #         with open(fullname) as f:
    #             cursor.copy_expert(sqlQuery, f)
    #         connection.commit()
    #     except:
    #         print("ada error")
    print("end time {}".format(time.time()-start))
