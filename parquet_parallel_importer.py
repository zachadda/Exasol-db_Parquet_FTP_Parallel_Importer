
from pathlib import Path
import os
import datetime
from datetime import date, datetime, timedelta
import re
import pyarrow.parquet as pq
import pandas as pd
import pyexasol

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from time import time
import hashlib

#from joblib import Parallel, delayed



######### USER INPUT !!!!!!!!
#create starting path
#path = input('Enter the PATH of the root directory in which you would like to look for parquet files:')
path = Path('/Users/zaad/Jupyter Notebooks/tmobile/') #this should be the parent directory of the dated folders
#path = Path(path)

######### USER INPUT !!!!!!!!
# days back you want to load
#num_days_back = input('How many days back would you like to load?:')
num_days_back = 17
num_days_back = int(num_days_back)

#calculate which date to start loading data
start_date = (date.today() - timedelta(days=num_days_back))

#create empty list
file_paths = []

#iterate through path objects, find all that end with .parquet and add to list of namedtuples
def find_files(input_path):
    for item in input_path.glob('**/*'):
        if item.suffix in ['.parquet']:
            #parquet_file = Path.resolve(item)
            path = os.path.normpath(item)
            split = str(item).split(os.sep)

            date_raw = re.search('\d{8}', split[-4])
            date = datetime.strptime(date_raw.group(), '%Y%m%d').date()

            if date >= start_date:
                file_paths.append(path)

    print("FILES LOADED!")
    print('There are ', len(file_paths), 'records to load from', start_date)


def create_target_schemas(files):
    for file in files:
        path = os.path.normpath(file)
        split = path.split(os.sep)
        schema_name = split[-3].upper()
        table_name = split[-2].upper()

        #create dataframe from parquet file
        schema = pq.read_schema(file, memory_map=True)
        schema = pd.DataFrame(({"column": name, "pa_dtype": str(pa_dtype)}
                               for name, pa_dtype in zip(schema.names, schema.types)))
        #Ensures columns in case the parquet file has an empty dataframe.
        schema = schema.reindex(
            columns=["column", "pa_dtype"], fill_value=pd.NA)

        ddl = []

        for index, row in schema.iterrows():
            column_def = row['column'] + " " + row['pa_dtype']
            ddl.append(column_def)

        ddl_stmt = f'CREATE TABLE IF NOT EXISTS ' + \
            table_name + '(' + ','.join(ddl) + ')'
        ddl_stmt = str(ddl_stmt).replace("decimal128", "DECIMAL").replace(
            "bool", "VARCHAR(20)").replace("date32[day]", "DATE").replace("timestamp[ns]", "TIMESTAMP").replace("string", "VARCHAR(2000)")

        #connect to Exasol database!
        C = pyexasol.connect_local_config(
            config_path=r'/Users/zaad/Jupyter Notebooks/.pyexasol.ini', config_section=r'my_exasol')
        C.execute('CREATE SCHEMA IF NOT EXISTS ' + schema_name)
        C.execute(ddl_stmt)

    print('***CORE TARGET SCHEMA & TABLES CREATED!***')


def import_files(file):
    #for file in files:

    #define inputs from file
    path = os.path.normpath(file)
    split = path.split(os.sep)
    #file_name = Path(file).stem
    hash_object = hashlib.md5(file.encode())
    md5_file_name = hash_object.hexdigest().upper()
    #schema_name = split[-3].upper()
    schema_name = 'RETAIL_2020'
    target_table_name = split[-2].upper()
    temp_table_suffix = split[-4].upper()
    temp_table_name = target_table_name + '_' + temp_table_suffix+'_'+md5_file_name

    #create dataframe from parquet file
    df = pq.read_table(file).to_pandas()

    # Generate the Schema from Pandas
    ddl = pd.io.sql.get_schema(df, temp_table_name, schema=schema_name).replace("TEXT", "VARCHAR(2000000)").replace(
        "INTEGER", "VARCHAR(2000000)").replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")

    #connect to Exasol database!
    C = pyexasol.connect_local_config(
        config_path=r'/Users/zaad/Jupyter Notebooks/.pyexasol.ini', config_section=r'my_exasol')
    # Execute DDL > CREATE OR REPLACE TABLE
    #C.execute('CREATE SCHEMA IF NOT EXISTS ' + schema_name)
    C.execute(ddl)

    # Import from pandas DataFrame into Exasol table
    C.import_from_pandas(df, (schema_name, temp_table_name))

    #get the columns from the staging tables to generate the MERGE statement
    stmt = C.execute(
        "select column_name from exa_all_columns where COLUMN_TABLE like '" + temp_table_name + "'")

    #create concated list of columns for the merge statement
    merge_column_names = []
    for row in stmt:
        merge_column_names.append(row["COLUMN_NAME"])
    merge_columns = ','.join(merge_column_names)

    C.execute("MERGE INTO " + schema_name + "." + target_table_name + " target USING "
              + schema_name + "." + temp_table_name + " stage "
              + " ON target." +
              merge_column_names[0] + " = stage." + merge_column_names[0]
              + " WHEN NOT MATCHED THEN INSERT VALUES (" + merge_columns + ")")
    C.execute("DROP TABLE "+schema_name+"."+temp_table_name)
    # Output
    stmt = C.last_statement()
    print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
    C.close()
    #print('COMPLETE!')


def main():
    start = time()
    cpuCount = int(os.cpu_count()/4)
    pool = ProcessPoolExecutor(max_workers=cpuCount)
    results = list(pool.map(import_files, file_paths))
    end = time()
    print('Took %.3f seconds' % (end - start))


if __name__ == '__main__':
    find_files(path)
    create_target_schemas(file_paths)
    main()
