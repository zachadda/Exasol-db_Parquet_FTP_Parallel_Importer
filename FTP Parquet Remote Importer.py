'''
This script scans all the sub directories within the root directory you specify and finds all .parquet files.
It creates SCHEMAS based on directory names
It creates tables based on the sub-directories of the SCHEMA directories
And finally loads the respective parquet files

The script parses the data from the level 2 directory. It expects the data format of YYYYMMDD to be contained somewhere in the directory. For example, export_US_f_20211017_50
You can select how many days back in which the files will be loaded. It uses this date to make that determination

Loaded files are stored in a log table in Exasol. Duplicate files will not be loaded. If a file with the same key exists, then the record will be updated with the latest value.

Expected directory structure
1. YOUR ROOT DIRECTORY/
    ├── 2. DIRECTORY WITH NAME CONTAINING DATE (e.g. YYYYMMDD)/
        ├── 3. SCHEMA_NAME/
            ├── 4. TABLE_NAME/
                └── 5. <file_name>.parquet
                  

Note: Parallelization will NOT work when running this code in a Jupyter notebook

Potential To-do's:
If days back = 0, then load all files
Change connection fetch_dict = True for the create_delta_list() function. That way same connection string can be used.

'''

from distutils import log
from operator import index
from pathlib import Path
import os
import datetime
from datetime import date, datetime, timedelta
import re
import hashlib

import pyarrow.parquet as pq
import pandas as pd
import pyexasol

import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from time import time



######### USER INPUT #########
# this should be the parent directory of the dated folders
path = Path('/Users/zachary.adda/Library/CloudStorage/OneDrive-EXASOLAG/Github/parquet_parallell_importer_py/')
schema_suffix = '' #what to append to the SCHEMAS that get created from the files. Leave blank with NO SPACES, if you don't want to add a suffix

# days back you want to load
num_days_back = 365
#Logging Schema Name
log_schema = 'ETL_PARQUET'
log_table = 'JOB_LOG'

#Use True or False to choose whether imports will run in parallell or not. The parallelization 1/4 of the cores in your system.
parallelize = False

# Add the Table and columns to import to the below list
table_columns = {'ARTICLE':['ARTICLE_ID','BASE_SALES_PRICE','DESCRIPTION','DISTRIBUTION_COST','EAN','PRODUCT_GROUP','PRODUCT_GROUP_DESC','PURCHASE_PRICE','QUANTITY_UNIT','SUBPRODUCT_GROUP','TMP_OLD_NR']
,'SALES':['EMPLOYEE_ID','LOYALTY_ID','MARKET_ID','MONEY_GIVEN','PRICE','RETURNED_CHANGE','SALES_DATE','SALES_ID','SALES_TIMESTAMP','TERMINAL_DAILY_SALES_NR','TERMINAL_ID']
,'SALES_POSITIONS':['AMOUNT','ARTICLE_ID','CANCELED','POSITION_ID','PRICE','SALES_ID','VOUCHER_ID']}

######### END USER INPUT #########


num_days_back = int(num_days_back)
#calculate which date to start loading data
start_date = (date.today() - timedelta(days=num_days_back))

#create empty list
file_paths = []


#iterate through path objects, find all that end with .parquet and add to list of namedtuples
def find_files(input_path):
    for item in input_path.glob('**/*'):
        if item.suffix in ['.parquet']:
            path = os.path.normpath(item) #return full file path of each parquet file
            split = str(item).split(os.sep) #split each file path based on OS separator. e.g. mac /, windows \
            date_raw = re.search('\d{8}', split[-4]) #4 is the parsed object which contains the date. e.g. the parent directory
            date = datetime.strptime(date_raw.group(), '%Y%m%d').date() #format the date

            #add the files to the list which are greater
            if date >= start_date:
                file_paths.append(path)


    print('\n We found ', len(file_paths), 'files for loading after', start_date.strftime("%B-%d, %Y"),'/',num_days_back,'days back \n If the files have not been loaded, they will be. \n')


def create_delta_list():
    #connect to Exasol database
    C = pyexasol.connect_local_config(
    config_path=r'/Users/zachary.adda/Library/CloudStorage/OneDrive-EXASOLAG/Github/parquet_parallell_importer_py/.pyexasol.ini', config_section=r'logging_exasol')#NOTE FETCH DICT MUST BE SET TO FALSE!!
    
    #create logging schema and tables if they do not already exist
    C.execute(f"CREATE SCHEMA IF NOT EXISTS {log_schema}")
    C.execute(f"CREATE TABLE IF NOT EXISTS {log_schema}.{log_table} (IMPORT_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,VINTAGE DATE COMMENT IS 'DATE FROM FILE' ,DEST_SCHEMA VARCHAR(2000) ,DEST_TABLE VARCHAR(2000) ,RECORDS_IMPORTED DECIMAL(18,0) ,EXECUTION_TIME DECIMAL(18,3) ,FILE_NAME VARCHAR(2000000) ,PARENT_DIRECTORY VARCHAR(2000) ,FILE_PATH VARCHAR(2000000))")
    
    # Retrieve Data from Exasol
    df = pd.DataFrame(file_paths,columns=['FILE_PATH'])

    df1 = C.export_to_pandas(f'SELECT FILE_PATH FROM {log_schema}.{log_table}')
    
    #merge dataframes and add column which indicates duplicate records
    merged = df.merge(df1,on='FILE_PATH',how='outer',indicator=True)

    #filter df on merge column to exclude duplicates
    final_df = merged.loc[(merged['_merge'] == 'left_only')]

    #convert df to list
    final_list = final_df['FILE_PATH'].tolist()
    
    print('\n', len(final_list), 'files of the',len(file_paths),' files found will be loaded.')
    return final_list


def create_target_schemas(files):
    for file in files:
        #file = repr(file)
        path = os.path.normpath(file)
        split = path.split(os.sep)
        schema_name = split[-3].upper()+ schema_suffix
        table_name = split[-2].upper()

        #create dataframe from parquet file
        schema = pq.read_schema(file, memory_map=True)
        
        #write schema to a df
        schema = pd.DataFrame(({"column": name, "pa_dtype": str(pa_dtype)}
                               for name, pa_dtype in zip(schema.names, schema.types)))

        #Ensures columns in case the parquet file has an empty dataframe.
        schema = schema.reindex(
            columns=["column", "pa_dtype"], fill_value=pd.NA)
 
        ddl = []
        ddl_final = []
        for index, row in schema.iterrows():
            if row['column'] in table_columns[table_name]:
                column_def = row['column'] + " " + row['pa_dtype']
                ddl.append(column_def)
                ddl_final.append(row['column'])

            else:
                pass


        ddl_stmt = f'CREATE TABLE IF NOT EXISTS ' + \
            table_name + '(' + ','.join(ddl) + ')'
        ddl_stmt = str(ddl_stmt).replace("decimal128", "DECIMAL").replace(
            "bool", "VARCHAR(20)").replace("date32[day]", "DATE").replace("timestamp[ns]", "TIMESTAMP").replace("string", "VARCHAR(10000)")
        
        

        #connect to Exasol database
        C = pyexasol.connect_local_config(
            config_path=r'/Users/zachary.adda/Library/CloudStorage/OneDrive-EXASOLAG/Github/parquet_parallell_importer_py/.pyexasol.ini', config_section=r'my_exasol')
        
        #ensure schema exists and is created, if not
        C.execute('CREATE SCHEMA IF NOT EXISTS ' + schema_name)
        
        #execute ddl
        C.execute(ddl_stmt)

        #create logging schema and tables
        C.execute("CREATE SCHEMA IF NOT EXISTS ETL")
        C.execute("CREATE TABLE IF NOT EXISTS ETL.JOB_LOG (IMPORT_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,VINTAGE DATE COMMENT IS 'DATE FROM FILE' ,DEST_SCHEMA VARCHAR(2000) ,DEST_TABLE VARCHAR(2000) ,RECORDS_IMPORTED DECIMAL(18,0) ,EXECUTION_TIME DECIMAL(18,3) ,FILE_NAME VARCHAR(2000000) ,PARENT_DIRECTORY VARCHAR(2000) ,FILE_PATH VARCHAR(2000000))")
        
        #close connection
        C.close()

    print('***CORE TARGET SCHEMA & TABLES CREATED!***')

  

def import_files(file):
    #define inputs from file
    path = os.path.normpath(file) #put file path in variable
    split = path.split(os.sep) #split each file path based on OS separator. e.g. mac /, windows \
    
    #metadata for logging
    date_raw = re.search('\d{8}', split[-4]) #4 is the parsed object which contains the date. e.g. the parent directory
    file_date = datetime.strptime(date_raw.group(), '%Y%m%d').date() #format the date
    file_name = split[-1].upper()
    parent_directory = split[-3].upper()

    #hash values used to create temp tables
    hash_object = hashlib.md5(file.encode()) #generate a hash object
    md5_file_name = hash_object.hexdigest().upper() #create hash value from hash object
    
    #define object names from filepath
    schema_name = split[-3].upper()+schema_suffix
    table_name = split[-2].upper()
    target_table_name = split[-2].upper()
    temp_table_suffix = split[-4].upper()
    temp_table_name = target_table_name + '_' + temp_table_suffix+'_'+md5_file_name

    ############# GET COLUMNS, correct order ##########
    #create dataframe from parquet file
    schema = pq.read_schema(file, memory_map=True)

    schema = pd.DataFrame(({"column": name, "pa_dtype": str(pa_dtype)}
                            for name, pa_dtype in zip(schema.names, schema.types)))

    # #Ensures columns in case the parquet file has an empty dataframe.
    schema = schema.reindex(
            columns=["column", "pa_dtype"], fill_value=pd.NA)


    ddl = []
    ddl_final = []
    for index, row in schema.iterrows():
        if row['column'] in table_columns[table_name]:
            column_def = row['column'] + " " + row['pa_dtype']
            ddl.append(column_def)
            ddl_final.append(row['column'])

        else:
            pass
    
    #############GET COLUMNS END##########
    
    #load parquet file into dataframe. Only load select columns via the columns parameter.
    df = pq.read_table(file,columns=ddl_final).to_pandas()
    


    ############ CREATE & IMPORT TEMP TABLES #############
    ddl_stmt = f'CREATE TABLE IF NOT EXISTS ' + \
        temp_table_name + '(' + ','.join(ddl) + ')'

    ddl_stmt = str(ddl_stmt).replace("decimal128", "DECIMAL").replace(
        "bool", "VARCHAR(20)").replace("date32[day]", "DATE").replace("timestamp[ns]", "TIMESTAMP").replace("string", "VARCHAR(10000)")
    
    #connect to Exasol database
    C = pyexasol.connect_local_config(
        config_path=r'/Users/zachary.adda/Library/CloudStorage/OneDrive-EXASOLAG/Github/parquet_parallell_importer_py/.pyexasol.ini', config_section=r'my_exasol')
    
    C.execute('CREATE SCHEMA IF NOT EXISTS ' + schema_name)
    C.execute(ddl_stmt)

    # Import from pandas DataFrame into Exasol table
    C.import_from_pandas(df, (schema_name, temp_table_name))
    #C.import_from_pandas(df, (schema_name, target_table_name))
    stmt_import = C.last_statement()

    ############ CREATE & IMPORT TEMP TABLES END #############
    
    
    ############ MERGE TEMP TABLES INTO TARGET #############
    
    #get the columns from the staging tables to generate the MERGE statement
    stmt = C.execute("select column_name from exa_all_columns where COLUMN_TABLE like '" + temp_table_name + "'")

    #create concated list of columns for the merge statement
    merge_column_names = [] #add HASHED_GUID
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
    print(f'IMPORTED {stmt_import.rowcount()} rows in {stmt.execution_time}s')
    
    print(f'IMPORTED {stmt_import.rowcount()} rows in {stmt_import.execution_time}s')

    ############ MERGE TEMP TABLES INTO TARGET END #############

    #write job log
    C.execute(f"insert into {log_schema}.{log_table}(VINTAGE,DEST_SCHEMA,DEST_TABLE,RECORDS_IMPORTED,EXECUTION_TIME,FILE_NAME,PARENT_DIRECTORY,FILE_PATH) values ('{file_date}','{schema_name}','{target_table_name}','{stmt_import.rowcount()}','{stmt_import.execution_time}','{file_name}','{parent_directory}','{path}')")
    C.close()


def parallel_import_files(file):
    import_files(file)

def serial_import_files(files):
    print('SERIAL IMPORT INITIATED!!')
    for file in files:
        import_files(file)


def main():
    find_files(path)
    file_paths = create_delta_list()
    create_target_schemas(file_paths)
    
    if parallelize == True:
        print('PARALLEL IMPORT INITIATED!!')
        start = time()
        cpuCount = int(os.cpu_count()/4)
        #cpuCount = 1 #if there are concurrency errors reduce the # of cpus
        pool = ProcessPoolExecutor(max_workers=cpuCount)
        results = list(pool.map(parallel_import_files, file_paths))
        end = time()
        print('Took %.3f seconds' % (end - start))
    else:
        start = time()
        serial_import_files(file_paths)
        end = time()
        print('Took %.3f seconds' % (end - start))


if __name__ == '__main__':
    main()