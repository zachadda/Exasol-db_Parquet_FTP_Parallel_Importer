# Overview
If you have PARQUET files stored on an external Windows or Linux server, this python script can be executed locally to load the parquet files to an Exasol database.

# Description

- This script scans all the sub directories within the root directory you specify and finds all .parquet files.
- It creates SCHEMAS based on directory names
- It creates tables based on the sub-directories of the SCHEMA directories
- And finally loads the respective parquet files

- The script parses the data from the level 2 directory. It expects the data format of YYYYMMDD to be contained somewhere in the directory. For example, export_US_f_20211017_50
- You can select how many days back in which the files will be loaded. It uses this date to make that determination

- Loaded files are stored in a log table in Exasol. Duplicate files will not be loaded. If a file with the same key exists, then the record will be updated with the latest value.

```

Expected directory structure
1. YOUR ROOT DIRECTORY/
    ├── 2. DIRECTORY WITH NAME CONTAINING DATE (e.g. YYYYMMDD)/
        ├── 3. SCHEMA_NAME/
            ├── 4. TABLE_NAME/
                └── 5. <file_name>.parquet
                  
```

Note: Parallelization will NOT work when running this code in a Jupyter notebook

# Potential To-do's:
- If days back = 0, then load all files
- Change connection fetch_dict = True for the create_delta_list() function. That way same connection string can be used.
