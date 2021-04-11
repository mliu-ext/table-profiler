# Snowflake Table Profiler and Comparison Tool

### table profiler: tbl_profile_sn.py
Collect below metrics for each column of the table. Summary is printed out in console. Saving to excel is optional.
```
RowCount
DistinctCount
NullCount
UniqueCount
Duplicatecount
Average
Max
Min
BlankCount
```

### Table comparison: tbl_compare.py
Compare two snowflake tables.

Check if row counts is the same

Check if column name and type are the same

Check if the count grouped by column is the same for each column 

Summary is printed out in console. It is optional to save details of results to excel. The results could be used for touble shooting. 

## Installation

git clone https://github.com/mliu-ext/table-profiler.git

setup snowflake account config in snowflake_config.py. External brower SSO authentication is used. No password is needed in config.

setup up python environment

run the following command to install required packages:

`pip install -r requirements.txt`

If you have conda installed you can also run the following command

```
conda env create -f requirements.yml
conda activate table_profiler
```

## Run table profiler or comparison tool

Note: table name should be passed in with the db name and schema name in the following format

`db_name.schema_name.table_name`

### run table profile tool tbl_profiler_sn.py

`python tbl_profiler_sn.py full_path_table_name`

run below cmd if you want to save the results to an excel file

`python tbl_profiler_sn.py full_path_table_name -f file_path/filename.xlsx`

Example 

`python tbl_profiler_sn.py SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center -f test.xlsx`

### run table comparison tool tbl_compare.py

`python tbl_compare.py full_path_table_A_name full_path_table_B_name`

run comparison and save results to excel file

`python tbl_compare.py full_path_table_A_name full_path_table_B_name -f file_path/filename.xlsx`


Examples below

`python tbl_compare.py SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center`

`python tbl_compare.py RAW.JING_VIPER.RAW_VIPER_20201110 RAW.JING_VIPER.RAW_VIPER_20201110 -f test.xlsx`

`python tbl_compare.py RAW.JING_VIPER.RAW_VIPER_20201110 SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center`

