# Snowflake Table Profiler and Comparison Tool

table profiler: tbl_profile.py collect below metrics for each column of the table
RowCount
DistinctCount
NullCount
UniqueCount
Duplicatecount
Average
Max
Min
BlankCount


## Installation

First setup snowflake account config in snowflake_config.py. Use External authentication.

git clone https://github.com/mliu-ext/table-profiler.git

setup snowflake account config in snowflake_config.py. External authentication is used. No password is needed.

## setup up python environment

run the following command to install required packages:
`pip install requirements`

If you have conda installed you can also run the following command
`conda env create -f requirements.yml`

### run table profiler or comparison tool
After installing the necessary packages run the following in cml

table name should be passed in with the db name and schema name in the following format
db_name.schema_name.table_name

run table profile tool
`python tbl_profiler_sn.py full_path_table_name`

run below cmd if you want to save the results to an excel file
`python tbl_profiler_sn.py full_path_table_name -f file_path/filename.xlsx`

Example 
`python tbl_profiler_sn.py SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center -f /users/downloads/test.xlsx`


### run table comparison tool tbl_compare.py

run table comparison and print results
`python tbl_compare.py full_path_table_A_name full_path_table_B_name`

run comparison and save results to excel file
`python tbl_compare.py full_path_table_A_name full_path_table_B_name -f file_path/filename.xlsx`


Examples below

python tbl.py SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center

python tbl.py SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center -f /users/mliu-ext/test.xlsx

python tbl.py RAW.JING_VIPER.RAW_VIPER_20201110 RAW.JING_VIPER.RAW_VIPER_20201110

python tbl.py RAW.JING_VIPER.RAW_VIPER_20201110 SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center

