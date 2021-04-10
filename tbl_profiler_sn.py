#!/usr/bin/env python
# coding: utf-8

import sys
import pandas as pd
import json
from utils import *
from snowflake_config import SNOW_CONFIG
import time
import argparse

    
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("table_name", help="full path of table name, db_name.schema_name.table_name")
    parser.add_argument("-f", "--file", help="filename.xlsx with full path to save the results")
    args = parser.parse_args()

    table = args.table_name
    filename = args.file if args.file else ''    

    # Create snowflake connector
    con = sn_conn(SNOW_CONFIG)
    query = "select current_role(), current_warehouse()"
    info = pd.read_sql(query, con)

    print()
    print(info)

    t1 = time.time()

    tblStats = TableStats(table, con)
    tblStats.profile_all_columns(filename)

    con.close()

    print('It took {:.0f} seconds to run'.format(time.time() - t1))
