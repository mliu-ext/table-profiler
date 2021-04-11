
from utils import *
import argparse
from snowflake_config import SNOW_CONFIG


if __name__ == '__main__':

	# create snowflake connector
	con = sn_conn(SNOW_CONFIG)
	# con.cursor().execute("USE WAREHOUSE ETL_WH")

	query = "select current_role(), current_warehouse(), current_database(), current_schema()"

	info = pd.read_sql(query, con)
	print(info)


	table = 'SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.Call_Center'	

	# print(pd.read_sql("desc table {}".format(table), con))

	TS = TableStats(table, con)

	print(TS.get_column_names_with_type())

	TC = TableComp()
	df = TC.compare_group_count(TS.get_column_names_with_type(), TS.get_column_names_with_type())
	print(df)

	print("describe table {}".format(table))
	try:
		pd.read_sql("desc table {}".format(table), con)
	except:
		# raise SystemExit('error in code want to exit')
		# raise Exception('Failed to open database')
		# raise ValueError('A very specific bad thing happened.')
		print(sys.exc_info())
		sys.exit("could NOT describe table {}. Check if table name is correct and in the format: db_name.schema_name.table_name".format(table))

	print('hello')

	
	# test comparison of goupby by column results
	TC = TableComp()
	dfa = pd.DataFrame([['a', 1], ['b',2]], columns=['value', 'cnt2'])
	dfb = pd.DataFrame([['a', 3], ['b',2]], columns=['value', 'cnt2'])
	res, df = TC.compare_group_count(dfa, dfb)
	print("---------------------test should fail------------------------")
	print(res)
	print(df)

	dfa = pd.DataFrame([['a', 1], ['b',2]], columns=['value', 'cnt'])
	dfb = pd.DataFrame([['a', 3], ['b',2]], columns=['value', 'cnt'])
	res, df = TC.compare_group_count(dfa, dfb)
	print("---------------------test should fail------------------------")
	print(res)
	print(df)

	dfa = pd.DataFrame([['a', 3], ['b',2]], columns=['value', 'cnt'])
	dfb = pd.DataFrame([['a', 3], ['b',2]], columns=['value', 'cnt'])
	res, df = TC.compare_group_count(dfa, dfb)
	print("---------------------test should pass------------------------")
	print(res)
	print(df)

	con.close()