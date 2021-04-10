## run this file to compare two tables
# pass in table name with db name and schema name in the format db_name.schema_name.table_name
# python tbl_compare.py tableA tableB [-f full_file_path_to_save_data]

from utils import *
import argparse
from snowflake_config import SNOW_CONFIG
import time

def main(table_left, table_right, filename):
	# create snowflake connector
	con = sn_conn(SNOW_CONFIG)
	query = "select current_role(), current_warehouse()"
	info = pd.read_sql(query, con)

	# if filename.strip() == '':
 #        current_time = strftime("%Y-%m-%d_%H%M", gmtime()) 
 #        filename = '/Users/mliu-ext/downloads/{}_{}_vs_{}.xlsx'.format(current_time, table_left, table_right)
        
 #    print('\nResults will be saved in {}'.format(filename))

	print(info)

	left = TableStats(table_left, con)
	right = TableStats(table_right, con)

	TC = TableComp()
	res, df = TC.compare_all_cols(left, right, filename=filename)

	print("-------------------------------------comparison summary---------------------------------------------")
	print(res)

	con.close()

	return res, df


if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument("table_left", help="full path of table name for comparison, db.schema.table")
	parser.add_argument("table_right", help="full path of table name for comparison, db.schema.table")
	parser.add_argument("-f", "--file", help="filename.xlsx with full path to save the results")
	
	args = parser.parse_args()
	table_left = args.table_left
	table_right = args.table_right
	filename = args.file if args.file else ''	

	t1 = time.time()

	main(table_left, table_right, filename)
	
	print('It took {:.0f} seconds to run'.format(time.time() - t1))
