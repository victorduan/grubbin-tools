from helper import MySqlHelper
from datetime import datetime
import logging.config
import yaml
import re
import os, sys
import time

def setup_logging(
    default_path='logging.yml', 
    default_level=logging.INFO
):
    """Setup logging configuration

    """
    path = default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

def setup_config(
	default_path='config.yml'
):
	# Load config files
	try:
		if len(sys.argv) > 1:
			filename = sys.argv[1]
		else:
			filename = default_path
		f = open(filename)
		config = yaml.safe_load(f)
		f.close()

		return config

	except Exception, err:
		logging.exception(err)
		sys.exit(1)

def insert_tips(data):
	query = """
		INSERT IGNORE INTO `warehouse`.`tips_transaction`
		(
		`composite_id`,
		`transaction_id`,
		`employee_id`,
		`tips_collected`,
		`tips_split`,
		`transaction_date`
		)
		VALUES
		(
		%(composite_id)s,
		%(transaction_id)s,
		%(employee_id)s,
		%(tips_collected)s,
		%(tips_split)s,
		%(transaction_date)s
		);
	"""
	for batch in data_batcher(data):
		bulk_insert(batch, query)

def data_batcher(data, batchSize=200):
	# Generic function to split items into batches
	current_batch = []

	for item in data:
		current_batch.append(item)
		if len(current_batch) == batchSize:
			yield current_batch
			current_batch = []
	if current_batch:
		yield current_batch

def bulk_insert(data, query):
	# Open a connection
	db.connect()

	for item in data:
		qr = query % item
		logging.debug(' '.join(qr.split()))
		db.execute_query(query, item)

	# Properly close the connection
 	if len(data): 
		db.commit()
	else: 
		db.close()

if __name__ == "__main__":

	setup_logging()

	config = setup_config()

	# Connect to MySQL
	db = MySqlHelper(config['user'], config['pass'], config['host'], config['db'])
	db.connect()

	# Define date to run
	try:
		date = {'date' : datetime.strptime('2015-07-05', '%Y-%m-%d') }
	except ValueError, err:
		print "Hit error trying to convert date: " + str(err)
		sys.exit(1)
	
	print date

	# Check timesheet for anyone working
	query = "SELECT emp_id, clock_in, clock_out FROM timesheets WHERE `date` = '%(date)s'"

	results = db.select_query(query % date)

	# Construct list of everyone who has clocked in
	employee_timesheets = []
	for (emp_id, clock_in, clock_out) in results:
		emp = {
			'emp_id'	: emp_id,
			'clock_in'	: clock_in,
			'clock_out'	: clock_out
		}
		employee_timesheets.append(emp)

	# Clean tips table to ensure no overlapping data

	# Grab transactions from day
	query = """
	SELECT CONVERT_TZ(created_at,'UTC','America/Los_Angeles') as local_time, 
	transaction_id, tips_collected, tender_type 
	from transactions where CONVERT_TZ(created_at,'UTC','America/Los_Angeles')  between '%(date)s' and date_add('%(date)s', interval 1 day)
	and net_collected > 0 and tips_collected > 0
	"""

	transactions = db.select_query(query % date)
	
	#print transactions
	
	# Iterate over each transaction and prepare data to be inserted into tips_transaction table
	tips_data = []
	for (local_time, transaction_id, tips_collected, tender_type) in transactions:
		data = {
			'local_time' : local_time,
			'transaction_id' : transaction_id,
			'tips_collected' : tips_collected,
			'tender_type'	: tender_type
		}

		temp_list = []
		for employee in employee_timesheets:
			if employee['clock_in'] < local_time < employee['clock_out']:
				tip_data = {
					'composite_id'		: transaction_id + "-" + str(employee['emp_id']),
					'transaction_id'	: transaction_id,
					'employee_id'		: str(employee['emp_id']),
					'tips_collected'	: tips_collected,
					'transaction_date'	: date['date']
				}
				temp_list.append(tip_data)

		# Calculate split 
		for x in range(0,len(temp_list)):
			temp_list[x]['tips_split'] = temp_list[x]['tips_collected']/len(temp_list)
			#print temp_list[x]
			tips_data.append(temp_list[x])

	# Send data to be inserted
	if len(tips_data):
		insert_tips(tips_data)

