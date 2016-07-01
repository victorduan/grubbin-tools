from helper import MySqlHelper
import sys
import os
from datetime import datetime
import logging.config
import yaml

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
        f.close()
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
		with open(filename, 'r') as f:
			config = yaml.safe_load(f)
		f.close()

		return config

	except Exception, err:
		logging.exception(err)
		sys.exit(1)

def insert_timesheet(data):
	query = """
		INSERT INTO `warehouse`.`timesheets`
		(
		`emp_id`,
		`date`,
		`clock_in`,
		`clock_out`
		)
		VALUES
		(
		%(emp_id)s,
		%(date)s,
		%(clock_in)s,
		%(clock_out)s
		)
		ON DUPLICATE KEY UPDATE
		`clock_in` = %(clock_in)s,
		`clock_out` = %(clock_out)s
		;
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

	employees = config['employees']

	insert_list = []

	with open(sys.argv[2], 'r') as file_object:
		for line in file_object:
			if 'Employee' in line: 
				# Break the line up
				current_employee = line.split(',')[1]
				first_name = current_employee.split(' ')[0]
			else:
				# Try to check the first column to see if it is a date
				try:
					column = line.split(',')

					date 		= datetime.strptime(column[0], '%B %d %Y')
					clock_in 	= datetime.strptime(column[0] + " " + column[1], '%B %d %Y %I:%M%p')
					clock_out 	= datetime.strptime(column[2] + " " + column[3], '%B %d %Y %I:%M%p')

					print first_name
					print clock_in
					print clock_out

					data = {
						'date' : date,
						'emp_id'	: employees[first_name],
						'clock_in'	: clock_in,
						'clock_out' : clock_out
					}

					insert_list.append(data)
				except ValueError, err:
					continue

	file_object.close()

	if len(insert_list):
		# Connect to MySQL
		db = MySqlHelper(config['user'], config['pass'], config['host'], config['db'])
		db.connect()
		insert_timesheet(insert_list)
