from helper import MySqlHelper
import logging.config
import yaml
import requests
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

def prepare_transactions(data):
	transaction_list = []
	query = """
		INSERT IGNORE INTO `warehouse`.`transactions`
		(
		`transaction_id`,
		`merchant_id`,
		`created_at`,
		`creator_id`,
		`payment_url`,
		`receipt_url`,
		`tax_collected`,
		`tips_collected`,
		`discounts_applied`,
		`processing_fees`,
		`gross_sale`,
		`total_amount`,
		`refunded_money`,
		`net_collected`,
		`tender_type`,
		`card_brand`,
		`pan_suffix`,
		`entry_method`)
		VALUES
		(
		%(transaction_id)s,
		%(merchant_id)s,
		%(created_at)s,
		%(creator_id)s,
		%(payment_url)s,
		%(receipt_url)s,
		%(tax_collected)s,
		%(tips_collected)s,
		%(discounts_applied)s,
		%(processing_fees)s,
		%(gross_sale)s,
		%(total_amount)s,
		%(refunded_money)s,
		%(net_collected)s,
		%(tender_type)s,
		%(card_brand)s,
		%(pan_suffix)s,
		%(entry_method)s);

	"""


	for transaction in data:
		for tender in transaction['tender']:
			transaction_data = {
				'transaction_id' 	: transaction['id'],
				'merchant_id'		: transaction['merchant_id'],
				'created_at'		: transaction['created_at'],
				'creator_id'		: transaction['creator_id'],
				'payment_url'		: transaction['payment_url'],
				'receipt_url'		: tender['receipt_url'],
				'tax_collected'		: transaction['tax_money']['amount']/100.0,
				'tips_collected'	: transaction['tip_money']['amount']/100.0,
				'discounts_applied'	: transaction['discount_money']['amount']/100.0,
				'processing_fees'	: transaction['processing_fee_money']['amount']/100.0,
				'gross_sale'		: transaction['gross_sales_money']['amount']/100.0,
				'total_amount'		: tender['total_money']['amount']/100.0,
				'refunded_money'	: tender['refunded_money']['amount']/100.0,
				'net_collected'		: (tender['refunded_money']['amount']/100.0)+(tender['total_money']['amount']/100.0),
				'tender_type'		: tender['type'],
				'card_brand'		: tender['card_brand'] if tender['type'] == 'CREDIT_CARD' else 'NULL',
				'pan_suffix'		: tender['pan_suffix'] if tender['type'] == 'CREDIT_CARD' else 'NULL',
				'entry_method'		: tender['entry_method'] if tender['type'] == 'CREDIT_CARD' else 'NULL'
			}

			if transaction_data['total_amount'] > 0:
				transaction_list.append(transaction_data)
			else:
				continue

	return { "data" : transaction_list, "query" : query }

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

def CheckHeaders(headers):
	if 'link' in headers:
		p = re.compile('<(.*)>')
		m = p.match(r.headers['link'])
		next_url = m.group().replace("<", "")
		next_url = next_url.replace(">", "")
		#print r.headers['link']
		#return next_url
	else:
		next_url = ""
	
	return next_url

if __name__ == "__main__":

	setup_logging()

	config = setup_config()

	begin_time 	= config['begin_time']
	end_time 	= config['end_time']
	url 		= config['url']
	params 		= { 'begin_time' : begin_time, 'end_time' : end_time }
	headers 	= { 'Authorization' : config['auth_token'] }

	r = requests.get(url, headers=headers, params=params)

	prepared_transactions =	prepare_transactions(r.json())

	# Connect to MySQL
	db = MySqlHelper(config['user'], config['pass'], config['host'], config['db'])

	for batch in data_batcher(prepared_transactions['data']):
			bulk_insert(batch, prepared_transactions['query'])

	next_url = CheckHeaders(r.headers)

	while len(next_url):
		r = requests.get(next_url, headers=headers)
		try:
			prepared_transactions =	prepare_transactions(r.json())
			for batch in data_batcher(prepared_transactions['data']):
					bulk_insert(batch, prepared_transactions['query'])
		except Exception, err:
			print "Hit some error: "
			print err
			print r.headers
			continue
		next_url = CheckHeaders(r.headers)
		time.sleep(1) # Sleep for 1 second

