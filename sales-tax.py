import requests
import re
import time
import logging.config
import yaml
import os, sys

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

def CheckToGo(JSONdata):
	drink_list	= [
					'Apple Juice',
					'Bottled Water',
					'Coconut Juice',
					'Gatorade',
					'Perrier',
					'Snapple',
					'Starbucks Mocha',
					'Sweet Tea',
					'Green Tea',
					'Kerns Juice'
					]

	for transaction in JSONdata:
		# Loop through itemizations and find To-Go
		togo = False
		for item in transaction['itemizations']:
			if item['name'] == "To-Go":
				togo = True
			elif item['name'] == "Phone Order":
				togo = True

		if togo == True:
			# Loop through the transaction again and look for specific items
			for item in transaction['itemizations']:
				if item['name'] in drink_list:
					#print transaction['receipt_url']
					print item['name'] + " : $" + str(item['total_money']['amount']/100) + " : " + transaction['receipt_url']

if __name__ == "__main__":
	#setup_logging()
	config = setup_config()

	begin_time 	= config['begin_time']
	end_time 	= config['end_time']
	url 		= config['url']
	params 		= { 'begin_time' : begin_time, 'end_time' : end_time }
	headers 	= { 'Authorization' : config['auth_token'] }

	r = requests.get(url, headers=headers, params=params)

	CheckToGo(r.json())

	next_url = CheckHeaders(r.headers)

	while len(next_url):
		r = requests.get(next_url, headers=headers)
		try:
			CheckToGo(r.json())
		except Exception, err:
			print "Hit some error: "
			print err
			print r.headers
			continue
		next_url = CheckHeaders(r.headers)
		time.sleep(1) # Sleep for 1 second





