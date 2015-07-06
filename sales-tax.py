"""
curl 'https://connect.squareup.com/v1/me/payments?begin_time=2014-04-01T00:00:00Z&end_time=2014-04-03T00:00:00Z&limit=1' \
-H 'Authorization: Bearer YA38Wo6Z5VS9CH6bF-H6IQ'
"""

import requests
import re
import time

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

		if togo == True:
			# Loop through the transaction again and look for specific items
			for item in transaction['itemizations']:
				if item['name'] in drink_list:
					#print transaction['receipt_url']
					print item['name'] + " : $" + str(item['total_money']['amount']/100) + " : " + transaction['receipt_url']

if __name__ == "__main__":
	begin_time 	= '2015-01-01T00:00:00Z'
	end_time 	= '2015-04-01T00:00:00Z'
	url 		= 'https://connect.squareup.com/v1/me/payments'
	params 		= { 'begin_time' : begin_time, 'end_time' : end_time }
	headers 	= { 'Authorization' : 'Bearer YA38Wo6Z5VS9CH6bF-H6IQ' }

	

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





