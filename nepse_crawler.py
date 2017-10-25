#!/usr/bin/env python

'''
@author: Sailesh Dhungana

nepse_crawler.py is a python script that crawls through historical
floor sheets of every company listed on NEPSE and records all raw data 
from all companies into all_stock_data.csv

Requirements:
1. python 2.7
2. BeautifulSoup 4
3. html5lib 1.0b3

Usage:
From directory containing this file and an empty directory
called data_from_NEPSE , run this command:

python nepse_crawler.py

'''

import urllib2
from bs4 import BeautifulSoup
import datetime
from os import path
import time

def getStocksAndNumbers():
	'''
	Parses NEPSE companies page and gets corresponding number for every company
	which can later be used to scrape historical data of each company.
	Returns a dictionary with stock symbols as keys and corresponding
	numbers as values.
	'''

	url_path = "http://www.nepalstock.com.np/company?_limit=500" 
	symbol_dict = {}

	opener = urllib2.build_opener()
	opener.addheaders = [('User-agent', 'Mozilla/5.0')]
	web_page = opener.open(url_path)
	soup = BeautifulSoup(web_page.read(), 'html5lib')
	table = soup.find('table')
	col = 0
	r = 0
	for row in table.findAll('tr'):
		# Skip the first two rows (One is filtering, One is labels)
		r += 1
		if r <= 2: 
			continue
		for data in row.findAll('td'):
			# Skip pager
			if data.find('div', {'class': 'pager'}):
				break	
			if col == 3: #Stock symbol column
				symbol = data.text
				col += 1
			elif col == 5: #Stock link column
				if data.a:
					# Get symbol number from the link 
					symbol_num = data.a.get('href').split('/')[-1]
					symbol_dict[symbol] = str(symbol_num)
				col = 0
			else:
				col += 1
	return symbol_dict


class CompanyCrawler(object):
	'''
	CompanyCrawler can crawl through NEPSE's individual company's historical
	floor sheet and records raw as well as processed data.
	'''

	def __init__(self, symbol, symbol_num):
		self.symbol = symbol
		self.symbol_num = symbol_num
		self.url_path = "http://www.nepalstock.com.np/company/transactions/%s?\
startDate=2010-01-01&endDate=%s&_limit=500\
" %(symbol_num, datetime.date.today().strftime('%Y-%m-%d'))
		self.next_page = 2
		self.raw_output = ""

	def crawl(self):
		opener = urllib2.build_opener()
		opener.addheaders = [('User-agent', 'Mozilla/5.0')]
		webPage = opener.open(self.url_path)
		soup = BeautifulSoup(webPage.read(), 'html5lib')
		raw_output = ""
		for_processing = []
		more = True # is there a next page?
		while more:
			more = False
			table = soup.find('table')
			col = 0
			r = 0
			for row in table.findAll('tr'):
				# Skip the first two rows (One is filtering, One is labels)
				r += 1
				if r <= 2: 
					continue
				for data in row.findAll('td'):
					# Skip pager
					if data.find('div', {'class': 'pager'}):
						break
					if col == 0: 
						col += 1 # Skip S.N.
					elif col == 1:
						raw_output += data.text + ',' + data.text[:4] + '-' + data.text[4:6] + '-' + data.text[6:8]+ ','
						col += 1
					elif col == 7: # Last column
						raw_output += data.text + '\n'
						col = 0
					else:
						raw_output += data.text + ','
						col += 1

			# For next pages
			for anchor in soup.findAll('a'):
				if str(self.next_page) == anchor.text:
					opener = urllib2.build_opener()
					opener.addheaders = [('User-agent', 'Mozilla/5.0')]
					urlPath = anchor['href']
					webPage = opener.open(urlPath)
					soup = BeautifulSoup(webPage, 'html5lib')
					more = True
					self.next_page += 1
					break
		
		self.raw_output = raw_output
		self.next_page = 2 # To reset nextPage in case crawl is used again

	def writeAllRawOutput(self):
		'''
		Writes all the raw outputs of all companies in all.csv file.
		'''

		out_file = 'data_from_NEPSE/all_stock_data.csv' 

		with open(out_file, 'ab') as output:
			output.write(self.raw_output)


if __name__ == '__main__':
	print 'Starting Program...'
	print 'Getting Company List...'
	symbol_dict = getStocksAndNumbers()
	print 'Company List Obtained.'

	out_file = 'data_from_NEPSE/all_stock_data.csv' 
	with open(out_file, 'wb') as output:
		output.write('Contract_No, Date, Stock_Symbol, Buyer_Broker, Seller_Broker, Quantity, Rate, Amount\n')

	i = 1 
	for symbol_tuple in symbol_dict.items():
		symbol = symbol_tuple[0]
		print 'Processing for %s' %symbol
		symbol_num = symbol_tuple[1]
		companyCrawler = CompanyCrawler(symbol, symbol_num)
		companyCrawler.crawl()
		companyCrawler.writeAllRawOutput()
		print '%s Processed (%s/%s)' %(symbol, i, len(symbol_dict))
		i += 1
	print 'The Program has successfully completed. Please find necessary files inside data_from_NEPSE folder'
