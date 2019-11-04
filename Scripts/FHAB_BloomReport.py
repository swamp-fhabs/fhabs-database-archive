'''

Author(s):
	Andrew Dix Hill; https://github.com/AndrewDixHill/CEDEN_to_DataCAGov ; andrew.hill@waterboards.ca.gov
	Michelle Tang; https://github.com/swamp-fhabs/fhabs-database-archive ; michelle.tang@waterboards.ca.gov

Agency:
	California State Water Resource Control Board (SWRCB)
	Office of Information Management and Analysis (OIMA)

Purpose:
	This script will query an internal WaterBoard dataMart for Fresh Water Harmful Algal Bloom data only. It will
	filter all data for a set of printable characters and then publish the data to a resource node on data.ca.gov.
	It will convert positive longitude values to negative and replace Latitude or Longitude values that are
	empty spaces. The file extension and delimiters are chosen specifically to work with data.ca.gov's preview
	function.

	UPDATE 9/18/19: On August 28, 2019, the CA Open Data Portal was migrated from DKAN to CKAN. The updated code 
	uploads data to the portal using the CKAN API.

How to use this script:
	-You must be connected to the internal Waternet
	-From a Powershell prompt (Windows), call Python and specify the complete path to this file. Below is an example, 
	where XXXXXXX should be replaced with the filename:

		python C:\\Users\\User***\\Downloads\\XXXXXXX.py

	-You must also set the SERVER, UID, base_path, sql, host, and key as environmental variables in your Windows account
	-You may also have to set the SQL driver on the pyodbc.connect line to your available drivers.
		Use pyodbc.drivers() to see a list of available drivers.

Prerequisites:
	-Windows platform (not strictly a requirement but I was unable to get the pyodbc library
		working on a Mac... I tried)
	-Python 3.X
	-pyodbc library for Python. See https://github.com/mkleehammer/pyodbc
	-ODBC Driver 11 for SQL Server, Microsoft product downloaded here: https://www.microsoft.com/en-us/download/details.aspx?id=36434
	-ckanapi library for Python. See https://github.com/ckan/ckanapi


'''

# import Python libraries
import pyodbc
import os
import csv
import re
from datetime import datetime
import string
import ckanapi
import requests
import getpass

# decodeAndStrip takes a string and filters each character through the printable variable. It returns a filtered string.
def decodeAndStrip(t):
	filter1 = ''.join(filter(lambda x: x in printable, str(t)))
	return filter1

if __name__ == "__main__":
	############   Set up environment variables for your computer   ##########
	SERVER = os.environ.get('FHAB_Server')
	UID = os.environ.get('FHAB_User')
	base_path = os.environ.get('CK_path') # output file location (parent) on internal shared drive
	sql = os.environ.get('CK_sql')
	host = os.environ.get('CK_host')
	key = os.environ.get('CK_key') 
	############   Change these   ##########
	printable = set(string.printable) - set('|"\`\t\r\n\f\v')
	############   Change this to save the file to a different location   ##########
	file_name = 'Python_Output'
	path = os.path.join(base_path, file_name)
	if not os.path.isdir(path):
		print('Creating new directory...')
		os.mkdir(path)
	FHAB = 'FHAB_BloomReport' # Name of the output file
	############   Ideally data.ca.gov will be able to generate data preview for tab delimited txt files... until then
	ext = '.csv' # data.ca.gov requires csv for preview functionality
	sep = ',' # delimiter type.
	file = os.path.join(path, FHAB + ext)
	print('Getting data...')
	cnxn = pyodbc.connect(Driver='ODBC Driver 11 for SQL Server', Server=SERVER, uid=UID, Trusted_Connection='Yes')
	cursor = cnxn.cursor()
	cursor.execute(sql)
	columns = [desc[0] for desc in cursor.description]
	print('Writing data...')
	with open(file, 'w', newline='', encoding='utf8') as writer:
		dw = csv.DictWriter(writer, fieldnames=columns, delimiter=sep, lineterminator='\n')
		dw.writeheader()
		FHAB_writer = csv.writer(writer, csv.QUOTE_MINIMAL, delimiter=sep, lineterminator='\n')
		for row in cursor:
			row = [str(word) if word is not None else '' for word in row]
			filtered = [decodeAndStrip(t) for t in list(row)]
			newDict = dict(zip(columns, filtered))
			try:
				long = float(newDict['Longitude'])
				if long > 0:
					newDict['Longitude'] = -long
			except ValueError:
				pass
			FHAB_writer.writerow(list(newDict.values()))

	############   Upload dataset to data.ca.gov   ##########
	ckan = ckanapi.RemoteCKAN(host, apikey=key)
	resource_info = ckan.action.resource_show(id='c6f760be-b94f-495e-aa91-2d8e6f426e11')
	print('Uploading to %s...' % host) 
	ckan.action.resource_update(id=resource_info['id'], upload=open(file,'rb'), format=resource_info['format'])
	print('Process finished.')


