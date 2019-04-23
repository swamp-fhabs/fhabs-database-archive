'''

Author:
	Adapted by Keith Bouma-Gregson from FHAB_BloomReport.py written by
	Andrew Dix Hill; https://github.com/AndrewDixHill/CEDEN_to_DataCAGov ; andrew.hill@waterboards.ca.gov

	Keith Bouma-Gregson; keith.bouma-gregson@waterboards.ca.gov

Agency:
	California State Water Resource Control Board (SWRCB)
	Office of Information Management and Analysis (OIMA)

Purpose:
	This script will take the CSV output from FHAB_BloomReport.py and export it to the
	HABs database on the Open Data Portal at https://data.ca.gov/dataset/surface-water-freshwater-harmful-algal-blooms


How to use this script:
	You must be connected to the internal waternet
	From a powershell prompt (windows), call python and specify
	the complete path to this file. Below is an example, where XXXXXXX should be replaced
	with the filename:
	python C:\\Users\\User***\\Downloads\\XXXXXXX.py
	You must also set the SERVER, UID as environmental variables in your windows account
	You may also have to set the SQL driver on the pyodbc.connect line to your available drivers.
		Use pyodbc.drivers() to see a list of available drivers.

	Currently data.ca.gov cannot be able to generate data preview for tab delimited txt files, therefore
	a .csv file is require for Data.ca.gov preview functionality

Prerequisites:
	Windows platform (not strictly a requirement but I was unable to get the pyodbc library
		working on a mac... I tried)
	Python 3.X
	pyodbc library for python.  See https://github.com/mkleehammer/pyodbc
	dkan library for python.    See https://github.com/GetDKAN/pydkan
	ODBC Driver 11 for SQL Server, Microsoft product downloaded here: https://www.microsoft.com/en-us/download/details.aspx?id=36434


'''
## Import the necessary libraries of python code
import os
import re
#from datetime import datetime
#import string
from dkan.client import DatasetAPI
import getpass

## Set up username and passwords for local comuter
user = os.environ.get('DCG_user')
password = os.environ.get('DCG_pw')
URI = os.environ.get('URI')

## Specify file path for CSV
first = 'C:\\Users\\%s\\Documents' % getpass.getuser()
path = os.path.join(first, 'FHAB_BloomReport')


## Define filename, .csv extension, and delimiter
FHAB = 'FHAB_BloomReport' 	# Name of output file
ext = '.csv'
sep = ','
file = os.path.join(path, FHAB + ext) # make file path for where file will be stored locally

## Export CSV file to the Online Data Portal
  # Node number = 2446 for the FHAB portal data (previously 2156)
NODE = 2446
api = DatasetAPI(URI, user, password, debug=False)
r = api.attach_file_to_node(file=file, node_id=NODE, field='field_upload', update=0)