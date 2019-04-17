:: This batch file runs the Python and R scripts necessary to update the
:: FHABs interactive map and Online Data Portal

ECHO OFF

:: Python script to retrieve updates from the SQL database and export a .csv
python

:: R script to merge current and updated .csv files on the Open Data Portal
Rscript join_db_versions.R

:: Python script to export the merged .csv file from Rscript to the Open Data Portal
python