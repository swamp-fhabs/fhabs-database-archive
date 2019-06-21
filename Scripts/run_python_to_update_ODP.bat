:: This batch file runs the two Python scripts necessary to update the FHABs interactive map and Online Data Portal.

@ECHO OFF

:: Python script to retrieve ALL updates from the SQL database and export a .csv locally
python "C:\Users\mtang\Documents\GitHub\fhabs-database-archive\Scripts\FHAB_BloomReport_to_Archive.py"


:: Python script to retrieve ONLY updates approved to make public from the SQL database and export a .csv locally AND to Open Data Portal
python "C:\Users\mtang\Documents\GitHub\fhabs-database-archive\Scripts\FHAB_BloomReport.py"


