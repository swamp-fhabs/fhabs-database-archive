## Script to prevent writing over FHABs reports in the data base
## Keith Bouma-Gregson, keith.bouma-gregson@waterboarda.ca.gov
## Written April 2019

## The script checks the new .csv to be uploaded to the database with the current .csv
## on the database. Then any records that are different between the versions are identified
## Records that are different are then appended to the end of the .csv file, rather than writing over the previous record
## A new column called AlgaeBloomReportID_Date is created. This is a unique ID for each report and observation date combination
## The function then writes a .csv file of the new appended data frame

## The new_df comes from the Python script XX, which queries the SQL database and exports a .csv file with any updated bloom report information
## The .csv file from the Python script is then sent to the FHABs interactive map and the Online Data Portal
## FHABs data on Online Data Portal: https://data.ca.gov/dataset/surface-water-freshwater-harmful-algal-blooms

## ARGUMENTS
  # new_df: the .csv file exported from the Python script that includes the updated reports to the FHABs database
  # input_path: the complete path (From home directory) to the new_df .csf file
  # output_path: the complete path for where the output .csv of the appended data frame will be exported

new_df= "FHAB_BloomReport_1_20190417_edit.csv"
input_path= "Data"
output_path = "Data"

append_new_observations <- function(new_df, input_path){
  require(tidyverse)
  require(lubridate)
  require(dataCompareR)
  
  
  ## Read in the current data on the FHABs portal
  curr.df <- read_csv("https://data.ca.gov/sites/default/files/FHAB_BloomReport_1.csv") %>% 
    mutate(AlgaeBloomReportID_Date= str_c(.$AlgaeBloomReportID, str_replace_all(.$ObservationDate, "-", ""), sep= "_"))
  
  ## Read in the updated database that is exported from the Python script
  new.df <- read_csv(file.path(input_path, new_df))
  
  ## Date format can be variable, this gets date columns in new.df int YYYY-MM-DD format.
  if(any(is.na(as.Date(new.df$ObservationDate, format= "%Y-%m-%d"))) == FALSE){
    new.df <- new.df %>% 
      mutate(AlgaeBloomReportID_Date= str_c(.$AlgaeBloomReportID, str_replace_all(.$ObservationDate, "-", ""), sep= "_")) 
  }
  
  if(any(is.na(as.Date(new.df$ObservationDate, format= "%m/%d/%y"))) == FALSE){
    new.df <- new.df %>% 
      mutate(ObservationDate= mdy(.$ObservationDate),
             BloomLastVerifiedOn= mdy(.$BloomLastVerifiedOn)) %>% 
      mutate(AlgaeBloomReportID_Date= str_c(.$AlgaeBloomReportID, str_replace_all(.$ObservationDate, "-", ""), sep= "_")) 
  }
  
  ## Find rows with mis-matches. rCompare function in package dataCompareR
  mis_matches <- rCompare(new.df, curr.df, keys= "AlgaeBloomReportID")
  
  ## Extract the mis-matched rows as a data frame
  rows.to.append <- generateMismatchData(mis_matches, new.df, curr.df)[[str_c("new.df", "_mm")]]
  names(rows.to.append) <- names(new.df)
  
  # Append the new rows to the current data frame
  output.df <- rbind(curr.df, rows.to.append)
  return(output.df)
}


## the file FHAB_BloomReport_1_20190417_edit.csv has one record  changed (1935) compared to the current .csv
new_fhabs_dataframe <- append_new_observations(new_df= "FHAB_BloomReport_1_20190417_edit.csv", input_path= "Data")


write_csv(new_fhabs_dataframe, path = file.path("Data", str_c("FHAB_BloomReport_1_", format(Sys.Date(), "%Y%m%d"), ".csv")))




