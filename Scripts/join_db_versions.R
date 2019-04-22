)## Script to prevent writing over FHABs reports in the data base
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

# new_df= "FHAB_BloomReport_1_20190417_edit.csv"
# input_path= "Data"
# output_path = "Data"

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

#### Extract dates from Incident reports to get the most recent report date linked with the ID
#make_unique_date_id <- function(){

  ## Regex command can match date format mm/dd/yyy & yyy-dd-mm with 1 or 2 digit months and days and 2 or 4 digit years
  date_regex <- "[0-9]+(\\/|-)[0-9]{1,2}(\\/|-)[0-9]+"
  # [0-9]+ match numbers one or more times
  # (\\/|-) match delimiter / or - (have to escape the the / with \\)
  # [0-9]{1,2} match numbers one or two times only. This prevents phone numbers from getting extracted because they have three values in the middle field
  # repeat other matches for last field
  incident_dates <- str_extract_all(curr.df$IncidentInformation, date_regex)
  
  
  ## Transform into dataframe (need to have all list elements the same length)
  seq.max <- max(sapply(incident_dates, length)) # length of longest list element
  id_df <- as.data.frame(sapply(incident_dates, '[', seq(seq.max)), stringsAsFactors = FALSE) %>%  # makes all elements same length adding NAs after last value in element
    as_tibble() %>% 
    mutate_all(list(~mdy(.)))
  
  ## Check to make sure all dates in Incident Reports are in descending order, with newest dates preceding older dates
  int_df <- apply(id_df, 2, function(x) int_length(interval(x[1], x)) > 0)
  if(any(int_df == TRUE, na.rm= TRUE)){
    warning("Incident dates are out of order, check file to ensure output is correct")
  } else {
    rm(int_df)
  }
  
  # Transpose data frame and add other date columns from curr.df
  # Then calculate differences between different date values 
  # return TRUE if LatestIncident is most recent, and FALSE if not
  id_df_t <- t(id_df)[, 1:2] %>% 
    as_tibble() %>% 
    mutate(AlgaeBloomReportID= curr.df$AlgaeBloomReportID,
           ObservationDate= curr.df$ObservationDate,
           BloomLastVerifiedOn= curr.df$BloomLastVerifiedOn) %>% 
    rename(LatestIncident= V1) %>% 
    select(AlgaeBloomReportID, LatestIncident, ObservationDate, BloomLastVerifiedOn) %>% 
    mutate(Latest_v_ObsDate= int_length(interval(.$LatestIncident, .$ObservationDate)) <= 0, 
           Latest_v_BloomVer= int_length(interval(.$LatestIncident, .$BloomLastVerifiedOn)) <= 0,
           ObsDate_v_BloomVer= int_length(interval(.$ObservationDate, .$BloomLastVerifiedOn)) <= 0)
  
  
  # Create a unique ID by concatenating the AlgaeBloomReportID with the date of most recent observation
  unique_IDs <- id_df_t %>% 
    mutate(AlgaeBloomReportID_Unique= ifelse(is.na(.$Latest_v_ObsDate), str_c(.$AlgaeBloomReportID, str_replace_all(.$ObservationDate, "-", ""), sep= "_"), 
                         ifelse((.$Latest_v_ObsDate == TRUE & .$Latest_v_BloomVer == TRUE), 
                                str_c(.$AlgaeBloomReportID, str_replace_all(.$LatestIncident, "-", ""), sep= "_"), 
                                ifelse((.$Latest_v_BloomVer == FALSE & .$ObsDate_v_BloomVer == FALSE), str_c(.$AlgaeBloomReportID, str_replace_all(.$BloomLastVerifiedOn, "-", ""), sep= "_"), 
                                       ifelse((.$Latest_v_BloomVer == TRUE & .$ObsDate_v_BloomVer == TRUE), str_c(.$AlgaeBloomReportID, str_replace_all(.$BloomLastVerifiedOn, "-", ""), sep= "_"), str_c(.$AlgaeBloomReportID, "XXXXXXXX", sep= "_"))))))
  
  
#}

## the file FHAB_BloomReport_1_20190417_edit.csv has one record  changed (1935) compared to the current .csv
new_fhabs_dataframe <- append_new_observations(new_df= "FHAB_BloomReport_1_20190417_edit.csv", input_path= "Data")


#write_csv(new_fhabs_dataframe, path = file.path("Data", str_c("FHAB_BloomReport_1_", format(Sys.Date(), "%Y%m%d"), ".csv")))




