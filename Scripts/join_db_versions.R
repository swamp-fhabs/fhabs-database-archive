## Script to prevent writing over FHABs reports in the SWAMP FHABs data base

## Keith Bouma-Gregson, keith.bouma-gregson@waterboarda.ca.gov
## Written April 2019

## The function append_new_observations() checks the new .csv to be uploaded to the database with the current .csv
## on the database. Then any records that are different between the versions are identified
## Records that are different are then appended to the end of the .csv file, rather than writing over the previous record
## A new column called AlgaeBloomReportID_Unique is created. This is a unique ID for each report and observation date combination
## The function then writes a .csv file of the new appended data frame

## append_new_observations() arguments
  # new_df: the .csv file exported from the Python script that includes the updated reports to the FHABs database
    # The new_df comes from the Python script XX, which queries the SQL database and exports a .csv file with any updated bloom report information
    # The .csv file from the Python script is then sent to the FHABs interactive map and the Online Data Portal
    # FHABs data on Online Data Portal: https://data.ca.gov/dataset/surface-water-freshwater-harmful-algal-blooms
  # input_path: the complete path (From home directory) to the new_df .csv file

## make_unique_date_id() 
  # function creates unique record identities based on the date of the latest report
  # this function is called within the append_new_observations function
  # df= the FHABs database data frame

make_unique_date_id <- function(df){
  ## Regex command can match date format mm/dd/yyy & yyy-dd-mm with 1 or 2 digit months and days and 2 or 4 digit years
  date_regex <- "[0-9]+(\\/|-)[0-9]{1,2}(\\/|-)[0-9]+"
  # [0-9]+ match numbers one or more times
  # (\\/|-) match delimiter / or - (have to escape the the / with \\)
  # [0-9]{1,2} match numbers one or two times only. This prevents phone numbers from getting extracted because they have three values in the middle field
  # repeat other matches for last field
  incident_dates <- str_extract_all(df$IncidentInformation, date_regex)
  
  
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
  
  # Transpose data frame and add other date columns from df
  # Then calculate differences between different date values 
  # return TRUE if LatestIncident is most recent, and FALSE if not
  id_df_t <- t(id_df)[, 1:2] %>% 
    as_tibble() %>% 
    mutate(AlgaeBloomReportID= df$AlgaeBloomReportID,
           ObservationDate= df$ObservationDate,
           BloomLastVerifiedOn= df$BloomLastVerifiedOn) %>% 
    rename(LatestIncident= V1) %>% 
    select(AlgaeBloomReportID, LatestIncident, ObservationDate, BloomLastVerifiedOn) %>% 
    mutate(Latest_v_ObsDate= int_length(interval(.$LatestIncident, .$ObservationDate)) <= 0, 
           Latest_v_BloomVer= int_length(interval(.$LatestIncident, .$BloomLastVerifiedOn)) <= 0,
           ObsDate_v_BloomVer= int_length(interval(.$ObservationDate, .$BloomLastVerifiedOn)) <= 0)
  
  
  # Create a unique ID by concatenating the AlgaeBloomReportID with the date of most recent observation
  unique_IDs <- id_df_t %>%   
    mutate(AlgaeBloomReportID_Unique= if_else((is.na(.$Latest_v_ObsDate) & is.na(.$ObsDate_v_BloomVer) & !is.na(.$ObservationDate)), 
                                              str_c(.$AlgaeBloomReportID, str_replace_all(.$ObservationDate, "-", ""), sep= "_"),
                                              if_else((is.na(.$Latest_v_ObsDate) & .$ObsDate_v_BloomVer == TRUE), 
                                                      str_c(.$AlgaeBloomReportID, str_replace_all(.$ObservationDate, "-", ""), sep= "_"),
                                                      if_else((is.na(.$Latest_v_ObsDate) & .$ObsDate_v_BloomVer == FALSE),
                                                              str_c(.$AlgaeBloomReportID, str_replace_all(.$BloomLastVerifiedOn, "-", ""), sep= "_"), 
                                                              if_else((.$Latest_v_ObsDate == TRUE & .$Latest_v_BloomVer == TRUE & !is.na(.$LatestIncident)), 
                                                                      str_c(.$AlgaeBloomReportID, str_replace_all(.$LatestIncident, "-", ""), sep= "_"), 
                                                                      if_else((.$Latest_v_BloomVer == FALSE & .$ObsDate_v_BloomVer == FALSE), 
                                                                              str_c(.$AlgaeBloomReportID, str_replace_all(.$BloomLastVerifiedOn, "-", ""), sep= "_"), 
                                                                              if_else((.$Latest_v_BloomVer == TRUE & .$ObsDate_v_BloomVer == TRUE & !is.na(.$BloomLastVerifiedOn)), 
                                                                                      str_c(.$AlgaeBloomReportID, str_replace_all(.$BloomLastVerifiedOn, "-", ""), sep= "_"),
                                                                                      str_c(.$AlgaeBloomReportID, "XXXXXXXX", sep= "_")))))))) %>% 
  mutate(AlgaeBloomReportID_Unique= as.character(AlgaeBloomReportID_Unique))
  
  
  ## Combine unique IDs with original data frame
  df_with_ids <-  data.frame(AlgaeBloomReportID_Unique= unique_IDs$AlgaeBloomReportID_Unique, df, stringsAsFactors = FALSE)# %>% 
    
  return(df_with_ids)
  str(df_with_ids)
}

append_new_observations <- function(new_df, input_path){
  require(tidyverse)
  require(lubridate)
  require(dataCompareR)
  
  ## Read in the data on the FHABs Open Data Portal
  odp.df <- read_csv("https://data.ca.gov/sites/default/files/FHAB_BloomReport_1.csv") 
  
  ## Create AlgaeBloomReportID_Unique column
  odp.df.id <- make_unique_date_id(odp.df) 
  
  ## Read in the updated database that is exported from the Python script
  prev.df <- read_csv(file.path(input_path, prev_df))
  
  ## Date format can be variable, these two if statements get date columns in prev.df into YYYY-MM-DD format.
  if(any(str_detect(prev.df$ObservationDate, "[0-9]+-[0-9]{1,2}-[0-9]+")) == TRUE){ # check for YYYY-MM-DD format
    prev.df <- prev.df
  }
  
  if(any(str_detect(prev.df$ObservationDate, "[0-9]+\\/[0-9]{1,2}\\/[0-9]+")) == TRUE){ # check for MM/DD/YYYY format
    prev.df <- prev.df %>% 
      mutate(ObservationDate= mdy(.$ObservationDate),
             BloomLastVerifiedOn= mdy(.$BloomLastVerifiedOn))
  }
  
  
  # if(any(is.na(as.Date(prev.df$ObservationDate, format= "%m/%d/%y"))) == FALSE){
  #   prev.df2 <- prev.df %>% 
  #     mutate(ObservationDate= mdy(.$ObservationDate),
  #            BloomLastVerifiedOn= mdy(.$BloomLastVerifiedOn))# %>% 
  #   #mutate(AlgaeBloomReportID_Date= str_c(.$AlgaeBloomReportID, str_replace_all(.$ObservationDate, "-", ""), sep= "_")) 
  # }
  
  # Add unique ID column
  if(any(str_detect(names(prev.df), "AlgaeBloomReportID_Unique")) == FALSE){
    prev.df.id <- make_unique_date_id(prev.df)
  } else {
    prev.df.id <- prev.df
  }
  
  ## Find rows with mis-matches 
  mis_matches <- rCompare(prev.df.id, odp.df.id, keys= "AlgaeBloomReportID") # rCompare function in package dataCompareR
  
  ## Extract the mis-matched rows from prev.df (these are the rows that were overwritten when exported to Open Data Portal)
  rows.to.append <- generateMismatchData(mis_matches, prev.df.id, odp.df.id)[[str_c("prev.df.id", "_mm")]]
  names(rows.to.append) <- names(prev.df.id)
  
  ## Append the rows mis-match rows from prev.df to the ODP data frame
  output.df <- rbind(odp.df.id, rows.to.append) %>% 
    arrange(AlgaeBloomReportID_Unique)
  return(output.df)
}

new_fhabs_dataframe <- append_new_observations(prev_df= "FHAB_BloomReport_1-20190423.csv", input_path= "Data")

## Write CSV locally to computer
output_path <- "Data"
write_csv(new_fhabs_dataframe, path = file.path(output_path, str_c("FHAB_BloomReport_1-", format(Sys.Date(), "%Y%m%d"), ".csv")))




