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
  #str(df_with_ids)
}

append_new_observations <- function(prev_df, input_path){
  suppressPackageStartupMessages(require(tidyverse))
  suppressPackageStartupMessages(require(lubridate))
  suppressPackageStartupMessages(require(dataCompareR))
  
  ## Read in the data on the FHABs Open Data Portal
  #odp.df <- read_csv("https://data.ca.gov/sites/default/files/FHAB_BloomReport_1.csv") 
  odp.df <- suppressMessages(read_csv("S:/OIMA/SHARED/Freshwater HABs Program/FHABs Database/Python_Output/FHAB_BloomReport.csv"))
  
  ## Create AlgaeBloomReportID_Unique column
  odp.df.id <- make_unique_date_id(df= odp.df) 
  
  ## Read in the previous .csv file, with data prior to the current update
  prev.df <- suppressMessages(read_csv(file.path(input_path, prev_df)))
  
  ## Date format can be variable, these two if statements get date columns in prev.df into YYYY-MM-DD format.
  if(any(str_detect(prev.df$ObservationDate, "[0-9]+-[0-9]{1,2}-[0-9]+")) == TRUE){ # check for YYYY-MM-DD format
    prev.df <- prev.df
  }
  
  if(any(str_detect(prev.df$ObservationDate, "[0-9]+\\/[0-9]{1,2}\\/[0-9]+")) == TRUE){ # check for MM/DD/YYYY format
    prev.df <- prev.df %>% 
      mutate(ObservationDate= mdy(.$ObservationDate),
             BloomLastVerifiedOn= mdy(.$BloomLastVerifiedOn))
  }
  
  # Add unique ID column
  if(any(str_detect(names(prev.df), "AlgaeBloomReportID_Unique")) == FALSE){
    prev.df.id <- make_unique_date_id(prev.df)
  } else {
    prev.df.id <- prev.df
  }
  
  #### 1) Find unique rows in prev.df.id, but not in odp.df.id ####
  # prev.unique.rows <- dataCompareR:::matchRows(prev.df.id, odp.df.id, indices= "AlgaeBloomReportID_Unique")[[3]][[1]][, 1] %>%   ## select 2nd element in 3rd element of output list. Then extract 1st column and transform to character vector
  #  as.character(.)

 
  # prev.unique.df <- prev.df.id %>% 
  #  filter(AlgaeBloomReportID_Unique %in% prev.unique.rows)
 
   prev.unique.df <-  prev.df.id[(prev.df.id$AlgaeBloomReportID_Unique %in% odp.df.id$AlgaeBloomReportID_Unique) == FALSE, ]
   

  #### 2) Find rows with mis-matches ####
  mis_matches <- rCompare(prev.df.id, odp.df.id, keys= "AlgaeBloomReportID_Unique") # rCompare function in package dataCompareR
  
  ## Extract the mis-matched rows from prev.df (these are the rows that have the same AlgaeBloomReportID_Unique as prev.df.id, but have changes made in the row)
     ## This may return no results, in which case all the changes are recorded in the prev.unique.df
  mis_matches.df <- generateMismatchData(mis_matches, prev.df.id, odp.df.id)
 
  if(is.null(mis_matches.df) == FALSE){
    ## Check to make sure the uniqueID is truly unique
    ## If changes were made to a row, and no date information was updated then the ID_Unique will not have changed
    check_duplicate_ID_Unique <- function(){
      
      ## Get vector of ID_Unique from prev.df that will be appended
      mis_match.prev.ID_UNIQUE <- mis_matches.df[["prev.df.id_mm"]]$ALGAEBLOOMREPORTID_UNIQUE
      
      ## Check to see if the ID_Unique from prev.df are duplicated in odp.df (this would mean changes were made to the ODP, but were not represented with a new ID_Unique)
      duplicate.row <- odp.df.id$AlgaeBloomReportID_Unique %in% mis_match.prev.ID_UNIQUE
      duplicate.ID <- odp.df.id[duplicate.row, ]$AlgaeBloomReportID_Unique
      
      ## Replace the ID_Unique in odp.df with todays date, since no other date information was provided when the person changed the report
      odp.df.id[duplicate.row, ]$AlgaeBloomReportID_Unique <- str_replace(duplicate.ID, "[^_]+$", format(Sys.Date(), "%Y%m%d"))
      return(odp.df.id)
    }
    odp.df.id <- check_duplicate_ID_Unique()
    
    ## Reformat mis-matches into a dataframe
    mismatch.rows <- generateMismatchData(mis_matches, prev.df.id, odp.df.id)[[str_c("prev.df.id", "_mm")]]
    names(mismatch.rows) <- names(prev.df.id)
  }
  
  
  ## Append the rows mis-match rows from prev.df to the ODP data frame
  if(exists("mismatch.rows") == TRUE){
    output.df <- rbind(odp.df.id, prev.unique.df, mismatch.rows) %>% 
      arrange(AlgaeBloomReportID_Unique)
  } 
  
  if(exists("mismatch.rows") == FALSE){
    output.df <- rbind(odp.df.id, prev.unique.df) %>% 
      arrange(AlgaeBloomReportID_Unique)
  }

  return(output.df)
}

## Define pathways
#inputPATH <- "Data"
shared.drive.path <- file.path("S:", "OIMA", "SHARED", "Freshwater HABs Program", "FHABs Database") # Path to shared S drive
inputPATH <- shared.drive.path
most_recent_file <- max(list.files(inputPATH, pattern = "FHAB_BloomReport_1-.*csv"))
output_path <- shared.drive.path

## Run function
new_fhabs_dataframe <- append_new_observations(prev_df= most_recent_file, input_path= inputPATH)

## Write CSV locally to computer
write_csv(new_fhabs_dataframe, path = file.path(output_path, str_c("FHAB_BloomReport_1-", format(Sys.Date(), "%Y%m%d"), ".csv")))

