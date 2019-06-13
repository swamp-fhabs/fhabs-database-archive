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
  require(tidyverse)
  require(lubridate)
  require(dataCompareR)
  
  ## Read in the data on the FHABs Open Data Portal
  #odp.df <- read_csv("https://data.ca.gov/sites/default/files/FHAB_BloomReport_2.csv") 
  odp.df <- suppressMessages(read_csv("S:/OIMA/SHARED/Freshwater HABs Program/FHABs Database/Python_Output/FHAB_BloomReport_Archive.csv")) %>% 
    mutate(ObservationDate= as.character(ObservationDate),
           BloomLastVerifiedOn= as.character(BloomLastVerifiedOn))
   
  ## Create AlgaeBloomReportID_Unique column
  odp.df.id <- make_unique_date_id(df= odp.df)
  
  ## Read in the previous .csv file, with data prior to the current update
  prev.df <- suppressMessages(read_csv(file.path(input_path, prev_df))) %>% 
    mutate(ObservationDate= as.character(ObservationDate),
           BloomLastVerifiedOn= as.character(BloomLastVerifiedOn))
  
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
  prev.unique.df <-  prev.df.id[(prev.df.id$AlgaeBloomReportID_Unique %in% odp.df.id$AlgaeBloomReportID_Unique) == FALSE, ]
   

  #### 2) Find rows with mis-matches ####
  mis_matches <- rCompare(prev.df.id, odp.df.id, keys= "AlgaeBloomReportID_Unique") # rCompare function in package dataCompareR
  
  ## Extract the mis-matched rows from prev.df (these are the rows that have the same AlgaeBloomReportID_Unique as prev.df.id, but have changes made in the row)
     ## This may return no results, in which case all the changes are recorded in the prev.unique.df
  mis_matches.df <- generateMismatchData(mis_matches, prev.df.id, odp.df.id)
  names(mis_matches.df[[1]]) <-  names(odp.df.id)
  names(mis_matches.df[[2]]) <-  names(odp.df.id)
  
  #### 3) Check the outputs ####
  
  ## Check to see if mismatched rows have already been documented in the prev.df 
  check_mismatch <- function(){
    
    
    # 1) Extract the row in odp.df.id of interest
    odp.row <- mis_matches.df[["odp.df.id_mm"]]
    
    
    
    for(id in odp.row$AlgaeBloomReportID_Unique){
      # 2) Extract all the rows in prev.df.id with the same algalreportID
      prev.rows <- filter(prev.df.id, prev.df.id$AlgaeBloomReportID %in% str_replace(id, "_.*$", ""))
      
      # 3) Select the most recent of the extracted rows in prev.df.id
      recent.prev.entry <- filter(prev.rows, AlgaeBloomReportID_Unique == max(AlgaeBloomReportID_Unique))
      
      # 4) Remove the AlgalreportID_Unique column from the extracted odp.df.id and prev.df.id (these will always be different)
      recent.prev.entry2 <- select(recent.prev.entry, -AlgaeBloomReportID_Unique)
      odp.row2<- odp.row %>% 
        filter(AlgaeBloomReportID_Unique == id) %>% 
        select(-AlgaeBloomReportID_Unique)
      
      # 5) Compare the two rows
      comp <- suppressMessages(rCompare(odp.row2, recent.prev.entry2))
      
      
      # 6) If they are the same then delete the row from the odp.df.id, because it is a duplicate of the row in prev.df.id
      if(is.null(suppressMessages(generateMismatchData(comp, odp.row2, recent.prev.entry2)))){
        # Initialize vector to store output
        if(!exists("ids.to.remove")){
          ids.to.remove <- as.character(NULL)
        }
        
        ids.to.remove <- c(ids.to.remove, id)
      }
      if(exists("ids.to.remove")){
        odp.row.export <- filter(odp.row, odp.row$AlgaeBloomReportID_Unique %in% ids.to.remove)
      } 
    }
    
    ## Output messages
    num_mismatches <-  length(odp.row$AlgaeBloomReportID_Unique)
    num_incorrect <- ifelse(exists("ids.to.remove"), length(ids.to.remove), 0)
    message(paste("Number of mismatched rows= ", num_mismatches, 
                  "\nNumber already documented and needing to be removed= ", num_incorrect))
    if(num_incorrect ==0){
      message("All mismatches are novel. No rows to remove")}
    
    ## Return vector of UniqueIDs for rows to remove
    if(exists("ids.to.remove") == FALSE){
      odp.row.export <- "All mismatches are novel. No rows to remove"
    }
    return(odp.row.export)
  }
  rows.to.remove <- check_mismatch()
  
  if(all(rows.to.remove != "All mismatches are novel. No rows to remove", na.rm=TRUE)){
    odp.df.id <- filter(odp.df.id, !(AlgaeBloomReportID_Unique %in% rows.to.remove$AlgaeBloomReportID_Unique))
  }
  
  
  ## Check to make sure the uniqueID is truly unique
  ## If changes were made to a row, and no date information was updated then the ID_Unique will not have changed
  if(is.null(mis_matches.df) != TRUE){
    
    check_duplicate_ID_Unique <- function(){
      
      ## Get vector of ID_Unique from prev.df that will be appended
      mis_match.prev.ID_UNIQUE <- mis_matches.df[["prev.df.id_mm"]]$AlgaeBloomReportID_Unique
      
      ## Check to see if the ID_Unique from prev.df are duplicated in odp.df (this would mean changes were made to the ODP, but were not represented with a new ID_Unique)
      duplicate.row <- odp.df.id$AlgaeBloomReportID_Unique %in% mis_match.prev.ID_UNIQUE
      duplicate.ID <- odp.df.id[duplicate.row, ]$AlgaeBloomReportID_Unique
      
      ## Replace the ID_Unique in odp.df with todays date, since no other date information was provided when the person changed the report
      odp.df.id[duplicate.row, ]$AlgaeBloomReportID_Unique <- str_replace(duplicate.ID, "[^_]+$", format(Sys.Date(), "%Y%m%d"))
      return(odp.df.id)
    }
    odp.df.id.no.duplicates <- check_duplicate_ID_Unique()
 
  }
  

  #### 4) Append the rows mis-match rows from prev.df to the ODP data frame ####
          # odp.df.id.no.duplicates= most recent data with no IDs that match prev.df.id
          # prev.unique.df= All the previously recorded rows that have since been overwritten by the database
          # mis_matches.df[["prev.df.id_mm"]]= the rows that had the same IDs between odp.df.id and prev.df.id, but contained different data
          # the check_duplicate_ID_Unique function updated IDs in the odp.df.id data frame, so that now there are unique IDs for all rows.
  
  if(exists("mis_matches.df") == TRUE){
    output.df <- rbind(odp.df.id.no.duplicates, prev.unique.df, mis_matches.df[["prev.df.id_mm"]]) %>% 
      arrange(AlgaeBloomReportID_Unique)
  } 
  
  if(exists("mis_matches.df") == FALSE){
    output.df <- rbind(odp.df.id.no.duplicates, prev.unique.df) %>% 
      arrange(AlgaeBloomReportID_Unique)
  }

  return(output.df)
}

## Define pathways
shared.drive.path <- file.path("S:", "OIMA", "SHARED", "Freshwater HABs Program", "FHABs Database") # Path to shared S drive
inputPATH <- shared.drive.path
most_recent_file <- max(list.files(inputPATH, pattern = "FHAB_BloomReport_1-.*csv"))
output_path <- shared.drive.path


## Run function
new_fhabs_dataframe <- append_new_observations(prev_df= most_recent_file, input_path= inputPATH)


## Write CSV locally to computer
write_csv(new_fhabs_dataframe, path = file.path(output_path, str_c("FHAB_BloomReport_1-", format(Sys.Date(), "%Y%m%d"), ".csv")))



