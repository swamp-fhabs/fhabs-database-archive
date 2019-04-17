## Script to prevent writing over FHABs reports in the data base
## The script checks the new .csv to be uploaded to the database with the curren .csv
## on the database. Then any records that are different between the versions are identified
## Records that are different are then appended to the end of the .csv file, rather than writing over the previous record
## A new column called AlgaeBloomReportID_Date is created. This is a unique ID for each report and observation date combination

## Designed to be run from folder above "Data/XX.csv"


append_new_observations <- function(current, new){
  require(tidyverse)
  require(lubridate)
  require(dataCompareR)
  
  ## Read in data. t0= time zero and is the current
  curr.df <- read_csv(file = file.path("Data", current)) %>% 
    mutate(ObsDateR= mdy(.$ObservationDate)) %>% 
    mutate(AlgaeBloomReportID_Date= str_c(.$AlgaeBloomReportID, str_replace_all(.$ObsDateR, "-", ""), sep= "_")) %>% 
    select(-ObsDateR)
  
  new.df <- read_csv(file = file.path("Data", new)) %>% 
    mutate(ObsDateR= mdy(.$ObservationDate)) %>% 
    mutate(AlgaeBloomReportID_Date= str_c(.$AlgaeBloomReportID, str_replace_all(.$ObsDateR, "-", ""), sep= "_")) %>%
    select(-ObsDateR)
  
  ## Find rows with mis-matches. rCompare function in package dataCompareR
  mis_matches <- rCompare(new.df, curr.df, keys= "AlgaeBloomReportID")
  
  ## Extract the mis-matched rows as a data frame
  rows.to.append <- generateMismatchData(mis_matches, new.df, curr.df)[[str_c("new.df", "_mm")]]
  names(rows.to.append) <- names(new.df) # Match column names to preapre for rbind
  
  # Append the new rows to the current data frame
  output.df <- rbind(curr.df, rows.to.append)
  return(output.df)
}

## Changed record 1916 in T1 files
## Changed record 1899 and 1916 in T2 files


## the file test_T2 has two records changed compared to test_T0. 
## After you run the function output1 will have 2 extra rows
output1 <- append_new_observations(current= "test_T0.csv", new= "test_T2.csv")
write.csv(output1, file= "out1.csv")

## the file FHAB_BloomReport_T2.csv has two records changed compared to FHAB_BloomReport_T0.csv
## After you run the function output1 will have 2 extra rows
output2 <- append_new_observations(current= "FHAB_BloomReport_T0.csv", new= "FHAB_BloomReport_T1.csv")
write.csv(output2, file= "out2.csv")

## the file FHAB_BloomReport_T1.csv has two records changed compared to FHAB_BloomReport_T0.csv
## After you run the function output1 will have 2 extra rows
output3 <- append_new_observations(current= "FHAB_BloomReport_T0.csv", new= "FHAB_BloomReport_T2.csv")
write.csv(output3, file= "out3.csv")



