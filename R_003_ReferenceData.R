library(tidyverse)
library(arrow)
library(here)
library(googlesheets4)

source('R_INCLUDE_Functions_Reference.R')

# -------------------------------------------------------------------------
# CREATE PARQUET FILES FROM REFERENCE DATA TO UPLOAD TO AWS FOR TEST CALLS DATASET
#
# UPDATE THE AWS SHEET WITH THE MOST RECENT DATA
# step 1. move tabs from brians sheet to the aws sheet, 
#         checking the data integrity in the process
# step 2. run the process that updates from okta into the aws sheet
# step 3. run the process that updates from lss into the aws sheet
#
# OUTPUT TO PARQUET FILE ON G:
# step 4. export from aws sheet tab to a parquet file on G:
# step 5. manually export to AWS

# -------------------------------------------------------------------------
# pointers

url_list <- fn_ref_url()

# google source 
url_aws <- url_list$url_aws
url_brian <- url_list$url_brian
path_parquet <- url_list$url_path
path_backup <- paste0(path_parquet,'Backup Versions/')

# Update from okta and LSS into aws workbook -----------------------------------

# OKTA
# 'G:/Shared drives/CA - Data Science Team/Data Products + Services/Adviser Data Service/Okta Daily Data Exports/OKTA-ReferenceData/'
# run R_103
# okta_id,	ref.okta.advisername,	ref.okta.member,	ref.okta.office,	upload_date
# writes direct to aws google sheet and Brians sheet
df_aws_oktaadvisers <- fn_get_aws_refdata('reference_oktaadvisers', url_aws)
glimpse(df_aws_oktaadvisers)
# save a new parquet file
write_parquet(df_aws_oktaadvisers, paste0( path_parquet,'reference_oktaadvisers.parquet'))

# LSS
# G:/Shared drives/CA - Data Science Team/Data Products + Services/Network Data Service/RAP Reference Data/MVP-MBR-Reference-Data/
# run R_MBR_ReferenceData.R
# member_aws, ref.lss.fullname, ref.lss.shortname, upload_date
# writes direct to aws google sheet and brians sheet
df_aws_mbr <- fn_get_aws_refdata('reference_mbr', url_aws)
glimpse(df_aws_mbr)
# save a new parquet file
write_parquet(df_aws_mbr, paste0( path_parquet,'reference_mbr.parquet'))

# -------------------------------------------------------------------------
# Update from Brians google workbook to the aws
# check that the data is clean in the process
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# get Brians sheets - phone numbers --------------------------------------------
# check for referential integrity
# check nothing currently in aws in replaced or missed
df_phonenumbers <- fn_get_source_refdata('reference_phonenumbers', url_brian)
df_aws_phonenumbers <- fn_get_aws_refdata('reference_phonenumbers', url_aws)

glimpse(df_phonenumbers)
glimpse(df_aws_phonenumbers)

# check everything in aws is still in the new dataset
df_missing <- df_aws_phonenumbers %>%
  anti_join(df_phonenumbers, by=c('ref.phone' = 'ref.phone',
                                  'ref.phone.start' = 'ref.phone.start',
                                  'ref.phone.service' = 'ref.phone.service'))

# check new entries
df_new <- df_phonenumbers %>% 
  anti_join(df_aws_phonenumbers, by=c('ref.phone' = 'ref.phone',
                                  'ref.phone.start' = 'ref.phone.start',
                                  'ref.phone.service' = 'ref.phone.service'))

# if it's OK to process, update the new data into the existing aws sheet
if (nrow(df_new) > 0) {
  # save a dated backup of the existing data
  write_parquet(df_aws_phonenumbers, paste0(path_backup, 'reference_phonenumbers_',Sys.Date(),'.parquet'))
  # write new data to the google sheet
  write_sheet(df_phonenumbers, url_aws, 'reference_phonenumbers')
  # save a new parquet file
  write_parquet(df_phonenumbers, paste0( path_parquet,'reference_phonenumbers.parquet'))
}

# get Brians sheets - queues ---------------------------------------------------
# check for referential integrity
# check nothing currently in aws in replaced or missed
df_queues <- fn_get_source_refdata('reference_queues', url_brian)
df_aws_queues <- fn_get_aws_refdata('reference_queues', url_aws)

glimpse(df_queues)
glimpse(df_aws_queues)

# check everything in aws is still in the new dataset
df_missing <- df_aws_queues %>%
  anti_join(df_queues, by=c('ref.queue' = 'ref.queue',
                                  'ref.queue.start' = 'ref.queue.start',
                                  'ref.queue.service' = 'ref.queue.service'))

# check new entries
df_new <- df_queues %>% 
  anti_join(df_aws_queues, by=c('ref.queue' = 'ref.queue',
                                      'ref.queue.start' = 'ref.queue.start',
                                      'ref.queue.service' = 'ref.queue.service'))

# if it's OK to process, update the new data into the existing aws sheet
if (nrow(df_new) > 0) {
  # save a dated backup of the existing data
  write_parquet(df_aws_queues, paste0(path_backup, 'reference_queues_',Sys.Date(),'.parquet'))
  # write new data to the google sheet
  write_sheet(df_queues, url_aws, 'reference_queues')
  # save a new parquet file
  write_parquet(df_queues, paste0( path_parquet,'reference_queues.parquet'))
}

