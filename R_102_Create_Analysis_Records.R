
library(tidyverse)
library(lubridate)
library(arrow)
library(here)

source('R_INCLUDE_Functions.R')

# -------------------------------------------------------------------------
# Get Call and CTR data and apply the inclusion list

month_to_process <- 'april'

df_ctrs <- read_parquet(here('data',paste0('data-',month_to_process,'-ctrs')))
df_calls <- read_parquet(here('data',paste0('data-',month_to_process,'-calls')))

# -------------------------------------------------------------------------
# Add the transforms
# queue.duration

# typically takes 2 mins
t1 <- Sys.time()
df_analysis <- fn_CALL_to_ANALYSIS(df_calls)
t2 <- Sys.time()
print(paste0(' ... Time taken ', round(difftime(t2, t1, units = "mins"), digits = 2), ' minutes'))

# -------------------------------------------------------------------------
# join the reference data and create dataset filters
# reorder the variables to make them meaningful to humans

t1 <- Sys.time()
df_refs <- fn_CALL_ReferenceData(df_analysis)
df_defs <- fn_CALL_dataset_defs(df_refs)
df_order <- fn_reorder(df_defs)
t2 <- Sys.time()
print(paste0(' ... Time taken ', round(difftime(t2, t1, units = "mins"), digits = 2), ' minutes'))

# -------------------------------------------------------------------------
# Save for future use
# write_parquet(df_order, here('data',paste0('CALLS-ANALYSIS-',month_to_process,'.parquet')))
# write_csv(df_order, here('data',paste0('CALLS-ANALYSIS-',month_to_process,'.csv')))
# -------------------------------------------------------------------------

if (FALSE) {
  # april dataset
  examples <- c('1add908f-4f0b-4e22-a839-375df5c1bda0',
                '00068443-beeb-4c96-8194-f50f421001bc',
                '388b877a-cbb6-43bd-bfb1-5df2848eefbb',
                '0e8d9673-9631-4ea7-be4e-effbe480ab6b',
                '000100da-57b2-4793-8872-404f5caaca42'
                )
  
  df_examples <- df_order %>% 
    filter(pipe.ctr_setid %in% examples)
  
  df_examples %>% 
    t() %>% 
    View()
}

df_order %>% 
  slice(1:5) %>% 
  t() %>% 
  View()

# -------------------------------------------------------------------------

df_order %>% 
#  filter(!is.na(ref.al.description)) %>% 
#  count(ref.phone.service) %>% 
  filter(ref.phone.service == 'Advicelink') %>% 
  count(ref.alink.service) %>% 
  identity()

# -------------------------------------------------------------------------

if (FALSE) {
  df_test <- fn_CTR_data_get(reduce = FALSE)
  df_test_examples <- df_test %>%     # make a ctr_setid, and a variable to store the junk condition
    mutate(ctr_setid = case_when(is.na(initialcontactid) ~ contactid, T ~ initialcontactid), .before = 1) %>%
    filter(ctr_setid %in% examples | contactid %in% c('6dea1b0f-4a40-4193-9b12-9356304410b0','8347adc2-8dbe-4ad8-ad16-211a280eff76')) %>% 
    # make dummy variables
    mutate(ctr_orig = 0, .after = 'ctr_setid') %>% 
    mutate(ctr_junk = 0, .after = 'ctr_orig') %>% 
    mutate(junk = 'NO', .after = 'ctr_junk') %>%
    mutate(junk = case_when(initiationmethod == 'DISCONNECT' ~ 'DISCONNECT CTR', T ~ junk)) %>% 
    mutate(leg_count = 0, .after = 'junk') %>% 
    mutate(leg_id = 0, .after = 'leg_count') %>% 
    mutate(dup_check = paste0(initiationmethod,':',initiationtimestamp,':',disconnectreason,':',disconnecttimestamp), .after = 'ctr_junk') %>% 
    # collect some original audit stats
    group_by(ctr_setid) %>% 
    arrange(ctr_setid, initiationtimestamp) %>% 
    # original numbers of ctrs
    mutate(ctr_orig = n()) %>% 
    # number of ctrs junked
    mutate(ctr_junk = sum(!junk == 'NO')) %>% 
    mutate(leg_count = n()) %>% 
    mutate(leg_id = row_number()) %>% 
    ungroup() %>% 
    identity()
}

