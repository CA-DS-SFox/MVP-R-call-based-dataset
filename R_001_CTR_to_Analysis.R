library(tidyverse)
library(arrow)
library(here)

source('R_INCLUDE_Functions.R')
source('R_INCLUDE_Functions_Reference.R')

# Create a calls-based dataset from raw CTRs
# -------------------------------------------------------------------------
# Stage 1 - create a call based dataset from multiple CTRS

t1 <- Sys.time()
# get the source raw data
month_to_process <- 'April'
test <- fn_CTR_data_get(month = month_to_process, set_info = TRUE)

# check how many transfer records are for OUTBOUND calls
# and how many calls have DISCONNECT as CTR #1
if (FALSE) {
  test %>%
    arrange(pipe.ctr_setid, initiationtimestamp) %>%
    group_by(pipe.ctr_setid) %>%
    mutate(call_type = case_when(row_number() == 1 ~ initiationmethod)) %>% 
    tidyr::fill(call_type) %>% 
    ungroup() %>%
    select(pipe.ctr_setid, call_type, initiationmethod) %>% 
    count(call_type, initiationmethod) %>% 
    pivot_wider(names_from = call_type, values_from = n, values_fill = 0) %>% 
    identity()
}
  
# split into single and multiple CTR callsets
test_list <- fn_CTR_data_split(test)
df_single0 <- test_list$single
df_multiple0 <- test_list$multiple

# junk records that are not clean
test_list <- fn_CTR_data_junk(df_single0, df_multiple0)
df_single1 <- test_list$single
df_multiple1 <- test_list$multiple

# make inbound 'QuickConnect' transfers their own calls
test_list <- fn_CTR_data_transfers(df_single1, df_multiple1)
df_single2 <- test_list$single
df_multiple2 <- test_list$multiple

# collapse multi-CTR recordsets into a call
df_calls <- fn_CTR_data_collapse(df_single2, df_multiple2)
t2 <- Sys.time()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Stage 2 - create a calls based dataset suitable for analysis in Tableau

# create transformed variables
df_transform <- fn_CALL_to_ANALYSIS(df_calls)
# write_parquet(df_transform, here('data','CALLS-SUZANNE-APRIL.parquet'))

print(paste0('TRANSFORMED CALL DATA TOOK ',round(t2 - t1, 1), ' minutes ',nrow(df_calls),' calls in total'))
# -------------------------------------------------------------------------
# join reference data
df_refs <- fn_CALL_ReferenceData(df_transform, google = TRUE)

# create service filters
df_defs <- fn_CALL_dataset_defs(df_refs)

# reorder variables into human-logical format
df_analysis <- fn_reorder(df_defs)

# outfile <- paste0('CALLS-ANALYSIS-',month_to_process,'_',Sys.Date(),'.csv')
# write_csv(df_analysis, here('data',outfile))
# now copy this to CALL-ANALYSIS-april.csv and replace into Tableau as a data source

print('Finished.')

# -------------------------------------------------------------------------
