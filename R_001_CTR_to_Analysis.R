library(tidyverse)
library(arrow)
library(here)

source('R_INCLUDE_Functions.R')

# Create a calls-based dataset from raw CTRs

# -------------------------------------------------------------------------
# Stage 1 - create a call based dataset from multiple CTRS

# get the source raw data
test <- fn_CTR_data_get(month = 'march', set_info = TRUE)

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

# -------------------------------------------------------------------------
# Stage 2 - create a calls based dataset suitable for analysis in Tableau

# create transformed variables
df_transform <- fn_CALL_to_ANALYSIS(df_calls)

# join reference data
df_refs <- fn_CALL_ReferenceData(df_transform)

# create service filters
df_defs <- fn_CALL_dataset_defs(df_refs)

# reorder variables into human-logical format
df_analysis <- fn_reorder(df_defs)


# -------------------------------------------------------------------------
