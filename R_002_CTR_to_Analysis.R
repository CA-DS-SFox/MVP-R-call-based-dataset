library(tidyverse)
library(arrow)
library(here)

source('R_INCLUDE_Functions.R')
source('R_INCLUDE_Functions_Reference.R')

# -------------------------------------------------------------------------
# Stage 1 - create a call based dataset from raw CTRs

t1 <- Sys.time()

# 1. get the source raw data
month_to_process <- 'March'
df_raw <- fn_CTR_data_get(month = month_to_process, set_info = TRUE)

# 2. create some computed variables, some will be empty and 
#    we'll populate them later
df_basic <- fn_CTR_add_variables(df_raw)

# 3. remove any calls which aren't inbound or outbound
#    determine this from the initiationmethod on the first CTR
df_input_good <- fn_CTR_remove_badtypes(df_basic)

# 4. remove duplicate records, leaving the first CTR
#    Phones team are not sure why these records occur 
df_input_dedup <- fn_CTR_remove_deduplicates(df_input_good)

# 5. split into inbound and outbound datasets
#    the cleaning rules are different for each type
list_df <- fn_CTR_split(df_input_dedup)
df_calls_out <- fn_CALLS_outbound(list_df$df_outbound)
df_calls_in <- fn_CALLS_inbound(list_df$df_inbound)

# 6. combine to give a call based dataset
df_calls <- df_calls_in %>% bind_rows(df_calls_out)

# write_parquet(df_calls, here('data',paste0('CALLS-CLEAN-',month_to_process,'.parquet')))

t2 <- Sys.time()
print(paste0('CREATE CALL BASED DATASET takes ',round(t2 - t1, 1)))

# -------------------------------------------------------------------------
# Stage 2 - Create a dataset with the analysis transforms

# 1. create transformed variables
df_transform <- fn_CALL_to_ANALYSIS(df_calls)

# write_parquet(df_calls, here('data',paste0('CALLS-ANALYSIS-',month_to_process,'.parquet')))

# -------------------------------------------------------------------------
# Stage 3 - Add reference data and service filters
#           this can be done in ATHENA for now

# 1. join reference data
df_refs <- fn_CALL_ReferenceData(df_transform, google = TRUE)

# 2. create service filters
df_defs <- fn_CALL_dataset_defs(df_refs)

# write_parquet(df_defs, here('data','CALLS-april-V2.parquet'))

# 3. reorder variables into human-logical format
df_analysis <- fn_reorder(df_defs)


                   