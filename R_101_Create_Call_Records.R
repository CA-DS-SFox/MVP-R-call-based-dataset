
library(tidyverse)
library(arrow)
library(here)

source('R_INCLUDE_Functions.R')

# -------------------------------------------------------------------------
# Get CTR data and apply the inclusion list
df_ctr_0 <- fn_CTR_data_get()

# -------------------------------------------------------------------------
# First cleaning pass, split into Calls with single CTR, multi CTR, identify junk records
t1 <- Sys.time()
print(paste0(' ... Cleaning pass 1, start at ', t1))
df_ctr_1 <- fn_CTR_data_clean_pass1(df_ctr_0)
df_ctr_single <- df_ctr_1$ctr_single
df_ctr_multi <- df_ctr_1$ctr_multi
df_ctr_junk <- df_ctr_1$ctr_junk
t2 <- Sys.time()
print(paste0(' ... Cleaning pass 1, took ', t2 - t1))

# -------------------------------------------------------------------------

# Sense checks
df_ctr_junk %>% 
  count(leg_id, junk, call_type, initiationmethod, disconnectreason) %>%
  pivot_wider(names_from = leg_id, values_from = n, values_fill = 0)

df_ctr_single %>% 
  count(leg_id, ctr_type, junk, call_type, initiationmethod, disconnectreason)

df_ctr_multi %>% 
  count(leg_id, ctr_type, junk, call_type, initiationmethod, disconnectreason) %>% 
  pivot_wider(names_from = leg_id, values_from = n, values_fill = 0)

df_ctr_multi %>% 
  count(junk, call_type, ctr_type, initiationmethod, disconnectreason, leg_count) %>% 
  pivot_wider(names_from = leg_count, values_from = n, values_fill = 0)

# -------------------------------------------------------------------------
# Second cleaning pass, deal with valid transfer records

# deal with transfer records - make them a new call set
print(paste0(' ... Cleaning pass 2, start at ', t1))
df_ctr_2 <- fn_CTR_data_clean_pass2(df_ctr_single, df_ctr_multi)
df_ctr_single <- df_ctr_2$ctr_single
df_ctr_multi <- df_ctr_2$ctr_multi
t2 <- Sys.time()
print(paste0(' ... Cleaning pass 2, took ', t2 - t1))

# -------------------------------------------------------------------------
# Third cleaning pass, collapse multi records into a call

df_multi_calls <- fn_CTR_data_clean_pass3(df_ctr_multi)

df_ctrs <- df_ctr_single %>% bind_rows(df_ctr_multi)
df_calls <- df_ctr_single %>% bind_rows(df_multi_calls)

write_parquet(df_ctrs, here('data','data-march-ctrs'))
write_parquet(df_calls, here('data','data-march-calls'))

# -------------------------------------------------------------------------

df_calls %>% count(ctr_type, leg_count)
