library(tidyverse)
library(arrow)
library(here)

source('R_INCLUDE_Functions.R')

# -------------------------------------------------------------------------

df_max <- read_parquet(here('data','CALLS-APRIL-MAX.parquet'), col_types = c(.default = 'c'))
df_suz <- read_parquet(here('data','CALLS-APRIL-SUZANNE.parquet'), col_types = c(.default = 'c'))
df_fin <- read_parquet(here('data','CALLS-ANALYSIS-april_2023-05-22.parquet'), col_types = c(.default = 'c'))
df_ctr <- fn_CTR_data_get(month = 'April', set_info = TRUE)


# -------------------------------------------------------------------------
# test 1. distribution of calls by date
dates_max <- df_max %>% 
  count(pipe.when_date) %>% 
  arrange(pipe.when_date)

dates_suz <- df_suz %>% 
  count(pipe.when_date) %>% 
  arrange(pipe.when_date)

# -------------------------------------------------------------------------

# test #1. record totals
tot_max <- df_max %>% tally() %>% as.integer()
tot_suz <- df_suz %>% tally() %>% as.integer()
check_1 <- abs(tot_suz - tot_max)
print(paste0('1. Record count : Max ',tot_max,' Suzanne ',tot_suz,' difference ',check_1))

df_setid <- df_ctr %>% distinct(pipe.ctr_setid) %>% select(ctr = pipe.ctr_setid) %>% 
  full_join(df_max %>% select(max = pipe.ctr_setid), by = c('ctr' = 'max'), keep = TRUE) %>% 
  full_join(df_suz %>% select(suz = pipe.ctr_setid), by = c('ctr' = 'suz'), keep = TRUE) %>% 
  mutate(max = coalesce(max, '-')) %>% 
  mutate(suz = coalesce(suz, '-')) %>% 
  mutate(ctr_no_max = ctr != max) %>% 
  mutate(ctr_no_suz = ctr != suz) %>% 
  mutate(max_suz = max == suz) 

df_setid %>% 
  count(ctr_no_max, ctr_no_suz)

df_setid %>% 
  filter(is.na(ctr_no_max))
df_setid %>% select(max) %>% distinct(max) %>% tally()

