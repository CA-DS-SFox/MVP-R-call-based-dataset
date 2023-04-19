
# reference tables
ref_dir <- 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Reference Tables/'

# -------------------------------------------------------------------------
# advisers from okta 
ref_file_okta <- 'reporting_oktaadvisers.csv'
df_ref_okta <- read_csv(paste0(ref_dir, ref_file_okta), col_types = cols(.default='c'))
if (df_ref_okta %>% count(okta_id) %>% filter(n > 1) %>% tally() > 0) {
  print(' ... Duplicate OKTA IDS !')
} else {
  print(' ... Okta data passed')
}

# MBR info from LSS
ref_file_mbr <- 'reference_mbr.parquet'
df_ref_mbr <- read_parquet(paste0(ref_dir, ref_file_mbr), col_types = cols(.default='c')) %>% 
  filter(!is.na(member_id))
if (df_ref_mbr %>% count(member_aws) %>% filter(n > 1) %>% tally() > 0) {
  print(' ... MBR Duplicates !')
  df_ref_mbr %>% add_count(member_aws) %>% filter(n > 1)
} else {
  print(' ... MBR data passed')
}

# phone nos to service mapping
ref_file_phonenos <- 'reference_phonenumbers.parquet'
df_ref_phonenos <- read_parquet(paste0(ref_dir, ref_file_phonenos), col_types = cols(.default='c')) %>% 
  distinct(ref.phone.service, ref.phone)
if (df_ref_phonenos %>% count(ref.phone) %>% filter(n > 1) %>% tally() > 0) {
  print(' ... MBR Duplicates !')
  df_ref_phonenos %>% add_count(ref.phone) %>% filter(n > 1)
} else {
  print(' ... Phone Service data passed')
}

# queue name to service mapping
ref_file_queue <- 'reference_queues.parquet'
df_ref_queue <- read_parquet(paste0(ref_dir, ref_file_queue), col_types = cols(.default='c'))
if (df_ref_queue %>% count(ref.queue) %>% filter(n > 1) %>% tally() > 0) {
  print(' ... Queue Duplicates !')
  df_ref_mbr %>% add_count(ref.queue) %>% filter(n > 1)
} else {
  print(' ... Queue data passed')
}

# MBR to adviceline groups
ref_file_algroups <- 'reference_adviceline_groups.parquet'
df_ref_algroups <- read_parquet(paste0(ref_dir, ref_file_algroups), col_types = cols(.default='c'))
if (df_ref_algroups %>% count(member_aws) %>% filter(n > 1) %>% tally() > 0) {
  print(' ... AL groups Duplicates !')
  df_ref_algroups %>% add_count(member_aws) %>% filter(n > 1)
} else {
  print(' ... AL groups data passed')
}

# single queues
ref_file_single <- 'reference_single_queue.parquet'
df_ref_single <- read_parquet(paste0(ref_dir, ref_file_single), col_types = cols(.default='c'))

