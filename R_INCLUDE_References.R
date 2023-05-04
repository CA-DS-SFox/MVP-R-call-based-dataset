
# reference tables
ref_dir <- 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Reference Tables/'

fn_REF_get <- function(what, show = FALSE) {
  
  what <- tolower(what)
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'okta') {
    # advisers from okta 
    # join key is agent.username = okta_id
    ref_file_okta <- 'reporting_oktaadvisers.csv'
    df_ref_okta <- read_csv(paste0(ref_dir, ref_file_okta), col_types = cols(.default='c')) %>% 
      rename(ref.okta.member = member_aws,
             ref.okta.advisername = adviserreportingname,
             ref.okta.office = office)
    if (df_ref_okta %>% count(okta_id) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... Duplicate OKTA IDS !')
    } else {
      print(' ... Okta data passed')
      if (show) glimpse(df_ref_okta)
    }
    return(df_ref_okta)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'mbr') {
    # MBR info from LSS
    ref_file_mbr <- 'reference_mbr.parquet'
    df_ref_mbr <- read_parquet(paste0(ref_dir, ref_file_mbr), col_types = cols(.default='c')) %>% 
      filter(!is.na(member_id)) %>% 
      select(member_aws, ref.lss.fullname = member_name, ref.lss.shortname = member_short)
    
    if (df_ref_mbr %>% count(member_aws) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... MBR Duplicates !')
      df_ref_mbr %>% add_count(member_aws) %>% filter(n > 1)
    } else {
      print(' ... MBR data passed')
      if (show) glimpse(df_ref_mbr)
    }
    return(df_ref_mbr)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'phonenos') {
    # phone nos to service mapping
    ref_file_phonenos <- 'reference_phonenumbers.parquet'
    df_ref_phonenos <- read_parquet(paste0(ref_dir, ref_file_phonenos), col_types = cols(.default='c')) %>%
      distinct(ref.phone.service, ref.phone)
    if (df_ref_phonenos %>% count(ref.phone) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... MBR Duplicates !')
      df_ref_phonenos %>% add_count(ref.phone) %>% filter(n > 1)
    } else {
      print(' ... Phone Service data passed')
      if (show) glimpse(df_ref_phonenos)
    }
    return(df_ref_phonenos)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'queue') {
    # queue name to service mapping
    ref_file_queue <- 'reference_queues.parquet'
    df_ref_queue <- read_parquet(paste0(ref_dir, ref_file_queue), col_types = cols(.default='c'))
    if (df_ref_queue %>% count(ref.queue) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... Queue Duplicates !')
      df_ref_mbr %>% add_count(ref.queue) %>% filter(n > 1)
    } else {
      print(' ... Queue data passed')
      if (show) glimpse(df_ref_queue)
    }
    return(df_ref_queue)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'algroup') {
    # MBR to adviceline groups
    ref_file_algroups <- 'reference_adviceline_groups.parquet'
    df_ref_algroups <- read_parquet(paste0(ref_dir, ref_file_algroups), col_types = cols(.default='c'))
    if (df_ref_algroups %>% count(member_aws) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... AL groups Duplicates !')
      df_ref_algroups %>% add_count(member_aws) %>% filter(n > 1)
    } else {
      print(' ... AL groups data passed')
      if (show) glimpse(df_ref_algroups)
    }
    return(df_ref_algroups)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'single') {
    # single queues
    ref_file_single <- 'reference_single_queue.parquet'
    df_ref_single <- read_parquet(paste0(ref_dir, ref_file_single), col_types = cols(.default='c'))
    if (df_ref_single %>% count(single_queue) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... Single groups Duplicates !')
      df_ref_single %>% add_count(single_queue) %>% filter(n > 1)
    } else {
      print(' ... Single queue data passed')
      if (show) glimpse(df_ref_single)
    }
    return(df_ref_single)
  }
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if (what == 'alink') {
    # # advicelink services
    ref_file_advicelink <- 'reference_advicelink_services.parquet'
    df_ref_alink <- read_parquet(paste0(ref_dir, ref_file_advicelink), col_types = cols(.default='c'))
    if (df_ref_alink %>% count(ref.alink.phone) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... Advicelink services Duplicates !')
      df_ref_alink %>% add_count(ref.alink.phone) %>% filter(n > 1)
    } else {
      print(' ... Advicelink services passed')
      if (show) glimpse(df_ref_alink)
    }
    return(df_ref_alink)
  }
}
