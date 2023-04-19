

# read the .parquet file exported from AWS
# select the variables that will be in Max's inclusion list
fn_CTR_data_get <- function() {
  
  # These are the variables that will be in Max's include list
  cols_include_list <- c(
    'channel',
    
    'contactid',
    'initialcontactid',
    'nextcontactid',
    'previouscontactid',
    
    'initiationmethod',
    'disconnectreason',
    
    'systemendpoint.address',
    'customerendpoint.address',
    'transferredtoendpoint.address',
    
    'queue.name',
    'agent.username',
    'agent.routingprofile.name',
    'agent.hierarchygroups.level1.groupname',
    'agent.hierarchygroups.level2.groupname',
    'agent.hierarchygroups.level3.groupname',
    'agent.hierarchygroups.level4.groupname',
    'agent.hierarchygroups.level5.groupname',
    
    'initiationtimestamp',
    'connectedtosystemtimestamp',
    'agent.connectedtoagenttimestamp',
    'queue.enqueuetimestamp',
    'queue.dequeuetimestamp',
    'transfercompletedtimestamp',
    'agent.aftercontactworkstarttimestamp',
    'agent.aftercontactworkendtimestamp',
    'disconnecttimestamp',
    'lastupdatetimestamp',
    
    'queue.duration',
    'agent.customerholdduration',
    'agent.longestholdduration',
    'agent.agentinteractionduration',
    'agent.aftercontactworkduration',
    
    'agent.numberofholds',
    'agentconnectionattempts',
    
    # Brian Budd - Standard Attributes
    'attributes.servicename',
    'attributes.finalservice',
    'attributes.keypress',
    'attributes.flowselection',
    'attributes.outcome',
    'attributes.formember',
    
    # Brian Budd - CSAT Attributes
    'attributes.appname',
    'attributes.numberofquestion',
    'attributes.question1',
    'attributes.question2',
    'attributes.question3',
    'attributes.question4',
    'attributes.question5',
    'attributes.question1response',
    'attributes.question2response',
    'attributes.question3response',
    'attributes.question4response',
    'attributes.question5response',
    
    # Brian Budd - National Queue Attributes
    'attributes.nationalreason')
  
  # get the March contact_trace_record CTR data 
  df <- read_parquet(here('data','CTR-march.parquet'), col_types = cols(.default='c'))
  
  # keep the columns from the include list
  df_useful <- df %>% select(all_of(cols_include_list))
  
  return(df_useful)
}

# remove spurious DISCONNECT and TRANSFER records to leave a clean CTR based dataset
# NOTE : All DISCONNECT records are artefacts, same TRANSFER records for OUTBOUND
#        but TRANSFER records for INBOUND seem valid
fn_CTR_data_clean_pass1 <- function(df_ctr) {
  
  # -------------------------------------------------------------------------
  # first reduce the recordset by throwing away redundant records
  df_ctr_basic <- df_ctr %>% 
    # make a ctr_setid, and a variable to store the junk condition
    mutate(ctr_setid = case_when(is.na(initialcontactid) ~ contactid, T ~ initialcontactid), .before = 1) %>%
    mutate(junk = 'NO', .after = 1) %>%
    mutate(junk = case_when(initiationmethod == 'DISCONNECT' ~ 'DISCONNECT CTR', T ~ junk)) %>% 
    mutate(leg_count = 0, .after = 3) %>% 
    mutate(leg_id = 0, .after = 4) %>% 
    mutate(dup_check = paste0(initiationmethod,':',initiationtimestamp,':',disconnectreason,':',disconnecttimestamp), .after = 2) %>% 
    identity()
  
  df_ctr_junk <- df_ctr_basic %>% filter(!junk == 'NO')
  df_ctr <- df_ctr_basic %>% filter(junk == 'NO')
  # -------------------------------------------------------------------------
  df_ctr_useful <- df_ctr %>% 
    # find the number of CTR legs in the call
    group_by(ctr_setid) %>%
    mutate(leg_count = n()) %>%
    mutate(leg_id = row_number()) %>%
    
    # find the call type
    mutate(call_type = case_when(any(initiationmethod == 'INBOUND') ~ 'INBOUND', 
                                 any(initiationmethod == 'OUTBOUND') ~ 'OUTBOUND',
                                 T ~ 'OTHER'), .after = 1) %>% 
    
    ungroup()
  
  # -------------------------------------------------------------------------
  # then split into single leg calls and multi leg calls because
  # single leg calls are fine as they are
  df_ctr_single <- df_ctr_useful %>% filter(leg_count == 1)
  df_ctr_multiple <- df_ctr_useful %>% filter(leg_count > 1)
  
  # -------------------------------------------------------------------------
  # Now evaluate the multi CTR dataset for records to junk
  df_ctr_eval <- df_ctr_multiple %>% 
    
    group_by(ctr_setid) %>% 
    arrange(ctr_setid, initiationtimestamp) %>% 
    
    # check for randomly created duplicates
    mutate(junk = case_when(dup_check == lag(dup_check) ~ 'DUPLICATE OF PREVIOUS CTR', 
                            T ~ junk)) %>% 
    
    # for outbound calls, throw away any legs greater than 1 with an inititationmethod of 'OUTBOUND'
    # these seem to be duplicates on every occurance that I have checked
    mutate(junk = case_when(row_number() > 1 & initiationmethod == 'OUTBOUND' ~ 'OUTBOUND NOT POSITION 1 IN CTR SET', 
                            T ~ junk)) %>% 
    
    # for inbound calls, throw away any records with an initiationmethod of 'TRANSFER'
    # and where the queue.name is blank AND the agent.username is blank, these seem to be system artefacts
    mutate(junk = case_when(call_type == 'INBOUND' & 
                              initiationmethod == 'TRANSFER' & 
                              is.na(agent.username) & 
                              is.na(queue.name) ~ 'INBOUND BLANK TRANSFERS', 
                            T ~ junk)) %>% 
    
    # for outbound calls, throw away any records with an initiationmethod of 'TRANSFER'
    # and where the agent.username is blank, these seem to be system artefacts
    mutate(junk = case_when(call_type == 'OUTBOUND' & 
                              initiationmethod == 'TRANSFER' & is.na(agent.username) ~ 'OUTBOUND BLANK TRANSFERS', 
                            T ~ junk)) %>% 
    
    # for outbound calls, throw away any records with an initiationmethod of 'TRANSFER'
    # and disconnectreason of 'CONTACT_FLOW_DISCONNECT' or 'CUSTOMER_DISCONNECT', these seem to be system artefacts
    mutate(junk = case_when(call_type == 'OUTBOUND' &
                              initiationmethod == 'TRANSFER' &
                              disconnectreason %in% c('CONTACT_FLOW_DISCONNECT','CUSTOMER_DISCONNECT') ~ 'INBOUND TRANSFER TO CONTACT_FLOW_DISCONNECT',
                            T ~ junk)) %>%
    ungroup() 
  
  # -------------------------------------------------------------------------
  # add new junk records from the multiple CTR Call sets
  df_ctr_junk <- df_ctr_junk %>% 
    bind_rows(df_ctr_eval %>% filter(!junk == 'NO'))
  
  # these are ok, but now some will be single CTR sets
  df_ctr_ok <- df_ctr_eval %>% filter(junk == 'NO')
  
  df_ctr_clean_single <- df_ctr_ok %>% 
    group_by(ctr_setid) %>% 
    filter(n() == 1) %>% 
    ungroup()
  
  df_ctr_clean_multiple <- df_ctr_ok %>% 
    group_by(ctr_setid) %>% 
    filter(n() > 1) %>% 
    ungroup()
  
  # reevaluate the datasets
  df_ctr_single <- df_ctr_single %>% 
    bind_rows(df_ctr_clean_single) %>% 
    group_by(ctr_setid) %>%
    mutate(leg_count = n()) %>%
    mutate(leg_id = row_number()) %>%
    ungroup()
  
  df_ctr_multiple <- df_ctr_clean_multiple %>% 
  group_by(ctr_setid) %>%
    mutate(leg_count = n()) %>%
    mutate(leg_id = row_number()) %>%
    ungroup()
  
  # -------------------------------------------------------------------------
  df_ctr_junk <- df_ctr_junk %>% 
    group_by(ctr_setid) %>% 
    arrange(ctr_setid, initiationtimestamp) %>% 
    mutate(ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()
  
  df_ctr_single <- df_ctr_single %>% 
    group_by(ctr_setid) %>% 
    arrange(ctr_setid, initiationtimestamp) %>% 
    mutate(ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()
  
  df_ctr_multiple <- df_ctr_multiple %>% 
    group_by(ctr_setid) %>% 
    arrange(ctr_setid, initiationtimestamp) %>% 
    mutate(ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()

  # -------------------------------------------------------------------------

  df_list <- list(ctr_single = df_ctr_single, 
                  ctr_multi = df_ctr_multiple, 
                  ctr_junk = df_ctr_junk)
  
  return(df_list)
  
}

# Only run this on multi-CTR datasets
fn_CTR_data_clean_pass2 <- function(df_ctr_single, df_ctr_multiple) {
  
  # remove TRANSFER records from the Call CTR set
  # the original Call set can be related from the initial or previous contactid
  df_ctr <- df_ctr_multiple %>% 
    mutate(ctr_setid = case_when(initiationmethod == 'TRANSFER' ~ contactid, T ~ ctr_setid)) %>% 
    group_by(ctr_setid) %>% 
    arrange(ctr_setid, initiationtimestamp) %>% 
    mutate(ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()
  
  # add single records to the single dataset
  df_ctr_2_single <- df_ctr %>% filter(ctr_type == 'only')
  df_ctr_single <- df_ctr_single %>% bind_rows(df_ctr_2_single)
  
  # keep multiple ctr call sets
  df_ctr_2_multi <- df_ctr %>% filter(ctr_type != 'only')
  
  # return
  df_list <- list(ctr_single = df_ctr_single,
                  ctr_multi = df_ctr_2_multi)
  
  return(df_list)
}

# Input MUST be a dataframe of multi-CTR calls
fn_CTR_data_clean_pass3 <- function(df_ctr_multiple) {

    # data from first record
  df_first <- df_ctr_multiple %>% 
    filter(ctr_type == 'first') %>% 
    select(ctr_setid:customerendpoint.address) %>% 
    mutate(ctr_type = 'multiple')
  
  df_middle <- df_ctr_multiple %>% 
    select(ctr_setid, queue.name, queue.duration) %>% 
    group_by(ctr_setid) %>% 
    summarise(queue.hops = paste(queue.name, collapse = ','),
              queue.total = sum(as.integer(queue.duration))) %>% 
    ungroup()
  
  df_final <- df_ctr_multiple %>% 
    filter(ctr_type == 'final') %>% 
    select(ctr_setid, transferredtoendpoint.address:last_col())
  
  df_call <- df_first %>% 
    left_join(df_middle, by = 'ctr_setid') %>% 
    left_join(df_final, by = 'ctr_setid')
  
  return(df_call)
}

# check that cleaning assumptions are still valid
# CHECKS : OUTBOUND calls only have 1 leg, report otherwise
# CHECKS : INBOUND transfer records don't look correct, timestamps overlap
fn_CTR_data_check <- function(df_ctr_clean) {
  df_checks <- df_ctr_clean
  return(df_checks)  
}

# data transformations to give analysable dataset
# CREATES : dataframe called df_calls which is a dataframe where ...
#           ROWS : represent call records
#           COLS : transformed variables suitable for analysis
#           INPUTS : df_calls_orig created from ctr records

fn_CALL_to_ANALYSIS <- function(df_calls_orig) {
  
  df_calls_transform <- df_calls_orig %>%
    # set important data types - timestamps should be posix
    # for inbound timestamps initiation == connected
    # for outbound etc, init is when the operator started some action, and connected is when the call was answered
    # sometimes connected is blank
    mutate(initiationtimestamp = as.POSIXct(initiationtimestamp)) %>%
    mutate(connectedtosystemtimestamp = as.POSIXct(connectedtosystemtimestamp)) %>%
    mutate(queue.enqueuetimestamp = as.POSIXct(queue.enqueuetimestamp)) %>%
    mutate(queue.dequeuetimestamp = as.POSIXct(queue.dequeuetimestamp)) %>%
    mutate(agent.connectedtoagenttimestamp = as.POSIXct(agent.connectedtoagenttimestamp)) %>%
    mutate(agent.aftercontactworkstarttimestamp = as.POSIXct(agent.aftercontactworkstarttimestamp)) %>%
    mutate(agent.aftercontactworkendtimestamp = as.POSIXct(agent.aftercontactworkendtimestamp)) %>%
    mutate(disconnecttimestamp = as.POSIXct(disconnecttimestamp)) %>%
    mutate(transfercompletedtimestamp = as.POSIXct(transfercompletedtimestamp)) %>%
    mutate(lastupdatetimestamp = as.POSIXct(lastupdatetimestamp)) %>%
    
    # we need more granular date and time fields for some timestamps
    # renamed to 'when' family of variables from 18/01/23
    mutate(date_call = as.Date(initiationtimestamp)) %>%
    # i know this is a straightforward copy, which may seem pointless from a adat perspective, but
    # from an analysis point of view it makes sense to have a 'set' of commonly named variables
    mutate(when_date = date_call) %>%
    mutate(when_week = format(date_call,'%y-%w')) %>%
    mutate(when_day = format(when_date, '%a')) %>%
    mutate(when_day = factor(when_day, levels = c('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'))) %>%
    mutate(when_month = format(when_date, '%y-%m')) %>%
    mutate(when_time = format(date_call, '%h-%m-%s')) %>%
    mutate(when_hour = format(initiationtimestamp, '%h')) %>%
    mutate(when_minute = format(initiationtimestamp, '%m')) %>%
    mutate(when_second = format(initiationtimestamp, '%s')) %>%
    
    # times without date
    mutate(tm_init = format(initiationtimestamp, '%h:%m:%s')) %>%
    mutate(tm_conn = format(connectedtosystemtimestamp, format = '%h:%m:%s')) %>%
    mutate(tm_quenq = format(queue.enqueuetimestamp, '%h:%m:%s')) %>%
    mutate(tm_qudeq = format(queue.dequeuetimestamp, '%h:%m:%s')) %>%
    mutate(tm_agcon = format(agent.connectedtoagenttimestamp, '%h:%m:%s')) %>%
    mutate(tm_tranf = format(transfercompletedtimestamp, '%h:%m:%s')) %>%
    mutate(tm_agwrs = format(agent.aftercontactworkstarttimestamp, '%h:%m:%s')) %>%
    mutate(tm_agwre = format(agent.aftercontactworkendtimestamp, '%h:%m:%s')) %>%
    mutate(tm_disc = format(disconnecttimestamp, '%h:%m:%s')) %>%
    mutate(tm_updat = format(lastupdatetimestamp, format = '%h:%m:%s')) %>%
    
    # duration in each state
    mutate(dur_init_conn = as.integer(difftime(connectedtosystemtimestamp, initiationtimestamp, unit = 'secs'))) %>%
    mutate(dur_init_que = as.integer(difftime(queue.enqueuetimestamp, initiationtimestamp, unit = 'secs'))) %>%
    mutate(dur_enq_deq = as.integer(difftime(queue.dequeuetimestamp, queue.enqueuetimestamp, unit = 'secs'))) %>%
    mutate(dur_deq_agnt = as.integer(difftime(agent.connectedtoagenttimestamp, queue.dequeuetimestamp, unit = 'secs'))) %>%
    # time to answer
    mutate(dur_conn = as.integer(difftime(agent.connectedtoagenttimestamp, queue.dequeuetimestamp, unit = 'secs'))) %>%
    # total customer in-call time
    mutate(dur_call = as.integer(difftime(agent.aftercontactworkstarttimestamp, agent.connectedtoagenttimestamp, unit = 'secs'))) %>%
    # interaction time from connect
    mutate(dur_call_interact = as.integer(agent.agentinteractionduration)) %>%
    # hold time
    mutate(dur_call_hold = as.integer(agent.customerholdduration)) %>%
    # mutate(dur_aft = as.integer(difftime(agent.aftercontactworkendtimestamp, agent.aftercontactworkstarttimestamp, unit = 'secs'))) %>%
    mutate(dur_aft = as.integer(agent.aftercontactworkduration)) %>%
    mutate(dur_dis_upd = as.integer(difftime(lastupdatetimestamp, disconnecttimestamp, unit = 'secs'))) %>%
    mutate(dur_total = as.integer(difftime(lastupdatetimestamp, initiationtimestamp, unit = 'secs'))) %>%
    
    # flags
    mutate(flag_weekday = case_when(when_day %in% c('monday', 'tuesday', 'wednesday', 'thursday', 'friday') ~ 1, T ~ 0)) %>%
    mutate(flag_queued = case_when(!is.na(tm_quenq) ~ 1, T ~ 0)) %>%
    mutate(flag_queuednot = case_when(flag_queued == 1 ~0, T ~ 1)) %>%
    mutate(flag_answer = case_when(!is.na(tm_agcon) ~ 1, T ~ 0)) %>%
    mutate(flag_inbound = case_when(initiationmethod == 'INBOUND' ~ 1, T ~ 0)) %>%
    mutate(flag_outbound = case_when(initiationmethod == 'OUTBOUND' ~ 1, T ~ 0)) %>%
    mutate(flag_other = case_when(!initiationmethod %in% c('INBOUND','OUTBOUND') ~ 1, T ~ 0)) %>%
    mutate(flag_answerin20 = case_when(flag_answer == 1 & (dur_enq_deq < 21) ~ 1, T ~ 0)) %>%
    mutate(flag_calllonger30 = case_when(dur_call > 30 ~ 1, T ~ 0)) %>%
    
    # phone number they called formatted as a key for reference data
    mutate(system_phone_number = str_replace(systemendpoint.address,'\\+','')) %>%
  
    identity()  
}

fn_CALL_ReferenceData <- function(df_calls) {
  
  
  
  # adviser data from okta
  df_joined <- df_calls 
  
  
}


# 
# strfix <- function(ss) {
#   return(str_replace_all(ss, '[\n\r]', ''))
# }
# 
# # remove line feeds and carriage returns from text fields
# df_calls <- data.frame(lapply(df_calls, strfix)) %>%
#   mutate(across(starts_with('dur_'), as.integer)) %>%
#   mutate(across(starts_with('flag_'), as.integer))
# 
# # define the rules for splitting each dataset
# df_calls %>%
#   mutate(dataset_htc = case_when(service == 'Help To Claim' ~ 1, T ~ 0)) %>%
#   mutate(dataset_pensionwise =  case_when(service == 'Pension Wise' ~ 1, T ~ 0))


# transform a dataframe of ctrs to a dataframe of calls
# CREATES : dataframe called df_calls_orig which is a dataframe where ...
#           ROWS : represent call records
#           COLS : just the interesting variables
