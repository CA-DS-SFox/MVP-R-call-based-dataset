

# read the .parquet file exported from AWS
# select the variables that will be in Max's inclusion list

# March data extracted like this ...
# SELECT *
# FROM contact_trace_records
# WHERE initiationtimestamp > '2023-03-01T00:00:00Z' and initiationtimestamp < '2023-04-01T00:00:00Z'


fn_CTR_data_get <- function(month = 'March', reduce = TRUE) {
  
  file_to_get <- here('data', paste0('CTR-', tolower(month) ,'.parquet'))
  print(paste0(' ... Getting ',file_to_get))

  # get the March contact_trace_record CTR data 
  df <- read_parquet(file_to_get, col_types = cols(.default='c'))
  if (!reduce) {
    print(paste0(' ... Returning. Inclusion list NOT applied'))
    return(df)
  } else {
    print(paste0(' ... Applying inclusion list to remove redundant variables'))
  }
  
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
  
  # keep the columns from the include list
  df_useful <- df %>% select(all_of(cols_include_list))
  
  return(df_useful)
}

# remove spurious DISCONNECT and TRANSFER records to leave a clean CTR based dataset
# NOTE : All DISCONNECT records are artefacts, same TRANSFER records for OUTBOUND
#        but TRANSFER records for INBOUND seem valid
fn_CTR_data_clean_pass1 <- function(df_input) {
  
  # ~~~~~~~~~~~~~~~~~~
  # first reduce the recordset by throwing away redundant records
  df_ctr_basic <- df_input %>% 
    # make a ctr_setid, and a variable to store the junk condition
    mutate(pipe.ctr_setid = case_when(is.na(initialcontactid) ~ contactid, T ~ initialcontactid), .before = 1) %>%
    # make dummy variables
    mutate(pipe.ctr_orig = 0, .after = 'pipe.ctr_setid') %>% 
    mutate(pipe.ctr_junk = 0, .after = 'pipe.ctr_orig') %>% 
    mutate(junk = 'NO', .after = 'pipe.ctr_junk') %>%
    mutate(junk = case_when(initiationmethod == 'DISCONNECT' ~ 'DISCONNECT CTR', T ~ junk)) %>% 
    mutate(leg_count = 0, .after = 'junk') %>% 
    mutate(leg_id = 0, .after = 'leg_count') %>% 
    mutate(dup_check = paste0(initiationmethod,':',initiationtimestamp,':',disconnectreason,':',disconnecttimestamp), .after = 'pipe.ctr_junk') %>% 
    mutate(pipe.queue.hops = NA, .after = 'pipe.ctr_junk') %>% 
    mutate(pipe.queue.duration = NA, .after = 'pipe.queue.hops') %>% 
    mutate(pipe.queue.total = NA, .after = 'pipe.queue.duration') %>% 
    # collect some original audit stats
    group_by(pipe.ctr_setid) %>% 
    # original numbers of ctrs
    mutate(pipe.ctr_orig = n()) %>% 
    # number of ctrs junked
    mutate(pipe.ctr_junk = sum(!junk == 'NO')) %>% 
    ungroup() %>% 
    identity()
  
  # split off the junk records
  df_ctr_junk <- df_ctr_basic %>% filter(!junk == 'NO')
  df_ctr <- df_ctr_basic %>% filter(junk == 'NO')
  
  # ~~~~~~~~~~~~~~~~~~
  df_ctr_useful <- df_ctr %>% 
    # find the number of CTR legs in the call
    group_by(pipe.ctr_setid) %>%
    mutate(leg_count = n()) %>%
    mutate(leg_id = row_number()) %>%
    
    # find the call type
    mutate(pipe.call_type = case_when(any(initiationmethod == 'INBOUND') ~ 'INBOUND', 
                                 any(initiationmethod == 'OUTBOUND') ~ 'OUTBOUND',
                                 T ~ 'OTHER'), .after = 1) %>% 
    
    ungroup()
  
  # ~~~~~~~~~~~~~~~~~~
  # then split into single leg calls and multi leg calls because
  # single leg calls are fine as they are
  df_ctr_single <- df_ctr_useful %>% filter(leg_count == 1)
  df_ctr_multiple <- df_ctr_useful %>% filter(leg_count > 1)
  
  # ~~~~~~~~~~~~~~~~~~
  # Now evaluate the multi CTR dataset for records to junk
  df_ctr_eval <- df_ctr_multiple %>% 
    
    group_by(pipe.ctr_setid) %>% 
    arrange(pipe.ctr_setid, initiationtimestamp) %>% 
    
    # check for randomly created duplicates
    mutate(junk = case_when(dup_check == lag(dup_check) ~ 'DUPLICATE OF PREVIOUS CTR', 
                            T ~ junk)) %>% 
    
    # for outbound calls, throw away any legs greater than 1 with an inititationmethod of 'OUTBOUND'
    # these seem to be duplicates on every occurance that I have checked
    mutate(junk = case_when(row_number() > 1 & initiationmethod == 'OUTBOUND' ~ 'OUTBOUND NOT POSITION 1 IN CTR SET', 
                            T ~ junk)) %>% 
    
    # for inbound calls, throw away any records with an initiationmethod of 'TRANSFER'
    # and where the queue.name is blank AND the agent.username is blank, these seem to be system artefacts
    mutate(junk = case_when(pipe.call_type == 'INBOUND' & 
                              initiationmethod == 'TRANSFER' & 
                              is.na(agent.username) & 
                              is.na(queue.name) ~ 'INBOUND BLANK TRANSFERS', 
                            T ~ junk)) %>% 
    
    # for outbound calls, throw away any records with an initiationmethod of 'TRANSFER'
    # and where the agent.username is blank, these seem to be system artefacts
    mutate(junk = case_when(pipe.call_type == 'OUTBOUND' & 
                              initiationmethod == 'TRANSFER' & is.na(agent.username) ~ 'OUTBOUND BLANK TRANSFERS', 
                            T ~ junk)) %>% 
    
    # for outbound calls, throw away any records with an initiationmethod of 'TRANSFER'
    # and disconnectreason of 'CONTACT_FLOW_DISCONNECT' or 'CUSTOMER_DISCONNECT', these seem to be system artefacts
    mutate(junk = case_when(pipe.call_type == 'OUTBOUND' &
                              initiationmethod == 'TRANSFER' &
                              disconnectreason %in% c('CONTACT_FLOW_DISCONNECT','CUSTOMER_DISCONNECT') ~ 'INBOUND TRANSFER TO CONTACT_FLOW_DISCONNECT',
                            T ~ junk)) %>%
    
    # update the ctr_junk count
    mutate(pipe.ctr_junk = pipe.ctr_junk + sum(!junk == 'NO')) %>% 
    ungroup() 
  
  
  # ~~~~~~~~~~~~~~~~~~
  # add new junk records from the multiple CTR Call sets
  df_ctr_junk <- df_ctr_junk %>% 
    bind_rows(df_ctr_eval %>% filter(!junk == 'NO'))
  
  # these are ok, but now some will be single CTR sets
  df_ctr_ok <- df_ctr_eval %>% filter(junk == 'NO')
  
  df_ctr_clean_single <- df_ctr_ok %>% 
    group_by(pipe.ctr_setid) %>% 
    filter(n() == 1) %>% 
    ungroup()
  
  df_ctr_clean_multiple <- df_ctr_ok %>% 
    group_by(pipe.ctr_setid) %>% 
    filter(n() > 1) %>% 
    ungroup()
  
  # reevaluate the datasets
  df_ctr_single <- df_ctr_single %>% 
    bind_rows(df_ctr_clean_single) %>% 
    group_by(pipe.ctr_setid) %>%
    mutate(leg_count = n()) %>%
    mutate(leg_id = row_number()) %>%
    ungroup()
  
  df_ctr_multiple <- df_ctr_clean_multiple %>% 
  group_by(pipe.ctr_setid) %>%
    mutate(leg_count = n()) %>%
    mutate(leg_id = row_number()) %>%
    ungroup()
  
  # ~~~~~~~~~~~~~~~~~~
  df_ctr_junk <- df_ctr_junk %>% 
    group_by(pipe.ctr_setid) %>% 
    arrange(pipe.ctr_setid, initiationtimestamp) %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()
  
  df_ctr_single <- df_ctr_single %>% 
    group_by(pipe.ctr_setid) %>% 
    arrange(pipe.ctr_setid, initiationtimestamp) %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()
  
  df_ctr_multiple <- df_ctr_multiple %>% 
    group_by(pipe.ctr_setid) %>% 
    arrange(pipe.ctr_setid, initiationtimestamp) %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()

  # ~~~~~~~~~~~~~~~~~~

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
    mutate(pipe.ctr_setid = case_when(initiationmethod == 'TRANSFER' ~ contactid, T ~ pipe.ctr_setid)) %>% 
    group_by(pipe.ctr_setid) %>% 
    arrange(pipe.ctr_setid, initiationtimestamp) %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                row_number() == 1 ~ 'first',
                                row_number() == n() ~ 'final',
                                T ~ 'middle'), .after = 7) %>%
    ungroup()
  
  # add single records to the single dataset
  df_ctr_2_single <- df_ctr %>% filter(pipe.ctr_type == 'only')
  df_ctr_single <- df_ctr_single %>% bind_rows(df_ctr_2_single)
  
  # keep multiple ctr call sets
  df_ctr_2_multi <- df_ctr %>% filter(pipe.ctr_type != 'only')
  
  # return
  df_list <- list(ctr_single = df_ctr_single,
                  ctr_multi = df_ctr_2_multi)
  
  return(df_list)
}

# Input MUST be a dataframe of multi-CTR calls
fn_CTR_data_clean_pass3 <- function(df_ctr_single, df_ctr_multi) {
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # for single ctr calls queue info is just the same as the raw data
  df_ctr_single <- df_ctr_single %>% 
    mutate(pipe.queue.hops = queue.name) %>% 
    mutate(pipe.queue.duration = queue.duration) %>% 
    mutate(pipe.queue.total = as.integer(queue.duration))

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # for multi-ctr call datastes we need to collapse across all the ctrs
  
  # get queue info from across the whole record set
  df_ctr_multi <- df_ctr_multi %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.queue.hops = paste(queue.name, collapse = ', ')) %>% 
    mutate(pipe.queue.duration = paste(queue.duration, collapse = ', ')) %>% 
    mutate(pipe.queue.total = sum(as.integer(queue.duration))) %>% 
    ungroup() 
  
  # then get info from first ctr
  df_first <- df_ctr_multi %>% 
    filter(pipe.ctr_type == 'first') %>% 
    select(pipe.ctr_setid:customerendpoint.address) %>% 
    mutate(pipe.ctr_type = 'multiple') %>%
    identity()
    
  df_final <- df_ctr_multi %>% 
    filter(pipe.ctr_type == 'final') %>% 
    select(pipe.ctr_setid, transferredtoendpoint.address:last_col()) %>% 
    identity()
  
  # create a call record from the relevant bits of first, middle, final ctrs
  df_call <- df_first %>% 
    left_join(df_final, by = 'pipe.ctr_setid') %>% 
    bind_rows(df_ctr_single)
  
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
fn_CALL_to_ANALYSIS <- function(df_calls_input) {
  
  df_calls_output <- df_calls_input %>%
    # for inbound timestamps initiation == connected
    # for outbound etc, init is when the operator started some action, and connected is when the call was answered
    # sometimes connected is blank
    
    # we need more granular date and time fields for some timestamps
    # we need more granular date and time fields for some timestamps
    mutate(initiationtimestamp = substr(initiationtimestamp, 1, 19)) %>% 
    mutate(initiationtimestamp = str_replace(initiationtimestamp, 'T', ' ')) %>% 
    
    mutate(pipe.when_date = as.Date(substr(as.POSIXct(initiationtimestamp), 1, 10))) %>%
    mutate(pipe.when_week = format(pipe.when_date,'%Y-%W')) %>%
    mutate(pipe.when_day = format(pipe.when_date, '%A')) %>%
    mutate(pipe.when_day = factor(pipe.when_day, levels = c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))) %>%
    mutate(pipe.when_month = format(pipe.when_date, '%Y-%m')) %>%
    mutate(pipe.when_time = substr(initiationtimestamp, 12, 19)) %>%
    mutate(pipe.when_hour = substr(pipe.when_time, 1, 2)) %>%
    mutate(pipe.when_minute = substr(pipe.when_time, 4, 5)) %>%
    mutate(pipe.when_second = substr(pipe.when_time, 7, 8)) %>% 
    
    # times without date
    mutate(pipe.tm_conn = substr(connectedtosystemtimestamp, 12, 19)) %>%
    mutate(pipe.tm_quenq = substr(queue.enqueuetimestamp, 12, 19)) %>%
    mutate(pipe.tm_qudeq = substr(queue.dequeuetimestamp, 12, 19)) %>%
    mutate(pipe.tm_agcon = substr(agent.connectedtoagenttimestamp, 12, 19)) %>%
    mutate(pipe.tm_tranf = substr(transfercompletedtimestamp, 12, 19)) %>%
    mutate(pipe.tm_agwrs = substr(agent.aftercontactworkstarttimestamp, 12, 19)) %>%
    mutate(pipe.tm_agwre = substr(agent.aftercontactworkendtimestamp, 12, 19)) %>%
    mutate(pipe.tm_disc = substr(disconnecttimestamp, 12, 19)) %>%
    mutate(pipe.tm_updat = substr(lastupdatetimestamp, 12, 19)) %>%
    
    # duration in each state
    # mutate(dur_init_conn = as.integer(difftime(connectedtosystemtimestamp, initiationtimestamp, unit = 'secs'))) %>%
    # mutate(dur_init_que = as.integer(difftime(queue.enqueuetimestamp, initiationtimestamp, unit = 'secs'))) %>%
    # mutate(dur_enq_deq = as.integer(difftime(queue.dequeuetimestamp, queue.enqueuetimestamp, unit = 'secs'))) %>%
    # mutate(dur_deq_agnt = as.integer(difftime(agent.connectedtoagenttimestamp, queue.dequeuetimestamp, unit = 'secs'))) %>%
    # # time to answer
    # mutate(dur_conn = as.integer(difftime(agent.connectedtoagenttimestamp, queue.dequeuetimestamp, unit = 'secs'))) %>%
    # # total customer in-call time
    # mutate(dur_call = as.integer(difftime(agent.aftercontactworkstarttimestamp, agent.connectedtoagenttimestamp, unit = 'secs'))) %>%
    # # interaction time from connect
    # mutate(dur_call_interact = as.integer(agent.agentinteractionduration)) %>%
    # # hold time
    # mutate(dur_call_hold = as.integer(agent.customerholdduration)) %>%
    # # mutate(dur_aft = as.integer(difftime(agent.aftercontactworkendtimestamp, agent.aftercontactworkstarttimestamp, unit = 'secs'))) %>%
    # mutate(dur_aft = as.integer(agent.aftercontactworkduration)) %>%
    # mutate(dur_dis_upd = as.integer(difftime(lastupdatetimestamp, disconnecttimestamp, unit = 'secs'))) %>%
    # mutate(dur_total = as.integer(difftime(lastupdatetimestamp, initiationtimestamp, unit = 'secs'))) %>%
    
    # flags
    mutate(pipe.flag_weekday = case_when(pipe.when_day %in% c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday') ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_queued = case_when(!is.na(pipe.tm_quenq) ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_queuednot = case_when(pipe.flag_queued == 1 ~0, T ~ 1)) %>%
    mutate(pipe.flag_answer = case_when(!is.na(pipe.tm_agcon) ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_inbound = case_when(initiationmethod == 'INBOUND' ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_outbound = case_when(initiationmethod == 'OUTBOUND' ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_other = case_when(!initiationmethod %in% c('INBOUND','OUTBOUND') ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_answerin20 = case_when(pipe.flag_answer == 1 & (queue.duration < 21) ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_calllonger30 = case_when(agent.agentinteractionduration > 30 ~ 1, T ~ 0)) %>%
    
    # fix text
    mutate(attributes.question1 = fn_STRING_fixes(attributes.question1)) %>% 
    mutate(attributes.question2 = fn_STRING_fixes(attributes.question2)) %>% 
    mutate(attributes.question3 = fn_STRING_fixes(attributes.question3)) %>% 
    mutate(attributes.question4 = fn_STRING_fixes(attributes.question4)) %>% 
    mutate(attributes.question5 = fn_STRING_fixes(attributes.question5)) %>% 
    
    identity()  
  
  return(df_calls_output)
}

# this works for either calls or ctrs
fn_CALL_ReferenceData <- function(df_input) {
  # source('R_INCLUDE_References.R')
  
  df_ref_okta <- fn_REF_get('okta')
  df_output <- df_input %>% 
    left_join(df_ref_okta, by = c('agent.username' = 'okta_id'))
  
  df_ref_mbr <- fn_REF_get('mbr')
  df_output <- df_output %>% 
    left_join(df_ref_mbr %>% select(member_aws, ref.lss.fullname, ref.lss.shortname), by = c('attributes.formember' = 'member_aws')) 
  
  df_ref_phonenos <- fn_REF_get('phonenos')
  df_output <- df_output %>% 
    left_join(df_ref_phonenos, by = c('systemendpoint.address' = 'ref.phone')) %>% 
    left_join(df_ref_phonenos %>% select(ref.transferno = ref.phone, ref.transfer.service = ref.phone.service), by = c('transferredtoendpoint.address' = 'ref.transferno'))
  
  df_ref_queue <- fn_REF_get('queue')
  df_output <- df_output %>% 
    left_join(df_ref_queue %>% select(ref.queue, ref.queue.service, ref.queue.description), by = c('queue.name' = 'ref.queue'))
  
  df_ref_algroups <- fn_REF_get('algroup')
  df_output <- df_output %>% 
    left_join(df_ref_algroups %>% select(member_aws, ref.formember.advicegroup = member_group), by = c('attributes.formember' = 'member_aws')) %>% 
    left_join(df_ref_algroups %>% select(member_aws, ref.okta.advicegroup = member_group), by = c('ref.okta.member' = 'member_aws')) 

  df_ref_single <- fn_REF_get('single')
  df_output <- df_output %>% 
    left_join(df_ref_single %>% select(single_queue, ref.single_group = single_group), by = c('queue.name' = 'single_queue')) 
    
  df_ref_alink <- fn_REF_get('alink')
  df_output <- df_output %>% 
    left_join(df_ref_alink, by = c('systemendpoint.address' = 'ref.alink.phone'))

  return(df_output)

}

# define service filters from either called number or queue
# this works for either calls or ctrs
fn_CALL_dataset_defs <- function(df_input) {
  
  df_output <- df_input %>% 
    mutate(pipe.dataset_advice_line = case_when(ref.phone.service == 'Adviceline' | ref.queue.service == 'Adviceline' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_advice_link = case_when(ref.phone.service == 'Advicelink' | ref.queue.service == 'Advicelink' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_brum_det = case_when(ref.phone.service == 'Birmingham Debt' | ref.queue.service == 'Birmingham Debt' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_brum_mac = case_when(ref.phone.service == 'Birmingham Macmillan' | ref.queue.service == 'Birmingham Macmillan' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_client = case_when(ref.phone.service == 'Client Services' | ref.queue.service == 'Client Services' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_consumer_ops = case_when(ref.phone.service == 'Consumer Operations' | ref.queue.service == 'Consumer Operations' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_consumer_srv = case_when(ref.phone.service == 'Consumer Service' | ref.queue.service == 'Consumer Service' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_dudley = case_when(ref.phone.service == 'Dudley Empowerment' | ref.queue.service == 'Dudley Empowerment' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_edf = case_when(ref.phone.service == 'EDF' | ref.queue.service == 'EDF' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_eu = case_when(ref.phone.service == 'EU Citizens Rights' | ref.queue.service == 'EU Citizens Rights' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_iow = case_when(ref.phone.service == 'Healthwatch Isle of Wight' | ref.queue.service == 'Healthwatch Isle of Wight' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_hth = case_when(ref.phone.service == 'Help Through Hardship' | ref.queue.service == 'Help Through Hardship' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_htc = case_when(ref.phone.service == 'Help To Claim' | ref.queue.service == 'Help To Claim' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_imm = case_when(ref.phone.service == 'Advisory Immigration' | ref.queue.service == 'Advisory Immigration' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_it = case_when(ref.phone.service == 'IT Service Desk' | ref.queue.service == 'IT Service Desk' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_map = case_when(ref.phone.service == 'MAPSDAP' | ref.queue.service == 'MAPSDAP' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_net = case_when(ref.phone.service == 'Network Support' | ref.queue.service == 'Network Support' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_pen = case_when(ref.phone.service == 'Pension Wise' | ref.queue.service == 'Pension Wise' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_pho = case_when(ref.phone.service == 'Phones Team' | ref.queue.service == 'Phones Team' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_wit = case_when(ref.phone.service == 'Witness Service' | ref.queue.service == 'Witness Service' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_fix = case_when(ref.phone.service == 'unclaimed' | ref.queue.service == 'unclaimed' ~ 1, T ~ 0)) %>% 
    identity()
  
  return (df_output)
}

# put the final data in a human-friendly order
fn_reorder <- function(df_input) {
  
  col_order <- c("pipe.ctr_setid","pipe.call_type",
                 "pipe.ctr_orig", "pipe.ctr_junk","pipe.ctr_type",
                 
                 "contactid","initialcontactid","nextcontactid","previouscontactid",
                 "initiationmethod","disconnectreason",
                 "systemendpoint.address","ref.phone.service","ref.alink.service",
                 "customerendpoint.address",
                 "transferredtoendpoint.address", "ref.transfer.service",
                 "queue.name","ref.queue.service","ref.queue.description","queue.duration",
                 "pipe.queue.hops", "pipe.queue.duration", "pipe.queue.total", 
                 "attributes.nationalreason",
                 "attributes.keypress",
                 "agent.username","ref.okta.advisername","ref.okta.member","ref.okta.office","ref.okta.advicegroup",
                 "agent.routingprofile.name",
                 "agent.hierarchygroups.level1.groupname",
                 "agent.hierarchygroups.level2.groupname",
                 "agent.hierarchygroups.level3.groupname",
                 "agent.hierarchygroups.level4.groupname",
                 "agent.hierarchygroups.level5.groupname",
                 
                 "pipe.when_date","pipe.when_week","pipe.when_day","pipe.when_month","pipe.when_time","pipe.when_hour","pipe.when_minute","pipe.when_second",
                 "pipe.flag_weekday","pipe.flag_queued","pipe.flag_queuednot","pipe.flag_answer","pipe.flag_inbound","pipe.flag_outbound","pipe.flag_other","pipe.flag_answerin20","pipe.flag_calllonger30",

                 "agent.customerholdduration","agent.longestholdduration","agent.agentinteractionduration","agent.aftercontactworkduration",
                 "agent.numberofholds",
                 "agentconnectionattempts",

                 "attributes.servicename",
                 "attributes.finalservice",
                 "attributes.flowselection",
                 "attributes.outcome",
                 "attributes.formember","ref.lss.fullname","ref.lss.shortname","ref.formember.advicegroup","ref.single_group",
                 "attributes.appname",
                 "attributes.numberofquestion",
                 "attributes.question1",
                 "attributes.question2",
                 "attributes.question3",
                 "attributes.question4",
                 "attributes.question5",
                 "attributes.question1response",
                 "attributes.question2response",
                 "attributes.question3response",
                 "attributes.question4response",
                 "attributes.question5response",
                 "pipe.tm_conn","pipe.tm_quenq","pipe.tm_qudeq","pipe.tm_agcon","pipe.tm_tranf","pipe.tm_agwrs","pipe.tm_agwre","pipe.tm_disc","pipe.tm_updat",
                 "pipe.dataset_advice_line",
                 "pipe.dataset_advice_link",
                 "pipe.dataset_brum_det",
                 "pipe.dataset_brum_mac",
                 "pipe.dataset_client",
                 "pipe.dataset_consumer_ops",
                 "pipe.dataset_consumer_srv",
                 "pipe.dataset_dudley",
                 "pipe.dataset_edf",
                 "pipe.dataset_eu",
                 "pipe.dataset_iow",
                 "pipe.dataset_hth",
                 "pipe.dataset_htc",
                 "pipe.dataset_imm",
                 "pipe.dataset_it",
                 "pipe.dataset_map",
                 "pipe.dataset_net",
                 "pipe.dataset_pen",
                 "pipe.dataset_pho",
                 "pipe.dataset_wit",
                 "pipe.dataset_fix")
  
  df_output <- df_input %>% 
    select(all_of(col_order))
}

# get reference datasets
fn_REF_get <- function(what, show = FALSE) {
  
  what <- tolower(what)
  # reference tables
  ref_dir <- 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Reference Tables/'
  
  print(paste0(' ... getting ref data from ',ref_dir))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'okta') {
    # advisers from okta 
    # join key is agent.username = okta_id
    ref_file_okta <- 'reporting_oktaadvisers.csv'
    # print(ref_file_okta)
    
    df_ref_okta <- read_csv(paste0(ref_dir, ref_file_okta), col_types = cols(.default='c')) %>% 
      filter(!is.na(okta_id)) %>% 
      rename(ref.okta.member = member_aws,
             ref.okta.advisername = reportingname,
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
    df_ref_queue <- read_parquet(paste0(ref_dir, ref_file_queue), col_types = cols(.default='c')) %>% 
      mutate(ref.queue.description = case_when(ref.queue.description == '#REF!' ~ '', T ~ ref.queue.description))
    
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

# remove hashtags from strings
fn_STRING_fixes <- function(text_in) {
  
  #print(text_in)
  text_out <- text_in
  text_out <- str_remove_all(text_out,"<speak>")
  text_out <- str_remove_all(text_out,"</speak>")
  text_out <- str_remove_all(text_out, "<break time = '0.25s'/>")
  text_out <- str_remove_all(text_out, "<break time = '0.2s'/>")
  text_out <- str_remove_all(text_out, "<break time = '2s'/>")
  text_out <- str_remove_all(text_out,"<break time = '1s'/>")
  text_out <- str_remove_all(text_out, "Thank you.")
  
  
  text_out <- str_remove(text_out, "\nFor very good, press 1  For good, press 2")
  text_out <- str_remove(text_out, "\nFor neither good nor poor, press 3 For\npoor, press 4. For very poor, press 5")
  text_out <- str_remove(text_out, " For\ndon't know, press 6")
  
  text_out <- str_remove(text_out, "\nPress 1 for very satisfied")
  text_out <- str_remove(text_out, "\nPress 2 for fairly satisfied")
  text_out <- str_remove(text_out, "\nPress 3 for neither satisfied nor dissatisfied")
  text_out <- str_remove(text_out, "\nPress 4 for fairly dissatisfied")
  text_out <- str_remove(text_out, "\nPress 5 for very dissatisfied.")
  
  text_out <- str_remove(text_out, "For very easy, press 1 ")
  text_out <- str_remove(text_out, "For easy, press 2 For a bit difficult, press 3 ")
  text_out <- str_remove(text_out, "Or for really difficult, press 4 ")
  text_out <- str_remove(text_out, ", Press 1 for Yes, Press 2 for No")
  
  text_out <- str_remove(text_out,"\nVery satisfied press 1 ")
  text_out <- str_remove(text_out,"Fairly satisfied press 2 ")
  text_out <- str_remove(text_out,"Neither satisfied nor dissatisfied press 3 ")
  text_out <- str_remove(text_out,"Fairly dissatisfied press 4 ")
  text_out <- str_remove(text_out,"Very dissatisfied press 5 ")
  text_out <- str_remove(text_out,"Neither satisfied nor dissatisfied press 3 ")
  
  text_out <- str_remove(text_out,"Press 1 for Very Easy Press 2 for Easy Press 3 for a bit difficult,")
  text_out <- str_remove(text_out," or Press 4 for Really difficult ")
  
  text_out <- str_remove(text_out,"Thankyou.")
  text_out <- str_remove(text_out,"\nFor very easy, press 1 For easy, press 2")
  text_out <- str_remove(text_out,"For neither easy nor difficult, press 3 For\ndifficult, press 4")
  text_out <- str_remove(text_out,"For very difficult, press 5\nFor don't know, press 6")
  text_out <- str_remove(text_out,"\nPress 1 for yes\nPress 2 for no\nPress 3 if you did not receive a text reminder.")
  
  text_out <- str_remove(text_out,"\nYes, press 1 No, press 2 Did not receive advice on this call press 3")
  text_out <- str_remove(text_out,"For very satisfied, press 1 For satisfied, press 2  For dissatisfied, ")
  
  text_out <- str_remove(text_out,"\nFor easy, press 2")
  text_out <- str_remove(text_out,"press 3  For really dissatisfied, press 4")
  text_out <- str_remove(text_out,",Press 1 for very satisfied, Press 2 for satisfied ")
  text_out <- str_remove(text_out,", Press 3 for dissatisfied , Press 4 for very dissatisfied")
  text_out <- str_remove(text_out,"Press 1, for Very satisfiedPress 2, for Satisfied ")
  
  text_out <- str_remove(text_out,"Press 3, for Dissatisfied,or\nPress 4, for Really dissatisfied")
  text_out <- str_remove(text_out,"Press 1 for very satisfied  Press 2 for satisfied  Press 3 for dissatisfied")
  text_out <- str_remove(text_out,"Press 4 for really dissatisfied")
  text_out <- str_remove(text_out,"Press 1 for yesPress 2 for no.")
  text_out <- str_remove(text_out,"Press 1 for yes  Press 2 for no")
  text_out <- str_remove(text_out,"Pwyswch 1 am Byddwn  Pwyswch 2 am Na Fyddwn.")
  
  text_out <- str_remove(text_out,", Press 1 if you established you didn't have any rights ,Press 2 if the adviser was unhelpful ,")
  text_out <- str_remove(text_out,"Press 3 if  the adviser did not understand your problem ,Press 4 if the adviser could not solve your problem ,")
  
  text_out <- str_remove(text_out,"Press 5 if you had problems getting through or contacting the service")
  text_out <- str_remove(text_out,"\nFor completely, press 1 For a great extent,press 2 For a moderate extent, ")
  text_out <- str_remove(text_out,"press 3 For a small extent, press 4 For not at all, press 5 For don't know, press 6")
  text_out <- str_remove(text_out,"\nFor completely, press 1 For mostly, press2 For partly, press 3 For slightly, press 4For not at all, press 5 For don't know, press 6")
  text_out <- str_remove(text_out,"For yes, press 1  For no, press 2")
  
  text_out <- str_remove(text_out,"\nFor very likely, press 1 For likely, press 2\nFor neither likely nor unlikely, press 3 For unlikely, ")
  text_out <- str_remove(text_out,"press 4 For very unlikely, press 5For don't know, press 6")
  # text_out <- str_remove(text_out,"")
  # text_out <- str_remove(text_out,"")
  
  text_out <- str_trim(text_out, "right")
  text_out <- str_trim(text_out, "left")
  return(text_out)
}

