

# RETURN the parquet data for the month specified from the data subfolder
# parquet data for each month was manually extracted from the AWS table [prod-rap-data-science].[contact_trace_records]
#     March data extracted like this ...
#     SELECT *
#     FROM contact_trace_records
#     WHERE initiationtimestamp > '2023-03-01T00:00:00Z' and initiationtimestamp < '2023-04-01T00:00:00Z'
# if reduce is set to TRUE then variables from the source data are reduced to those which (I assume) will be 
# in Max's AWS inclusion list
fn_CTR_data_get <- function(month = 'March', reduce = TRUE, set_info = FALSE) {
  
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
  
  # add a setid 
  if (set_info == TRUE) {
    print(' ... Adding the variables that define a call set')
    df_useful <- df_useful %>% 
      # make a ctr_setid, and count the number of ctrs in the call
      mutate(pipe.ctr_setid = case_when(is.na(initialcontactid) ~ contactid, T ~ initialcontactid), .before = 1) %>% 
      group_by(pipe.ctr_setid) %>% 
      arrange(pipe.ctr_setid, initiationtimestamp) %>% 
      mutate(pipe.ctr_orig = n(), .after = 'pipe.ctr_setid') %>% 
      ungroup() %>% 
      identity()
    
    rpt <- nrow(df_useful)
    
    rpt1 <- df_useful %>% 
      distinct(pipe.ctr_setid) %>% 
      tally()
    
    rpt.calls <- df_useful %>% distinct(pipe.ctr_setid) %>% tally()
    print(paste0(' ... RAW : records=',nrow(df_useful),' calls=',rpt.calls))
    
  }
  
  return(df_useful)
}

# add some useful variables, most will be empty for now and populated later
# input dataframe is all CTRs
fn_CTR_add_variables <- function(df_in) {
  
  df_out <- df_in %>% 
    arrange(pipe.ctr_setid, initiationtimestamp) %>%
    mutate(dup_check = paste0(initiationmethod,':',initiationtimestamp,':',disconnectreason,':',disconnecttimestamp), .after = 'pipe.ctr_setid') %>% 
    mutate(dup_bad = FALSE, .after = 'dup_check') %>% 
    mutate(dup_set = FALSE, .after = 'dup_bad') %>% 
    mutate(pipe.queue.hops = '', .after = 'queue.name') %>% 
    mutate(pipe.queue.duration = '', .after = 'pipe.queue.hops') %>% 
    mutate(pipe.queue.total = '', .after = 'pipe.queue.duration') %>% 
    mutate(pipe.inout_first = '', .after = 'pipe.ctr_orig') %>% 
    mutate(pipe.inout_final = '', .after = 'pipe.inout_first') %>% 
    group_by(pipe.ctr_setid) %>%
    mutate(pipe.call_type = case_when(row_number() == 1 ~ initiationmethod), .after = 'dup_set') %>% 
    tidyr::fill(pipe.call_type) %>% 
    ungroup() %>%
    identity()
  
  return(df_out)
}

# remove any call types not inbound or outbound
# input dataframe is all CTRs
fn_CTR_remove_badtypes <- function(df_in) {
  
  df_out <- df_in %>% filter(pipe.call_type %in% c('INBOUND', 'OUTBOUND'))
  
  rpt.dropped <- nrow(df_in) - nrow(df_out)
  rpt.calls <- df_out %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... INOUT only : dropped ',rpt.dropped,' records.'))
  print(paste0(' ... INOUT only : records=',nrow(df_out),' calls=',rpt.calls))
  return(df_out)  
}

# remove any duplicates, based on the dup_check variable
# there's usually 750-800 each month
# input dataframe is all CTRs with bad types removed
fn_CTR_remove_deduplicates <- function(df_in) {
  
  df_out <- df_in %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(dup_bad = case_when(dup_check == lag(dup_check) ~ TRUE, T ~ dup_bad)) %>% 
    mutate(dup_set = any(dup_bad)) %>% 
    ungroup() %>% 
    filter(dup_bad == FALSE) %>% 
    identity()

  rpt.dropped <- nrow(df_in) - nrow(df_out)
  rpt.calls <- df_out %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... DEDUPLICATE : dropped ',rpt.dropped,' records.'))
  print(paste0(' ... DEDUPLICATE : records=',nrow(df_out),' calls=',rpt.calls))
  
  return(df_out)  
  
}

# split into Inbound and Outbound datasets, and determine CTR order
# input dataframe is all clean CTRs
fn_CTR_split <- function(df_in) {
  
  # identify ctr order
  df_ctrs <- df_in %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                     row_number() == 1 ~ 'first',
                                     row_number() == n() ~ 'final',
                                     T ~ 'middle'), .after = 'pipe.call_type') %>% 
    ungroup()
  
  # SPLIT INTO INBOUND & OUTBOUND
  df_outbound <- df_ctrs %>% filter(pipe.call_type == 'OUTBOUND')
  rpt.out <- df_outbound %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... OUTBOUND : records=',nrow(df_outbound),' calls=',rpt.out))
  
  df_inbound <- df_ctrs %>% filter(pipe.call_type == 'INBOUND')
  rpt.in <- df_inbound %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... INBOUND : records=',nrow(df_inbound),' calls=',rpt.in))
  
  print(paste0(' ... TOTAL : records=',nrow(df_outbound) + nrow(df_inbound),' calls=',rpt.out + rpt.in))
  
  return_list <- list(df_ctrs = df_ctrs,
                      df_outbound = df_outbound,
                      df_inbound = df_inbound)
  
  return(return_list)
}

# collapse outbound CTRs to calls
# input dataframe is outbound CTRS
fn_CALLS_outbound <- function(df_in) {

  # the only data we want from the final CTR is the exit method
  df_exit <- df_in %>% 
    filter(pipe.ctr_type == 'final') %>% 
    mutate(exit = paste0(initiationmethod,' : ',disconnectreason)) %>% 
    select(pipe.ctr_setid, exit)

  # outbound calls only have 1 informative CTR which is the first in the set
  # often there's a second record with initiationmethod = TRANSFER
  # but it's an artefact of the system and has no information
  df_out <- df_in %>% 
    filter(pipe.ctr_type %in% c('first', 'only')) %>% 
    mutate(pipe.ctr_type = 'only') %>% 
    mutate(pipe.inout_first = paste0(initiationmethod,' : ',disconnectreason)) %>% 
    left_join(df_exit, by = 'pipe.ctr_setid') %>% 
    mutate(pipe.inout_final = exit) %>% 
    select(-exit)
  
  rpt.dropped <- nrow(df_in) - nrow(df_out)
  print(paste0(' ... Dropping ',rpt.dropped,' secondary CTR records (TRANSFERS)'))
  
  rpt.out <- df_out %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... OUTBOUND : records=',nrow(df_out),' calls=',rpt.out))
  
  return(df_out)
  
}

# collapse inbound CTRs to calls
# input dataframe is inbound CTRS
fn_CALLS_inbound <- function(df_in) {
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # first process any valid transfers, they need to be recorded
  # as unique calls so make the pipe.ctr_setid unique
  # and reorder the dataset
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  rpt.transfer <- df_in %>% 
    filter(initiationmethod == 'TRANSFER') %>% 
    tally()
  
  if (rpt.transfer > 0) {
    print(paste0(' ... TRANSFER records found, total ',rpt.transfer))
    print(' ... Moving to a new call set')
    
    set.transfer <- df_in %>% 
      filter(initiationmethod == 'TRANSFER') %>% 
      select(pipe.ctr_setid) %>% 
      pull()
    
    # make transfers a new call and re-sort them
    df_transfer <- df_in %>% 
      filter(pipe.ctr_setid %in% set.transfer) %>% 
      mutate(pipe.ctr_setid = case_when(initiationmethod == 'TRANSFER' ~ contactid, T ~ pipe.ctr_setid)) %>% 
      arrange(pipe.ctr_setid, initiationtimestamp) %>% 
      group_by(pipe.ctr_setid) %>% 
      mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                       row_number() == 1 ~ 'first',
                                       row_number() == n() ~ 'final',
                                       T ~ 'middle'), .after = 'pipe.call_type') %>% 
      ungroup() %>% 
      mutate(pipe.call_type = case_when(initiationmethod == 'TRANSFER' ~ 'TRANSFER', T ~ pipe.call_type)) %>% 
      identity()

    # keep the regular records and replace the transfer calls
    df_in <- df_in %>% 
      filter(!pipe.ctr_setid %in% set.transfer) %>% 
      bind_rows(df_transfer)
    
  } else {
    print(paste0(' ... NO INBOUND TRANSFER records found'))
  }

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # save the exit reason from the final cTR
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # save the disconnect reason
  df_exit <- df_in %>% 
    filter(pipe.ctr_type == 'final') %>% 
    mutate(exit = paste0(initiationmethod,' : ',disconnectreason)) %>% 
    select(pipe.ctr_setid, exit) %>% 
    identity()
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Often the final CTR is a initiationmethod = DISCONNECT, the
  # only useful information is the exit reason which we have
  # saved above, so remove these records now, only keep useful CTRs
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  print(' ... Dropping DISCONNECT records')
  df_useful <- df_in %>% 
    filter(initiationmethod != 'DISCONNECT') %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                     row_number() == 1 ~ 'first',
                                     row_number() == n() ~ 'final',
                                     T ~ 'middle'), .after = 'pipe.call_type') %>% 
    ungroup() %>% 
    identity()
  
    
  rpt.dropped <- nrow(df_in) - nrow(df_useful)
  print(paste0(' ... Dropping ', rpt.dropped ,' inbound disconnect records'))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # collapse queue info from across all the CTRs
  print(' ... getting data from across all CTRs')
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_out <- df_useful %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.queue.hops = paste(queue.name, collapse = ', ')) %>% 
    mutate(pipe.queue.duration = paste(queue.duration, collapse = ', ')) %>% 
    mutate(pipe.queue.total = as.character(sum(as.integer(queue.duration)))) %>% 
    ungroup() %>% 
    mutate(pipe.inout_first = case_when(pipe.ctr_type %in% c('first','only') ~ paste0(initiationmethod,' : ',disconnectreason))) %>% 
    left_join(df_exit, by = 'pipe.ctr_setid') %>% 
    mutate(pipe.inout_final = exit) %>% 
    select(-exit) %>% 
    identity()

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # collapse all the info from across the CTRs
  print(' ... Creating call records')
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # we want some information from the first CTR, and some from the final one
  # so split into component ctr types 
  df_inbound_only <- df_out %>% filter(pipe.ctr_type == 'only')  
  df_inbound_first <- df_out %>% filter(pipe.ctr_type == 'first') %>% mutate(pipe.ctr_type = 'multiple')
  df_inbound_final <- df_out %>% filter(pipe.ctr_type == 'final')

  # get the variables from the correct CTR
  a1 <- df_inbound_first %>% select(pipe.ctr_setid:queue.enqueuetimestamp)
  a2 <- df_inbound_final %>% select(pipe.ctr_setid, queue.dequeuetimestamp:last_col())
  a3 <- a1 %>% left_join(a2, by='pipe.ctr_setid')
  
  # join to give a calls based dataset
  df_out <- df_inbound_only %>% bind_rows(a3)
  
  rpt.out <- df_out %>% distinct(pipe.ctr_setid) %>% tally()
  rpt.mult <- df_out %>% filter(pipe.ctr_type == 'multiple') %>% tally()
  print(paste0(' ... INBOUND : records=',nrow(df_out),' calls=',rpt.out, ' multi-CTRs ',rpt.mult))
  
  return(df_out) 
}

# -------------------------------------------------------------------------

# split into single ctr and multi ctr datasets
fn_CTR_data_split <- function(df_in) {
  
  num_calls <- df_in %>% 
    distinct(pipe.ctr_setid) %>% 
    tally()
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  print(' ... single CTR calls : extracting')
  df_single <- df_in %>% 
    filter(pipe.ctr_orig == 1) %>% 
    mutate(pipe.ctr_legid = 1, .after = 'pipe.ctr_orig') %>% 
    mutate(pipe.ctr_type = 'only', .after = 'pipe.ctr_legid') %>%
    mutate(pipe.call_type = initiationmethod, .after = 'pipe.ctr_type') %>% 
    mutate(pipe.inout_first = paste0(initiationmethod,' : ',disconnectreason), .after = 'pipe.call_type') %>% 
    mutate(pipe.inout_final = '', .after = 'pipe.inout_first') %>% 
    mutate(pipe.ctr_junk = 0, .after = 'pipe.ctr_orig') %>% 
    mutate(pipe.ctr_junkreason = '', .after = 'pipe.ctr_junk') %>% 
    identity()
  rpt <- df_single %>% tally()
  print(paste0(' ... There are ',rpt,' single CTR calls '))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  print(' ... multiple CTR calls : extracting')
  df_multiple <- df_in %>% 
    filter(pipe.ctr_orig > 1) %>% 
    arrange(pipe.ctr_setid, initiationtimestamp) %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.ctr_legid = row_number(), .after = 'pipe.ctr_orig') %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                     row_number() == 1 ~ 'first',
                                     row_number() == n() ~ 'final',
                                     T ~ 'middle'), .after = 'pipe.ctr_legid') %>% 
    mutate(pipe.call_type = case_when(any(pipe.ctr_legid == 1 & initiationmethod == 'INBOUND') ~ 'INBOUND',
                                      any(pipe.ctr_legid == 1 & initiationmethod == 'OUTBOUND') ~ 'OUTBOUND',
                                      T ~ 'OTHER'), .after = 'pipe.ctr_type') %>% 
    mutate(pipe.inout_first = paste0(initiationmethod,' : ',disconnectreason), .after = 'pipe.call_type') %>% 
    mutate(pipe.inout_final = '', .after = 'pipe.inout_first') %>% 
    ungroup() %>% 
    mutate(pipe.ctr_junk = 0, .after = 'pipe.ctr_orig') %>% 
    mutate(pipe.ctr_junkreason = '', .after = 'pipe.ctr_junk') %>% 
    identity()
  rpt <- df_multiple %>% tally()
  rpt2 <- df_multiple %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... There are ',rpt2,' multi CTR calls across ',rpt,' records'))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_return <- list(single = df_single, 
                    multiple = df_multiple)
  return(df_return)
  
}

# There are some bad records
fn_CTR_data_junk <- function(df_single, df_multiple, remove = TRUE) {
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # some single ctrs are disconnect from the start  
  print(' ... Removing from single CTR calls - initiationmethod == DISCONNECT')
  df_single <- df_single %>% 
    mutate(pipe.ctr_junkreason = case_when(initiationmethod == 'DISCONNECT' ~ 'SINGLE CTR DISCONNECT',
                                           T ~ pipe.ctr_junkreason)) %>% 
    mutate(pipe.ctr_junk = case_when(pipe.ctr_junkreason != '' ~ 1,
                                     T ~ pipe.ctr_junk)) %>% 
    identity()
  
  rpt <- df_single %>% filter(pipe.ctr_junk == 1) %>% tally()
  print(paste0(' ... ',rpt,' records removed'))
  
  # remove junk records
  if (remove) df_single <- df_single %>% filter(pipe.ctr_junk == 0)
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # check for randomly created duplicates, weird outbound CTRs
  print(' ... Removing from multi CTR calls - duplicate records')
  df_multiple <- df_multiple %>% 
    # use this to check for near duplicates
    mutate(dup_check = paste0(initiationmethod,':',initiationtimestamp,':',disconnectreason,':',disconnecttimestamp)) %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.ctr_junkreason = case_when(dup_check == lag(dup_check) ~ 'DUPLICATE OF PREVIOUS CTR', 
                                           row_number() > 1 & initiationmethod == 'OUTBOUND' ~ 'OUTBOUND NOT CTR 1',
                                           T ~ '')) %>% 
    mutate(ctr_junk = case_when(pipe.ctr_junkreason != '' ~ 1, T ~ 0)) %>% 
    mutate(pipe.ctr_junk = sum(ctr_junk)) %>% 
    mutate(pipe.ctr_junkreason = last(pipe.ctr_junkreason)) %>% 
    ungroup()
  
  rpt <- df_multiple %>% filter(ctr_junk == 1) %>% tally()
  print(paste0(' ... ',rpt,' records removed'))
  
  # remove junk records
  if (remove) {
    df_multiple <- df_multiple %>% 
      filter(ctr_junk == 0) %>% 
      group_by(pipe.ctr_setid) %>% 
      mutate(newcount = n()) %>% 
      ungroup()
    
    # if any have become single ctr calls now, move them to the single dataset
    df_new_single <- df_multiple %>% 
      filter(newcount == 1) %>% 
      select(-dup_check, -ctr_junk, - newcount)
    
    df_single <- df_single %>% 
      bind_rows(df_new_single) %>% 
      mutate(pipe.ctr_type = 'only')
    
    # and multiple we need to recalculate the first/middle/final
    df_multiple <- df_multiple %>% 
      filter(newcount > 1) %>% 
      group_by(pipe.ctr_setid) %>% 
      mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                       row_number() == 1 ~ 'first',
                                       row_number() == n() ~ 'final',
                                       T ~ 'middle'), .after = 'pipe.ctr_legid') %>% 
      ungroup()
    
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # remove temp variables
  df_multiple <- df_multiple %>% 
    select(-dup_check, -ctr_junk, -newcount)
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_return <- list(single = df_single, 
                    multiple = df_multiple)
  return(df_return)
  
}

# make INBOUND TRANSFERS their own call
fn_CTR_data_transfers <- function(df_single, df_multiple) {
  
  print(' ... Checking multi CTR calls for initiationmethod == TRANSFER')
  
  rpt <- df_multiple %>% filter(pipe.call_type == 'INBOUND' & initiationmethod == 'TRANSFER') %>% tally()
  print(paste0(' ... ',rpt,' records moved to single dataset overwriting pipe.ctr_setid with contactid'))
  
  if (rpt > 0) {
    # remove TRANSFER records from the multi CTR set
    df_single <- df_single %>% 
      bind_rows(df_multiple %>% 
                  filter(pipe.call_type == 'INBOUND' & initiationmethod == 'TRANSFER') %>% 
                  mutate(pipe.ctr_setid = contactid) %>% 
                  mutate(pipe.ctr_type = 'only'))
    
    print(' ... Reorganising remaining multi CTR calls')
    df_multiple <- df_multiple %>% 
      filter(!(pipe.call_type == 'INBOUND' & initiationmethod == 'TRANSFER')) %>% 
      group_by(pipe.ctr_setid) %>% 
      mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                       row_number() == 1 ~ 'first',
                                       row_number() == n() ~ 'final',
                                       T ~ 'middle'), .after = 'pipe.ctr_legid') %>% 
      ungroup()
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_return <- list(single = df_single, 
                    multiple = df_multiple)
  return(df_return)
  
}

# collpse into calls
fn_CTR_data_collapse <- function(df_single, df_multiple) {
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  print('SINGLE CTR calls')
  print(' ... single CTR calls : Adding spoof variables')
  df_single <- df_single %>% 
    mutate(pipe.queue.hops = queue.name) %>% 
    mutate(pipe.queue.duration = queue.duration) %>% 
    mutate(pipe.queue.total = as.integer(queue.duration))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # for multiple-CTR outbound calls the FINAL record is always
  # a TRANSFER record, but the rest of the variables are NA
  print('MULTI CTR calls')
  print(' ... multi CTR calls : Getting inititation/disconnect from final record')
  df_exit <- df_multiple %>% 
    filter(pipe.ctr_type == 'final') %>% 
    select(pipe.ctr_setid, exit = pipe.inout_first)
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # process outbound calls
  print(' . MULTI CTR calls - OUTBOUND')
  print(' ... multi CTR calls : Outbound calls, only first record has data')
  rpt <- df_multiple %>% 
    filter(pipe.call_type == 'OUTBOUND') %>% 
    filter(pipe.ctr_type != 'first') %>% 
    tally()
  print(paste0(' ... loosing ', rpt,' non-informative outbound records'))
  
  df_outbound <- df_multiple %>% 
    filter(pipe.call_type == 'OUTBOUND') %>% 
    filter(pipe.ctr_type == 'first')
  
  print(' ... multi CTR calls : Outbound calls, merge exit variable from final record')
  df_out <- df_outbound %>% 
    left_join(df_exit, by = 'pipe.ctr_setid') %>% 
    mutate(pipe.inout_final = exit) %>% 
    select(-exit)
  
  print(' ... multi CTR calls : Outbound calls, adding spoof variables')
  df_out <- df_out %>% 
    mutate(pipe.queue.hops = queue.name) %>% 
    mutate(pipe.queue.duration = queue.duration) %>% 
    mutate(pipe.queue.total = as.integer(queue.duration)) %>% 
    mutate(pipe.ctr_type = 'only')
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # for multiple-CTR inbound calls the FINAL record is sometimes
  # DISCONNECT and sometimes QUEUE_TRANSFER these need to be handled differently
  
  print(' . MULTI CTR calls - INBOUND')
  print(' ... multi CTR calls : Inbound calls, merge exit variable from final record')
  df_inbound <- df_multiple %>% 
    filter(pipe.call_type != 'OUTBOUND') %>% 
    left_join(df_exit, by = 'pipe.ctr_setid') %>% 
    mutate(pipe.inout_final = exit) %>% 
    select(-exit)
  
  callcount <- df_inbound %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... multi CTR calls : ',callcount,' Inbound calls, remove final record if it is inititationmethod = DISCONNECT'))
  
  rpt <- df_inbound %>% 
    filter(initiationmethod == 'DISCONNECT') %>% 
    tally()
  print(paste0(' ... loosing ', rpt,' non-informative inbound records'))
  
  # now we need to throw away the final DISCONNECT records because there is no info on them
  # they are different to when a final record which is QUEUE_TRANSFER and is informative
  df_inbound <- df_inbound %>% 
    filter(initiationmethod != 'DISCONNECT') %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
                                     row_number() == 1 ~ 'first',
                                     row_number() == n() ~ 'final',
                                     T ~ 'middle'), .after = 'pipe.ctr_legid') %>% 
    ungroup()
  
  callcount <- df_inbound %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... multi CTR calls : ',callcount,' Inbound calls, after DISCONNECT removal'))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # move any ctrs which are now 'only' to the single dataset
  df_only <- df_inbound %>% filter(pipe.ctr_type == 'only')
  df_single <- df_single %>% bind_rows(df_only)
  
  callcount <- df_only %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... multi CTR calls : ',callcount,' moved to single CTR dataset'))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # keep remaining true multi CTR inbound calls
  df_inbound <- df_inbound %>% filter(pipe.ctr_type != 'only')
  
  callcount <- df_inbound %>% distinct(pipe.ctr_setid) %>% tally()
  print(paste0(' ... multi CTR calls : ',callcount,' true multi-CTR calls remain'))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # get queue info from across the whole CTR record set
  
  print(' ... multi CTR calls : Inbound calls, counting queue hops over informative records')
  df_inbound <- df_inbound %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(pipe.queue.hops = paste(queue.name, collapse = ', ')) %>% 
    mutate(pipe.queue.duration = paste(queue.duration, collapse = ', ')) %>% 
    mutate(pipe.queue.total = sum(as.integer(queue.duration))) %>% 
    ungroup() 
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  print(' ... multi CTR calls : Inbound calls, create single call record from first and final records')
  
  # initiationtimestamp needs to be the one from the first ctr
  df_inbound <- df_inbound %>% 
    group_by(pipe.ctr_setid) %>% 
    mutate(initiationtimestamp = first(initiationtimestamp)) %>% 
    ungroup()
  
  df_first <- df_inbound %>% 
    filter(pipe.ctr_type %in% c('first')) %>% 
    select(pipe.ctr_setid:customerendpoint.address) %>% 
    mutate(pipe.ctr_type = case_when(pipe.ctr_type == 'first' ~ 'multiple',
                                     T ~ 'only')) %>%
    identity()
  
  df_final <- df_inbound %>% 
    filter(pipe.ctr_type == 'final') %>% 
    select(pipe.ctr_setid, transferredtoendpoint.address:last_col()) %>% 
    identity()
  
  # create a call record from the relevant bits of first, middle, final ctrs
  df_in <- df_first %>% 
    left_join(df_final, by = 'pipe.ctr_setid') 
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  print(' ... combining single CTR and multi CTR output into call based dataset')
  df_calls <- df_single %>% bind_rows(df_out) %>% bind_rows(df_in)
  
  rpt <- df_calls %>% 
    distinct(pipe.ctr_setid) %>% 
    tally()
  
  print(paste0(' ... total calls : ', rpt,' calls' ))
  
  return(df_calls)
  
}

# NOT IMPLEMENTED YET
# check that cleaning assumptions are still valid
# CHECKS : OUTBOUND calls only have 1 leg, report otherwise
# CHECKS : INBOUND transfer records don't look correct, timestamps overlap
fn_CTR_data_check <- function(df_ctr_clean) {
  df_checks <- df_ctr_clean
  return(df_checks)  
}

# CREATE data transformations to give analysable dataset
fn_CALL_to_ANALYSIS <- function(df_calls_input) {
  
  print(' ... Creating transformed variables')
  
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
    
    # time
    mutate(tm_init = as.POSIXct(pipe.when_time, tz = '', format = '%H:%M:%S')) %>% 
    mutate(tm_disc = as.POSIXct(pipe.tm_disc, tz = '', format = '%H:%M:%S')) %>% 
    mutate(pipe.time.total = as.integer(difftime(tm_disc, tm_init, units = 'secs'))) %>% 
    
    mutate(agent.agentinteractionduration = coalesce(as.integer(agent.agentinteractionduration),0)) %>% 
    mutate(agent.aftercontactworkduration = coalesce(as.integer(agent.aftercontactworkduration),0)) %>% 
    mutate(agent.customerholdduration = coalesce(as.integer(agent.customerholdduration),0)) %>% 
    mutate(pipe.talk.total = agent.agentinteractionduration + agent.aftercontactworkduration + agent.customerholdduration) %>% 
    
    # flags
    mutate(pipe.flag_weekday = case_when(pipe.when_day %in% c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday') ~ 1, T ~ 0)) %>%
    
    mutate(pipe.flag_queued = case_when(!is.na(pipe.tm_quenq) ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_queuednot = case_when(pipe.flag_queued == 1 ~0, T ~ 1)) %>%
    mutate(pipe.flag_answer = case_when(!is.na(pipe.tm_agcon) ~ 1, T ~ 0)) %>%
    
    mutate(pipe.flag_queued_abandoned10 = case_when(pipe.flag_queued == 1 & pipe.flag_answer == 0 & pipe.queue.total < 11 ~ 1, T ~ 0)) %>% 
    mutate(pipe.flag_queued_abandoned = case_when(pipe.flag_queued == 1 & pipe.flag_answer == 0 ~ 1, T ~ 0)) %>% 
    
    mutate(pipe.flag_inbound = case_when(initiationmethod == 'INBOUND' ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_outbound = case_when(initiationmethod == 'OUTBOUND' ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_other = case_when(!initiationmethod %in% c('INBOUND','OUTBOUND') ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_answerin20 = case_when(pipe.flag_answer == 1 & (pipe.queue.duration < 21) ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_answerin30 = case_when(pipe.flag_answer == 1 & (pipe.queue.duration < 31) ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_answerin60 = case_when(pipe.flag_answer == 1 & (pipe.queue.duration < 61) ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_calllonger30 = case_when(agent.agentinteractionduration > 30 ~ 1, T ~ 0)) %>%
    mutate(pipe.flag_talk_abandoned5 = case_when(pipe.flag_answer == 1 & pipe.talk.total < 6 ~ 1, T ~ 0)) %>% 
    
    
    # fix survey text
    mutate(attributes.question1 = fn_STRING_fixes(attributes.question1)) %>% 
    mutate(attributes.question2 = fn_STRING_fixes(attributes.question2)) %>% 
    mutate(attributes.question3 = fn_STRING_fixes(attributes.question3)) %>% 
    mutate(attributes.question4 = fn_STRING_fixes(attributes.question4)) %>% 
    mutate(attributes.question5 = fn_STRING_fixes(attributes.question5)) %>% 
    
    # survey
    mutate(q1 = case_when(is.na(attributes.question1response) ~ 0,
                          attributes.question1response %in% c('1','2','3','4','5') ~ 1,
                          T ~ 0)) %>% 
    mutate(q2 = case_when(is.na(attributes.question2response) ~ 0,
                          attributes.question2response %in% c('1','2','3','4','5') ~ 1,
                          T ~ 0)) %>% 
    mutate(q3 = case_when(is.na(attributes.question3response) ~ 0,
                          attributes.question3response %in% c('1','2','3','4','5') ~ 1,
                          T ~ 0)) %>% 
    mutate(q4 = case_when(is.na(attributes.question4response) ~ 0,
                          attributes.question4response %in% c('1','2','3','4','5') ~ 1,
                          T ~ 0)) %>% 
    mutate(q5 = case_when(is.na(attributes.question5response) ~ 0,
                          attributes.question5response %in% c('1','2','3','4','5') ~ 1,
                          T ~ 0)) %>% 
    mutate(pipe.flag_survey = case_when(q1+q2+q3+q4+q5 > 0 ~ 1, T ~ 0)) %>% 
    
    identity()  
  
  return(df_calls_output)
}

# JOIN the reference datasets
fn_CALL_ReferenceData <- function(df_input, google = TRUE) {
  # source('R_INCLUDE_References.R')
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # take a copy to manipulate
  df_output <- df_input
  rows_in <- nrow(df_input)
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # okta_id is always unique
  df_ref_okta <- fn_REF_get('okta')
  df_output <- df_output %>% 
    left_join(df_ref_okta, by = c('agent.username' = 'okta_id'))
  
  if (nrow(df_input) != nrow(df_output)) stop('join creates records')
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # member_id is always unique
  df_ref_mbr <- fn_REF_get('mbr')
  df_output <- df_output %>% 
    left_join(df_ref_mbr %>% select(member_aws, ref.lss.fullname, ref.lss.shortname), by = c('attributes.formember' = 'member_aws')) 
  if (nrow(df_input) != nrow(df_output)) stop('join creates records')

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # phone no to service mapping, date dependent
  df_ref_phonenos <- fn_REF_get('phonenos')
  df_ref_phonenos <- fn_REF_expand('phonenos', df_ref_phonenos)
  
  df_output <- df_output %>% 
    left_join(df_ref_phonenos, by = c('systemendpoint.address' = 'ref.phone', 
                                      'pipe.when_date' = 'ref.phone.date')) 
  
  # check 
  rows_out <- nrow(df_output)
  if (rows_in != rows_out) {
    print(paste0(' ... input rows ',rows_in,' output ',rows_out))
    stop('join creates records')
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # transfer calls
  # df_output <- df_output %>% 
  #   left_join(df_ref_phonenos %>% 
  #               select(ref.transferno = ref.phone, ref.transfer.service = ref.phone.service), 
  #             by = c('transferredtoendpoint.address' = 'ref.transferno'))
  # if (nrow(df_input) != nrow(df_output)) stop('join creates records')
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_ref_queue <- fn_REF_get('queue')
  df_output <- df_output %>% 
    left_join(df_ref_queue %>% select(ref.queue, ref.queue.service, ref.queue.description), by = c('queue.name' = 'ref.queue'))
  if (nrow(df_input) != nrow(df_output)) stop('join creates records')
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_ref_algroups <- fn_REF_get('algroup')
  df_output <- df_output %>% 
    left_join(df_ref_algroups %>% select(member_aws, ref.formember.advicegroup = member_group), by = c('attributes.formember' = 'member_aws')) %>% 
    left_join(df_ref_algroups %>% select(member_aws, ref.okta.advicegroup = member_group), by = c('ref.okta.member' = 'member_aws')) 
  if (nrow(df_input) != nrow(df_output)) stop('join creates records')
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_ref_single <- fn_REF_get('single')
  df_output <- df_output %>% 
    left_join(df_ref_single %>% select(single_queue, ref.single_group = single_group), by = c('queue.name' = 'single_queue')) 
  if (nrow(df_input) != nrow(df_output)) stop('join creates records')
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_ref_alink <- fn_REF_get('alink')
  df_output <- df_output %>% 
    left_join(df_ref_alink, by = c('systemendpoint.address' = 'ref.alink.phone'))
  if (nrow(df_input) != nrow(df_output)) stop('join creates records')
  
  return(df_output)
  
}

# CREATE variables which define service filters from either called number or queue
fn_CALL_dataset_defs <- function(df_input) {
  
  print(' ... Creating dataset filters')
  
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
    mutate(pipe.dataset_dro = case_when(ref.phone.service == 'Durham DRO' | ref.queue.service == 'Durham DRO' ~ 1, T ~ 0)) %>% 
    mutate(pipe.dataset_fix = case_when(ref.phone.service == 'unclaimed' | ref.queue.service == 'unclaimed' ~ 1, T ~ 0)) %>% 
    identity()
  
  return (df_output)
}

# REFORMAT the final data into a human-friendly order
fn_reorder <- function(df_input) {
  
  print(' ... Reordering variables for human readability')
  
  col_order <- c("pipe.ctr_setid","pipe.call_type",
                 "pipe.ctr_orig", "pipe.ctr_junk","pipe.ctr_type",
                 "pipe.inout_first","pipe.inout_final",
                 "systemendpoint.address","ref.phone.service","ref.alink.service",
                 "customerendpoint.address",
                 "transferredtoendpoint.address", "ref.transfer.service",
                 "queue.name","ref.queue.service","ref.queue.description", 
                 "queue.duration",
                 "pipe.queue.hops", "pipe.queue.duration", "pipe.queue.total", "pipe.talk.total", "pipe.time.total",
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
                 "pipe.flag_weekday",
                 "pipe.flag_queued","pipe.flag_queuednot","pipe.flag_answer",
                 "pipe.flag_inbound","pipe.flag_outbound","pipe.flag_other",
                 "pipe.flag_answerin20", "pipe.flag_answerin30", "pipe.flag_answerin60",
                 "pipe.flag_calllonger30",
                 "pipe.flag_queued_abandoned","pipe.flag_queued_abandoned10",
                 "pipe.flag_talk_abandoned5",
                 "pipe.flag_survey",
                 
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
                 "pipe.dataset_dro",
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
  
  missing <- df_input %>% colnames() %>% setdiff(col_order, .)
  
  if (length(missing) > 0) {
    print(paste0(' ... Variables not in dataframe are ', paste0(missing, collapse = ', ')))
    return('')
  } else {
    df_output <- df_input %>% select(all_of(col_order))
    return(df_output)
  }
}

# RETURN reference datasets
fn_REF_get <- function(what, show = FALSE) {
  
  what <- tolower(what)
  valid_whats <- c('okta','mbr','phonenos','queue','algroup','single','alink')
  
  if (!(what %in% valid_whats)) {
    print(paste0(' ... ',what,' is not in valid arguements of ',paste0(valid_whats, collapse = ',')))
  }
  
  # pick up from the parquet files in the source folder
  url_list <- fn_ref_url()
  ref_dir <- url_list$url_path
  ref_title <- url_list$url_title
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'phonenos') {
    ref_tab <- 'reference_phonenumbers'
    ref_file <- paste0(ref_dir,ref_tab,'.parquet')
    
    # any null dates need to be set to today so they will join 
    # with call dates
    df_ref_phonenos <- read_parquet(ref_file, col_types = c(.default = 'c')) %>% 
      mutate(ref.phone.finish = coalesce(ref.phone.finish, as.character(Sys.Date()))) %>% 
      select(ref.phone, ref.phone.service, ref.phone.start, ref.phone.finish)
    
    ref.check <- df_ref_phonenos %>% 
      arrange(ref.phone, ref.phone.start) %>% 
      group_by(ref.phone) %>% 
      mutate(prev.finish = lag(ref.phone.finish)) %>% 
      mutate(overlap = ref.phone.start <= prev.finish) %>% 
      ungroup() %>% 
      filter(overlap == TRUE) %>% 
      tally()
    
    # check phonenumbers are unique
    if (ref.check > 0) {
      print(' ... ERROR : Phone Service Duplicates or overlapping dates !')
      return('')
    } 
    
    print(' ... Phone Service data passed - no duplicates or overlaps')
    if (show) glimpse(df_ref_phonenos)
    
    return(df_ref_phonenos)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'queue') {
    
    ref_tab <- 'reference_queues'
    ref_file <- paste0(ref_dir,ref_tab,'.parquet')
    df_ref_queue <- read_parquet(ref_file, col_types = c(.default = 'c'))
    
    if (df_ref_queue %>% count(ref.queue) %>% filter(n > 1) %>% tally() > 0) {
      print(' ... Queue Duplicates !')
      df_ref_mbr %>% add_count(ref.queue) %>% filter(n > 1)
      return('')
    } 
    
    print(' ... Queue data passed')
    if (show) glimpse(df_ref_queue)
    
    return(df_ref_queue)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  if (what == 'okta') {
    
    ref_tab <- 'reference_oktaadvisers'
    ref_file <- paste0(ref_dir,ref_tab,'.parquet')
    df_ref_okta <- read_parquet(ref_file, col_types = c(.default = 'c'))
    
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
    
    ref_tab <- 'reference_mbr'
    ref_file <- paste0(ref_dir,ref_tab,'.parquet')
    df_ref_mbr <- read_parquet(ref_file, col_types = c(.default = 'c')) %>% 
      filter(!is.na(member_aws))
    
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
  if (what == 'algroup') {
    
    ref_tab <- 'reference_adviceline_groups'
    ref_file <- paste0(ref_dir,ref_tab,'.parquet')
    df_ref_algroups <- read_parquet(ref_file, col_types = c(.default = 'c'))
    
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
    
    ref_tab <- 'reference_single_queue'
    ref_file <- paste0(ref_dir,ref_tab,'.parquet')
    df_ref_single <- read_parquet(ref_file, col_types = c(.default = 'c'))
    
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
    
    ref_tab <- 'reference_advicelink_services'
    ref_file <- paste0(ref_dir,ref_tab,'.parquet')
    df_ref_alink <- read_parquet(ref_file, col_types = c(.default = 'c'))
    
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

# expand start/finish date to get around R not having a 'between' join 
fn_REF_expand <- function(what, df_ref_in) {
  
  what <- tolower(what)
  valid_whats <- c('okta','mbr','phonenos','queue','algroup','single','alink')

  if (!(what %in% valid_whats)) {
    print(paste0(' ... ',what,' is not in valid arguements of ',paste0(valid_whats, collapse = ',')))
  }

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if (what == 'phonenos') {
    print(' ... expanding dates')
    for (i in seq(1, nrow(df_ref_in))) {
      ref.phone <- df_ref_in[i, c('ref.phone')] %>% as.character()
      ref.phone.service <- df_ref_in[i, c('ref.phone.service')] %>% as.character()
      ref.phone.start <- df_ref_in[i, c('ref.phone.start')] %>% as.character()
      ref.phone.finish <- df_ref_in[i, c('ref.phone.finish')] %>% as.character()

      ref.seq <- as_tibble(seq(as.Date(ref.phone.start), as.Date(ref.phone.finish), by = 1)) %>%
        rename(ref.phone.date = value) %>%
        mutate(ref.phone = ref.phone,
               ref.phone.service = ref.phone.service,
               ref.phone.start = ref.phone.start,
               ref.phone.finish = ref.phone.finish) %>%
        select(ref.phone.date, ref.phone, ref.phone.service) %>%
        identity()

      if (i == 1) {
        df_ref_out = ref.seq
      } else {
        df_ref_out <- bind_rows(df_ref_out, ref.seq)
      }
    }
    return(df_ref_out)
  }

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

}

# REFORMAT remove hashtags etc from strings
fn_STRING_fixes <- function(text_in) {
  
  #print(text_in)
  text_out <- text_in
  text_out <- str_remove_all(text_out,"<speak>")
  text_out <- str_remove_all(text_out,"\n")
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

# where reference data is sourced on G:
fn_ref_url <- function() {
  
  url_path <- 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Reference Tables/'
  url_title <- 'Reference Data - Calls dataset - 2023-05-24 onwards'
  url_source <- 'https://docs.google.com/spreadsheets/d/1jmzZXW3gKeG_e3l_x2ECMQ1sE84AeSj8UiAlknU5-us/edit#gid=1152636255'

  url_brian_title <- 'Reporting Reference Data Updates - running workbook'
  url_brian <- 'https://docs.google.com/spreadsheets/d/1FEcmgQmYk_Dmf9IgjM1CdYjSFTMQzHB2DyiMpzpbCas/edit#gid=1617783057'
  
  ret_list <- list(url_path = url_path,
                   url_title = url_title,
                   url_source = url_source,
                   url_brian_title = url_brian_title,
                   url_brian = url_brian)
  
  return(ret_list)
}

# check a string is a valid date
checkdate = function(mydate, nullOK = TRUE) {
  
  if (nullOK == TRUE & is.na(mydate)) return(TRUE)
  
  y <- as.integer(str_sub(mydate, 1, 4))
  m <- as.integer(str_sub(mydate, 6, 7))
  d <- as.integer(str_sub(mydate, 9, 10))
  
  #Convert to an R Date object.
  #If the date is not valid, NA is returned.
  dt = as.Date(paste(y, m, d, sep='-'), optional=TRUE)
  
  ifelse(is.na(dt), FALSE, TRUE)
}

# check brians reference sheet before adding it to the aws sheet
fn_check_refdata <- function(what) {
  
  what <- tolower(what)
  valid_whats <- c('reference_phonenumbers','reference_queues')
  
  if (!(what %in% valid_whats)) {
    print(paste0(' ... ',what,' is not in valid arguements of ',paste0(valid_whats, collapse = ',')))
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if (what == 'reference_phonenumbers') {
    
    sheet_ok <- 'OK'
    sheet <- what
    cols_in <- c('added by','date added','date removed','service','phone number','description')
    df_in <- read_sheet(url_brian, sheet, col_types = c(.default = 'c')) %>%
      rename_with(., tolower) %>%
      select(any_of(cols_in)) %>%
      identity()
    
    # check everything is there
    if (length(cols_in) != ncol(df_in)) {
      print(paste0(' ... column mismatch for ',sheet))
      sheet_ok <- 'Some columns are missing from brians sheet'
    }
    
    # reformat for output
    cols_out <- c('ref.phone.author', 'ref.phone.start', 'ref.phone.finish', 'ref.phone.service', 'ref.phone', 'ref.phone.description')
    df_out <- df_in %>%
      rename(ref.phone.author = `added by`,
             ref.phone.start = `date added`,
             ref.phone.finish = `date removed`,
             ref.phone.service = service,
             ref.phone = `phone number`,
             ref.phone.description = description) %>%
      select(all_of(cols_out))
    
    # checks
    # check 1. ref.phone.start is a valid date
    print(' .. Checking Start date')
    for (mydate in unique(df_out$ref.phone.start)) {
      if (checkdate(mydate, nullOK = FALSE) == FALSE) {
        print(paste0(' ... Invalid start date in ',sheet,' is ',mydate))
        sheet_ok <- paste0('Invalid start date ', mydate)
      }
    }
    
    # check 2. ref.phone.finish is a valid date or NULL
    print(' .. Checking Finish date')
    for (mydate in unique(df_out$ref.phone.finish)) {
      if (checkdate(mydate) == FALSE) {
        print(paste0(' ... Invalid finish date in ',sheet,' is ',mydate))
        sheet_ok <- paste0('Invalid finish date ', mydate)
      }
    }
    
    # check 3. check key={phone number, date range} is unique
    check_unique <- df_out %>%
      select(ref.phone, ref.phone.start, ref.phone.finish) %>%
      add_count(ref.phone) %>%
      filter(n > 1) %>%
      arrange(ref.phone, ref.phone.start) %>%
      group_by(ref.phone) %>%
      mutate(previous.finish = lag(ref.phone.finish)) %>%
      mutate(error = ref.phone.start <= previous.finish) %>%
      filter(error == TRUE) %>%
      ungroup() %>%
      identity()
    
    if (nrow(check_unique) > 0) {
      print(' ... not all phone numbers are unique within date range')
      sheet_ok <- ('not all phone numbers are unique within date range')
    }
    
    ret_list <- list(ok = sheet_ok, df_ref = df_out)
    return(ret_list)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if (what == 'reference_queues') {
    
    sheet_ok <- 'OK'
    sheet <- what
    cols_in <- c('added by','date added','date removed','service','queue','description')
    df_in <- read_sheet(url_brian, sheet, col_types = c(.default = 'c')) %>%
      rename_with(., tolower) %>%
      select(any_of(cols_in)) %>%
      identity()
    
    # check everything is there
    if (length(cols_in) != ncol(df_in)) {
      print(paste0(' ... column mismatch for ',sheet))
      sheet_ok <- 'Some columns are missing from brians sheet'
    }
    
    # reformat for output
    cols_out <- c('ref.queue.author', 'ref.queue.start', 'ref.queue.finish', 'ref.queue.service', 'ref.queue', 'ref.queue.description')
    df_out <- df_in %>%
      rename(ref.queue.author = `added by`,
             ref.queue.start = `date added`,
             ref.queue.finish = `date removed`,
             ref.queue.service = service,
             ref.queue = `queue`,
             ref.queue.description = description) %>%
      select(all_of(cols_out))
    
    # checks
    # check 1. ref.phone.start is a valid date
    print(' .. Checking Start date')
    for (mydate in unique(df_out$ref.queue.start)) {
      if (checkdate(mydate, nullOK = FALSE) == FALSE) {
        print(paste0(' ... Invalid start date in ',sheet,' is ',mydate))
        sheet_ok <- paste0('Invalid start date ', mydate)
      }
    }
    
    # check 2. ref.phone.finish is a valid date or NULL
    print(' .. Checking Finish date')
    for (mydate in unique(df_out$ref.queue.finish)) {
      if (checkdate(mydate) == FALSE) {
        print(paste0(' ... Invalid finish date in ',sheet,' is ',mydate))
        sheet_ok <- paste0('Invalid finish date ', mydate)
      }
    }
    
    # check 3. check key={queue, date range} is unique
    check_unique <- df_out %>%
      select(ref.queue, ref.queue.start, ref.queue.finish) %>%
      add_count(ref.queue) %>%
      filter(n > 1) %>%
      arrange(ref.queue, ref.queue.start) %>%
      group_by(ref.queue) %>%
      mutate(previous.finish = lag(ref.queue.finish)) %>%
      mutate(error = ref.queue.start <= previous.finish) %>%
      filter(error == TRUE) %>%
      ungroup() %>%
      identity()
    
    if (nrow(check_unique) > 0) {
      print(' ... not all phone numbers are unique within date range')
      sheet_ok <- ('not all phone numbers are unique within date range')
    }
    
    ret_list <- list(ok = sheet_ok, df_ref = df_out)
    return(ret_list)
    
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if (what == 'reference_queues') {
    sheet_ok <- 'OK'
    sheet <- what
    
    ret_list <- list(ok = sheet_ok, df_ref = df_out)
    return(ret_list)
  }
  
}

# --------------------------------------------------------------------------------
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# OLD STUFF
# remove spurious DISCONNECT and TRANSFER records to leave a clean CTR based dataset, then
# RETURN a list of three dataframes, one for multi CTR calls, one for single CTR calls, one for junked records
# NOTE : All DISCONNECT records are artefacts, same TRANSFER records for OUTBOUND
#        but TRANSFER records for INBOUND seem valid
# fn_CTR_data_clean_pass1 <- function(df_input) {
#   
#   # ~~~~~~~~~~~~~~~~~~
#   # first reduce the recordset by throwing away redundant records
#   df_ctr_basic <- df_input %>% 
#     # make a ctr_setid, and a variable to store the junk condition
#     mutate(pipe.ctr_setid = case_when(is.na(initialcontactid) ~ contactid, T ~ initialcontactid), .before = 1) %>%
#     # make dummy variables
#     mutate(pipe.ctr_orig = 0, .after = 'pipe.ctr_setid') %>% 
#     mutate(pipe.ctr_junk = 0, .after = 'pipe.ctr_orig') %>% 
#     mutate(junk = 'NO', .after = 'pipe.ctr_junk') %>%
#     mutate(junk = case_when(initiationmethod == 'DISCONNECT' ~ 'DISCONNECT CTR', T ~ junk)) %>% 
#     mutate(leg_count = 0, .after = 'junk') %>% 
#     mutate(leg_id = 0, .after = 'leg_count') %>% 
#     mutate(dup_check = paste0(initiationmethod,':',initiationtimestamp,':',disconnectreason,':',disconnecttimestamp), .after = 'pipe.ctr_junk') %>% 
#     mutate(pipe.queue.hops = NA, .after = 'pipe.ctr_junk') %>% 
#     mutate(pipe.queue.duration = NA, .after = 'pipe.queue.hops') %>% 
#     mutate(pipe.queue.total = NA, .after = 'pipe.queue.duration') %>% 
#     # collect some original audit stats
#     group_by(pipe.ctr_setid) %>% 
#     # original numbers of ctrs
#     mutate(pipe.ctr_orig = n()) %>% 
#     # number of ctrs junked
#     mutate(pipe.ctr_junk = sum(!junk == 'NO')) %>% 
#     ungroup() %>% 
#     identity()
#   
#   # split off the junk records
#   df_ctr_junk <- df_ctr_basic %>% filter(!junk == 'NO')
#   df_ctr <- df_ctr_basic %>% filter(junk == 'NO')
#   
#   # ~~~~~~~~~~~~~~~~~~
#   df_ctr_useful <- df_ctr %>% 
#     # find the number of CTR legs in the call
#     group_by(pipe.ctr_setid) %>%
#     mutate(leg_count = n()) %>%
#     mutate(leg_id = row_number()) %>%
#     
#     # find the call type
#     mutate(pipe.call_type = case_when(any(initiationmethod == 'INBOUND') ~ 'INBOUND', 
#                                       any(initiationmethod == 'OUTBOUND') ~ 'OUTBOUND',
#                                       T ~ 'OTHER'), .after = 1) %>% 
#     
#     ungroup()
#   
#   # ~~~~~~~~~~~~~~~~~~
#   # then split into single leg calls and multi leg calls because
#   # single leg calls are fine as they are
#   df_ctr_single <- df_ctr_useful %>% filter(leg_count == 1)
#   df_ctr_multiple <- df_ctr_useful %>% filter(leg_count > 1)
#   
#   # ~~~~~~~~~~~~~~~~~~
#   # Now evaluate the multi CTR dataset for records to junk
#   df_ctr_eval <- df_ctr_multiple %>% 
#     
#     group_by(pipe.ctr_setid) %>% 
#     arrange(pipe.ctr_setid, initiationtimestamp) %>% 
#     
#     # check for randomly created duplicates
#     mutate(junk = case_when(dup_check == lag(dup_check) ~ 'DUPLICATE OF PREVIOUS CTR', 
#                             T ~ junk)) %>% 
#     
#     # for outbound calls, throw away any legs greater than 1 with an inititationmethod of 'OUTBOUND'
#     # these seem to be duplicates on every occurance that I have checked
#     mutate(junk = case_when(row_number() > 1 & initiationmethod == 'OUTBOUND' ~ 'OUTBOUND NOT POSITION 1 IN CTR SET', 
#                             T ~ junk)) %>% 
#     
#     # for inbound calls, throw away any records with an initiationmethod of 'TRANSFER'
#     # and where the queue.name is blank AND the agent.username is blank, these seem to be system artefacts
#     mutate(junk = case_when(pipe.call_type == 'INBOUND' & 
#                               initiationmethod == 'TRANSFER' & 
#                               is.na(agent.username) & 
#                               is.na(queue.name) ~ 'INBOUND BLANK TRANSFERS', 
#                             T ~ junk)) %>% 
#     
#     # for outbound calls, throw away any records with an initiationmethod of 'TRANSFER'
#     # and where the agent.username is blank, these seem to be system artefacts
#     mutate(junk = case_when(pipe.call_type == 'OUTBOUND' & 
#                               initiationmethod == 'TRANSFER' & is.na(agent.username) ~ 'OUTBOUND BLANK TRANSFERS', 
#                             T ~ junk)) %>% 
#     
#     # for outbound calls, throw away any records with an initiationmethod of 'TRANSFER'
#     # and disconnectreason of 'CONTACT_FLOW_DISCONNECT' or 'CUSTOMER_DISCONNECT', these seem to be system artefacts
#     mutate(junk = case_when(pipe.call_type == 'OUTBOUND' &
#                               initiationmethod == 'TRANSFER' &
#                               disconnectreason %in% c('CONTACT_FLOW_DISCONNECT','CUSTOMER_DISCONNECT') ~ 'INBOUND TRANSFER TO CONTACT_FLOW_DISCONNECT',
#                             T ~ junk)) %>%
#     
#     # update the ctr_junk count
#     mutate(pipe.ctr_junk = pipe.ctr_junk + sum(!junk == 'NO')) %>% 
#     ungroup() 
#   
#   
#   # ~~~~~~~~~~~~~~~~~~
#   # add new junk records from the multiple CTR Call sets
#   df_ctr_junk <- df_ctr_junk %>% 
#     bind_rows(df_ctr_eval %>% filter(!junk == 'NO'))
#   
#   # these are ok, but now some will be single CTR sets
#   df_ctr_ok <- df_ctr_eval %>% filter(junk == 'NO')
#   
#   df_ctr_clean_single <- df_ctr_ok %>% 
#     group_by(pipe.ctr_setid) %>% 
#     filter(n() == 1) %>% 
#     ungroup()
#   
#   df_ctr_clean_multiple <- df_ctr_ok %>% 
#     group_by(pipe.ctr_setid) %>% 
#     filter(n() > 1) %>% 
#     ungroup()
#   
#   # reevaluate the datasets
#   df_ctr_single <- df_ctr_single %>% 
#     bind_rows(df_ctr_clean_single) %>% 
#     group_by(pipe.ctr_setid) %>%
#     mutate(leg_count = n()) %>%
#     mutate(leg_id = row_number()) %>%
#     ungroup()
#   
#   df_ctr_multiple <- df_ctr_clean_multiple %>% 
#     group_by(pipe.ctr_setid) %>%
#     mutate(leg_count = n()) %>%
#     mutate(leg_id = row_number()) %>%
#     ungroup()
#   
#   # ~~~~~~~~~~~~~~~~~~
#   df_ctr_junk <- df_ctr_junk %>% 
#     group_by(pipe.ctr_setid) %>% 
#     arrange(pipe.ctr_setid, initiationtimestamp) %>% 
#     mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
#                                      row_number() == 1 ~ 'first',
#                                      row_number() == n() ~ 'final',
#                                      T ~ 'middle'), .after = 7) %>%
#     ungroup()
#   
#   df_ctr_single <- df_ctr_single %>% 
#     group_by(pipe.ctr_setid) %>% 
#     arrange(pipe.ctr_setid, initiationtimestamp) %>% 
#     mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
#                                      row_number() == 1 ~ 'first',
#                                      row_number() == n() ~ 'final',
#                                      T ~ 'middle'), .after = 7) %>%
#     ungroup()
#   
#   df_ctr_multiple <- df_ctr_multiple %>% 
#     group_by(pipe.ctr_setid) %>% 
#     arrange(pipe.ctr_setid, initiationtimestamp) %>% 
#     mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
#                                      row_number() == 1 ~ 'first',
#                                      row_number() == n() ~ 'final',
#                                      T ~ 'middle'), .after = 7) %>%
#     ungroup()
#   
#   # ~~~~~~~~~~~~~~~~~~
#   
#   df_list <- list(ctr_single = df_ctr_single, 
#                   ctr_multi = df_ctr_multiple, 
#                   ctr_junk = df_ctr_junk)
#   
#   return(df_list)
#   
# }
# 
# # COPE with any multi-CTR calls records which have a genuine TRANSFER
# # make the TRANSFER into a call of it's own 
# fn_CTR_data_clean_pass2 <- function(df_ctr_single, df_ctr_multiple) {
#   
#   # remove TRANSFER records from the Call CTR set
#   # the original Call set can be related from the initial or previous contactid
#   df_ctr <- df_ctr_multiple %>% 
#     mutate(pipe.ctr_setid = case_when(initiationmethod == 'TRANSFER' ~ contactid, T ~ pipe.ctr_setid)) %>% 
#     group_by(pipe.ctr_setid) %>% 
#     arrange(pipe.ctr_setid, initiationtimestamp) %>% 
#     mutate(pipe.ctr_type = case_when(n() == 1 ~ 'only',
#                                      row_number() == 1 ~ 'first',
#                                      row_number() == n() ~ 'final',
#                                      T ~ 'middle'), .after = 7) %>%
#     ungroup()
#   
#   # add single records to the single dataset
#   df_ctr_2_single <- df_ctr %>% filter(pipe.ctr_type == 'only')
#   df_ctr_single <- df_ctr_single %>% bind_rows(df_ctr_2_single)
#   
#   # keep multiple ctr call sets
#   df_ctr_2_multi <- df_ctr %>% filter(pipe.ctr_type != 'only')
#   
#   # return
#   df_list <- list(ctr_single = df_ctr_single,
#                   ctr_multi = df_ctr_2_multi)
#   
#   return(df_list)
# }
# 
# # COLLAPSE the multi-CTR dataframe into a single Call record
# fn_CTR_data_clean_pass3 <- function(df_ctr_single, df_ctr_multi) {
#   
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # for single ctr calls queue info is just the same as the raw data
#   df_ctr_single <- df_ctr_single %>% 
#     mutate(pipe.queue.hops = queue.name) %>% 
#     mutate(pipe.queue.duration = queue.duration) %>% 
#     mutate(pipe.queue.total = as.integer(queue.duration))
#   
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # for multi-ctr call datastes we need to collapse across all the ctrs
#   
#   # get queue info from across the whole record set
#   df_ctr_multi <- df_ctr_multi %>% 
#     group_by(pipe.ctr_setid) %>% 
#     mutate(pipe.queue.hops = paste(queue.name, collapse = ', ')) %>% 
#     mutate(pipe.queue.duration = paste(queue.duration, collapse = ', ')) %>% 
#     mutate(pipe.queue.total = sum(as.integer(queue.duration))) %>% 
#     ungroup() 
#   
#   # then get info from first ctr
#   df_first <- df_ctr_multi %>% 
#     filter(pipe.ctr_type == 'first') %>% 
#     select(pipe.ctr_setid:customerendpoint.address) %>% 
#     mutate(pipe.ctr_type = 'multiple') %>%
#     identity()
#   
#   df_final <- df_ctr_multi %>% 
#     filter(pipe.ctr_type == 'final') %>% 
#     select(pipe.ctr_setid, transferredtoendpoint.address:last_col()) %>% 
#     identity()
#   
#   # create a call record from the relevant bits of first, middle, final ctrs
#   df_call <- df_first %>% 
#     left_join(df_final, by = 'pipe.ctr_setid') %>% 
#     bind_rows(df_ctr_single)
#   
#   return(df_call)
# }
# 
