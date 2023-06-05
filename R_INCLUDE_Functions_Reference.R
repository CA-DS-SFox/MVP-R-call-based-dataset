
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
