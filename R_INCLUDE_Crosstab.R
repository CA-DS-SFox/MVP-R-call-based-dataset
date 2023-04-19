
# -------------------------------------------------------------------------
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# generates each column of the crosstab
# assumes df_tab is an input dataframe of 2 columns called sidevar, headvar
# if col_item == 'Make_Total' then generate a total column
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
generate_xtab_column <- function(df_tab, col_item = '', options=c(), na_text = na_text) {
  
  text_total <- 'TOTAL'
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # total column for sidevar or headvar to form the framework of
  # the table output. everything s joined to this, so 
  # order and so on goes here
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if (tolower(col_item) == 'make_total') {
    line_tot <- df_tab %>% mutate(sidevar = text_total) %>% count(sidevar)
    line_all <- df_tab %>% filter(!is.na(sidevar)) %>% count(sidevar)
    line_nas <- df_tab %>% filter(is.na(sidevar)) %>% count(sidevar) %>% mutate(sidevar = na_text)
    
    # assumes at this point the sidevar can have any variable type
    # need to sort first then change to character
    # so that if sidevar is integer the sorting is correct
    if ('sortside_n' %in% options) line_all <- line_all %>% arrange(desc(n))
    line_all <- line_all %>% mutate(sidevar = as.character(sidevar))
    
    col_stub <- bind_rows(line_tot, line_all, line_nas) %>% rename(TOTAL = n)
    return(col_stub)
  }
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # this will generate a column of occurance counts for the
  # sidevar for a specific item in the headvar denoted by col_item
  # assumes at this point df_tab will be all character
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df_tab <- df_tab %>% filter(headvar == col_item)
  
  # for each element of the sidevar
  col_items <- bind_rows(
    # generate a total
    df_tab %>% 
      summarise(n = n()) %>% 
      mutate(sidevar = text_total) %>% 
      select(sidevar, n),
    
    # totals for each side item
    df_tab %>% 
      group_by(sidevar) %>% 
      summarise(n = n()) %>% 
      ungroup() 
  )
  
  return(col_items)
}

# -------------------------------------------------------------------------

# valid options are sortside_n, sorthead_n
xtab <- function(.data, side, header, options = c(), na_text = 'N/A') {
  
  df <- .data
  
  # STEP 0 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # get the two variables involved in the cross tabulation
  # so the code is generic make a dataframe with just those
  # two variables, and call them sidevar and headvar
  # rename these back to the original values before 
  # returning the result
  
  df_tab <- df %>% 
    select(sidevar = (!!enquo(side)),
           headvar = (!!enquo(header)))
  
  # get string variables
  side_txt = deparse(substitute(side))
  head_txt = deparse(substitute(header))
  
  # get the type of variables
  df_types <- sapply(df_tab, class)
  
  # STEP 1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # create the stub/side/column 0 from the sidevar
  # it has a total as the first element
  # and if there are NAs those go at the end
  col0 <- generate_xtab_column(df_tab, 
                               col_item = 'Make_Total', 
                               options = options,
                               na_text = na_text) 
  
  # STEP 2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # now that column 0 knows about any NAs, and has done
  # the ordering appropriately for the variable type
  # sidevar and headvar can be character and NAs can 
  # be character with text as specified in na_text
  df_tab <- df_tab %>% 
    mutate(sidevar = as.character(sidevar)) %>% 
    mutate(headvar = as.character(headvar)) %>% 
    mutate(sidevar = coalesce(sidevar, na_text)) %>% 
    mutate(headvar = coalesce(headvar, na_text))  
  
  # STEP 3 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # create an output column for each occurance in the headvar
  head_items <- df_tab %>% count(headvar)
  if ('sorthead_n' %in% options) head_items <- head_items %>% arrange(desc(n))
  head_items <- head_items$headvar
  if (na_text %in% head_items) head_items <- c(head_items[!head_items %in% c(na_text)], na_text)
  
  for (item in head_items) {
    # make a column for each item
    colx <- generate_xtab_column(df_tab,
                                 col_item=item,
                                 options = options,
                                 na_text = na_text)
    
    out_name <- paste0(head_txt, '_', item)
    colx <- colx %>% rename(!!out_name := n)
    col0 <- col0 %>% full_join(colx, by = 'sidevar')
  }
  
  # STEP 4 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # rename sidevar to the original variable name
  # set any nas to zero
  col0 <- col0 %>% 
    rename(!!side_txt := sidevar) %>% 
    mutate(across(where(is.numeric), coalesce, 0)) %>% 
    identity()
  
  return(col0)
}

# -------------------------------------------------------------------------

hcount <- function(dataframe, elem_max = 20, elem_sort = TRUE, 
                   cols_include = c(), cols_exclude = c()) {
  
  # columns to process. Include is the defaults
  cols_analyse <- dataframe %>% colnames()
  if (length(cols_include) > 0) {
    cols_analyse <- intersect(cols_analyse, cols_include)
    print(glue(' ... Including {cols_analyse}'))
  } else if (length(cols_exclude) > 0) {
    cols_analyse <- setdiff(cols_analyse, cols_exclude)
  }
  
  for (i in seq_along(cols_analyse)) {
    
    varname = cols_analyse[i]
    
    # count occurances   
    df_count <- dataframe %>% 
      count(!!sym(varname), sort = elem_sort) %>% 
      setNames(c('dataset', 'total')) %>%  
      mutate(source = varname, .before = 1) %>%  
      mutate(info = 'element', .before = 2) %>%  
      identity 
    
    # get some information 
    elem_count <- nrow(df_count) 
    elem_type <- typeof(df_count$dataset) 
    unique_text <- paste0(', uniques : ', elem_count) 
    
    # truncate if there are too many elements 
    if (nrow(df_count) > elem_max) { 
      df_count <- df_count %>%  
        mutate(dataset = as.character(dataset)) %>%  
        slice(1:elem_max) 
      
      unique_text <- paste0(unique_text, ', max printed : ', elem_max) 
    }
    
    # make counts collapsable 
    df_count <- df_count %>%  
      mutate(dataset = as.character(dataset)) 
    
    # calculate %ages 
    df_count <- df_count %>%  
      mutate(pcent = paste0(round(total / nrow(dataframe) * 100), ' %')) 
    
    df_count <- bind_rows( 
      data.frame(source = varname,  
                 info = paste0('summary - datatype :',elem_type, unique_text), 
                 dataset = '', 
                 total = elem_count, 
                 pcent = ''), 
      df_count) 
    
    if (i == 1) { 
      df_counts <- df_count 
    } else { 
      df_counts <- bind_rows(df_counts, df_count) 
    } 
  } 
  
  return(df_counts)
}
