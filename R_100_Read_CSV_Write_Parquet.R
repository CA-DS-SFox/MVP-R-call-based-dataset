library(tidyverse)
library(arrow)
library(here)

# -------------------------------------------------------------------------

file_to_convert <- here('data','CTR-april.csv')
file_to_create <- here('data','CTR-april.parquet')
  
df_in <- read_csv(file_to_convert, col_types = cols(.default = 'c'))
write_parquet(df_in, file_to_create)

# if the process is successful ...
# file.remove(file_to_convert)