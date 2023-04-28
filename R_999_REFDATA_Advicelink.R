

x <- tibble::tribble(
 ~ref.alink.phone, ~ref.alink.service, ~remove,
 "+448082505700", "Advicelink CWY", 0L,
 "+442038302742", "Advicelink Claim what is yours", 0L,
 "+448081898190", "Advicelink Claim what is yours", 0L,
 "+442038302731", "Advicelink Private Rented Sector", 0L,
 "+443003302177", "Advicelink Private Rented Sector", 0L,
 "+448081898191", "Advicelink Private Rented Sector", 0L,
 "+448082787920", "Advicelink Private Rented Sector", 0L,
 "+443003309066", "Adviser Immigration Helpline", 0L,
 "+448007022020", "Advicelink Remote Service", 0L,
 "+442038302691", "Advicelink Remote Service", 0L,
 "+443444772020", "Advicelink Remote Service", 0L,
 "+448081898151", "Advicelink Remote Service", 0L,
 "+448082505720", "Advicelink Remote Service", 0L,
 "+448444772020", "Advicelink Remote Service", 0L,

 "+448082787921", "Advicelink Employment and Discrimination", 0L,
 "+443003302165", "Advicelink Employment and Discrimination", 0L,
 "+442038302680", "Advicelink Employment and Discrimination", 0L,
 "+448081898169", "Advicelink Employment and Discrimination", 0L,
 
 "+443003301178", "Advicelink Local 55/0004- Wrexham", 0L,
 "+443003301192", "Advicelink Local 55/0008- Ynys Mon", 0L,
 "+443003302117", "Advicelink Local 60/0017- Gwent", 0L,
 "+443003302118", "Advicelink Local 55/0014- Flintshire", 0L,
 "+443003302119", "Advicelink Local 60/0052- Cardiff", 0L,
 "+443003302121", "Advicelink Local 60/0009- Merthyr", 0L,
 "+443003302122", "Advicelink Local 60/0034- Ceredigion", 0L,
 "+443003302124", "Advicelink Local 55/0011- Denbighshire", 0L,
 "+443003302167", "Advicelink Local 55/0007- Conwy", 0L,
 "+443003309082", "Advicelink Local 60/0041- Swansea", 0L,
 "+443454503064", "Advicelink Local 55/0016- Gwynedd", 0L,
 "+448082505711", "Advicelink Local 60/0034- Ceredigion", 0L,
 "+448082787922", "Advicelink Local 55/0016- Gwynedd", 0L,
 "+448082787923", "Advicelink Local 55/0014- Flintshire", 0L,
 "+448082787925", "Advicelink Local 60/0052- Cardiff", 0L,
 "+448082787926", "Advicelink Local 60/0041- Swansea", 0L,
 "+448082787931", "Advicelink Local 55/0004- Wrexham", 0L,
 "+448082787932", "Advicelink Local 55/0008- Ynys Mon", 0L,
 "+448082787933", "Advicelink Local 55/0011- Denbighshire", 0L,
 "+448082787934", "Advicelink Local 60/0009- Merthyr", 0L,
 "+448082787935", "Advicelink Local 60/0017- Gwent", 0L,
 "+442038302724", "Advicelink Local", 0L,
 "+442038302734", "Advicelink Local", 0L,
 "+448081898171", "Advicelink Local", 0L,
 "+448081898184", "Advicelink Local", 0L,
 "+448454503064", "Advicelink Local", 0L
)

x <- x %>% 
  filter(remove == 0) %>% 
  select(ref.alink.phone, ref.alink.service) 

# check
x %>% 
  add_count(ref.alink.phone) %>% 
  filter(n > 1)

# save
mypath <- 'G:/Shared drives/CA - Interim Connect Report Log Files & Guidance/Interim Reports/Reference Tables/'
myfile <- 'reference_advicelink_services.parquet'
write_parquet(x, paste0(mypath, myfile))

file.info(path = list.files(mypath, full.names = TRUE)) %>% 
  rownames_to_column(var = 'filename') %>% 
  mutate(filename = str_remove(filename, mypath))
  
              