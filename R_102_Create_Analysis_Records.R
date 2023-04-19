
library(tidyverse)
library(arrow)
library(here)

source('R_INCLUDE_Functions.R')

# -------------------------------------------------------------------------
# Get Call and CTR data and apply the inclusion list

df_ctrs <- read_parquet(here('data','data-march-ctrs'))
df_calls <- read_parquet(here('data','data-march-calls'))

# -------------------------------------------------------------------------
# Add the transforms

t1 <- Sys.time()
df_analysis <- fn_CALL_to_ANALYSIS(df_calls)
t2 <- Sys.time()
print(paste0(' ... Time taken ', t2 - t1))

# -------------------------------------------------------------------------
# join the reference data
df_analysis <- df_calls %>% 
#  select('systemendpoint.address', 'agent.username','attributes.formember', 'queue.name') %>% 
  left_join(df_ref_phonenos, by = c('systemendpoint.address' = 'ref.phone')) %>% 
  left_join(df_ref_okta %>% rename(member_okta = member_aws), by = c('agent.username' = 'okta_id')) %>% 
  left_join(df_ref_mbr %>% select(member_aws, member_name, member_short), by = c('attributes.formember' = 'member_aws')) %>% 
  left_join(df_ref_queue %>% select(ref.queue, ref.queue.service, ref.queue.description), by = c('queue.name' = 'ref.queue')) %>% 
  left_join(df_ref_algroups %>% select(member_aws, member_group_called = member_group), by = c('attributes.formember' = 'member_aws')) %>% 
  left_join(df_ref_algroups %>% select(member_aws, member_group_answer = member_group), by = c('member_okta' = 'member_aws')) %>% 
  left_join(df_ref_single %>% select(single_queue, single_group), by = c('queue.name' = 'single_queue')) %>% 
  identity()


# -------------------------------------------------------------------------

df_ctrs %>% 
  filter(ctr_setid == '00005b92-b7fb-4824-8cb0-4b3136deda41') %>% 
  select(ctr_setid:attributes.outcome) %>% 
  t()

# 'ctr_setid','call_type','junk','dup_check','channel','leg_count','leg_id','ctr_type',
# 'initiationmethod','disconnectreason',
# 'systemendpoint.address','customerendpoint.address','transferredtoendpoint.address',
# 'queue.name','queue.hops','queue.total',
# 'agent.username',
# 'agent.routingprofile.name',
# 
# 'when_date','when_week','when_day','when_month','when_time','when_hour','when_minute','when_second',
# 'flag_weekday','flag_queued','flag_queuednot','flag_answer','flag_inbound','flag_outbound','flag_other',
# 'flag_answerin20','flag_calllonger30',
# 
# 'agent.hierarchygroups.level1.groupname','agent.hierarchygroups.level2.groupname','agent.hierarchygroups.level3.groupname','agent.hierarchygroups.level4.groupname','agent.hierarchygroups.level5.groupname',
# 'agent.numberofholds','agentconnectionattempts',
# 'attributes.servicename','attributes.finalservice','attributes.keypress','attributes.flowselection','attributes.outcome',
# 'attributes.formember',
# 'attributes.appname',
# 'attributes.numberofquestion',
# 'attributes.question1','attributes.question2','attributes.question3','attributes.question4','attributes.question5',
# 'attributes.question1response','attributes.question2response','attributes.question3response','attributes.question4response','attributes.question5response',
# 'attributes.nationalreason',
# 'tm_init','tm_conn','tm_quenq','tm_qudeq','tm_agcon','tm_tranf','tm_agwrs','tm_agwre','tm_disc','tm_updat',
# 'dur_init_conn','dur_init_que','dur_enq_deq','dur_deq_agnt','dur_conn','dur_call','dur_call_interact','dur_call_hold','dur_aft','dur_dis_upd','dur_total',
# 'oktaid',
# 'system_phone_number',
# 
# 'contactid','initialcontactid','nextcontactid','previouscontactid',
# 'date_call','initiationtimestamp',
# 'connectedtosystemtimestamp','agent.connectedtoagenttimestamp','queue.enqueuetimestamp','queue.dequeuetimestamp','transfercompletedtimestamp','agent.aftercontactworkstarttimestamp','agent.aftercontactworkendtimestamp','disconnecttimestamp','lastupdatetimestamp',
# 'queue.duration',
# 'agent.customerholdduration','agent.longestholdduration','agent.agentinteractionduration','agent.aftercontactworkduration',
# 
# )
# 
# X %>% 
#   colnames() %>% 
#   paste(., collapse = "','")
