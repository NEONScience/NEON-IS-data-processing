### next steps filter out high values in any of the envelopes and run again
## check on percent difference 

library(magrittr)
library(dplyr)  

func_pat <- '/home/NEON/tburlingame/GitHub/NEON-IS-data-processing/flow/flow.pri.precip.aepg/'
  
source(paste0(func_pat, 'envelope_calc_func.R'))

files <- list.files('/scratch/pfs/aepg600m_fill_date_gaps_and_regularize', recursive = T, full.names = T, pattern = '[0-9].parquet')

# Regular expression to match CFGLOC##### pattern
pattern <- "CFGLOC\\d{6}"  

# Apply the function to each filename and store results in a vector
cfgs <- lapply(files, stringr::str_extract, pattern)

# Remove missing values (NA)
cfgs <- unlist(unique(cfgs))

envelope <- data.frame()
#break into chunks, pretty memory heavy. 
for (cfg in cfgs[16:25]){
  df <- envelope_calc_func(files, cfg)
  envelope <- rbind(envelope, df)
}

# readr::write_csv(envelope, file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope3.csv')
# 
# df1 <- readr::read_csv( file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope1.csv', col_names = T)
# df2 <- readr::read_csv(file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope2.csv', col_names = T)
# df3 <- readr::read_csv( file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope3.csv', col_names = T)
# 
# df <- rbind(df1, df2, df3)

readr::write_csv(envelope, file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope.csv')

envelope_df <-  readr::read_csv( file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope.csv', col_names = T)

envelope_df <- envelope_df
envelope_df$rnd_envelope <- round(envelope_df$envelope,2)
env_filt <- envelope_df %>% filter(envelope != -Inf)  %>%
                            filter(!is.na(site_id)) %>% 
                            filter(envelope < 250) %>%
                            filter( env_s1 <250)  %>% filter(env_s2 < 250) %>% filter(env_s3 < 250) %>% 
                            mutate(absdiff_g1g2 = abs(((env_s1-env_s2)/((env_s1 + env_s2)/2))*100),
                                   absdiff_g2g3 = abs(((env_s2-env_s3)/((env_s2 + env_s3)/2))*100),
                                   absdiff_g1g3 = abs(((env_s1-env_s3)/((env_s1 + env_s3)/2))*100))
                                                                   
                                   

# calculate mode, filter out -inf

envelope_summ <- env_filt %>%  dplyr::group_by(site_id) %>% 
                                 dplyr::summarise(mean_en = mean(envelope, na.rm = T), 
                                                   median_en = median(envelope, na.rm = T),
                                                   q80_en = quantile(envelope,na.rm = T,probs = 0.80),
                                                   q85_en = quantile(envelope,na.rm = T,probs = 0.85),
                                                   q90_en = quantile(envelope,na.rm = T,probs = 0.90), 
                                                   mod_en = modeest::mfv1(round(envelope,1)),
                                                   median_g1 = median(env_s1, na.rm = T),
                                                   median_g2 = median(env_s2, na.rm = T),
                                                   median_g3 = median(env_s3, na.rm = T),
                                                   count = dplyr::n())

envelope_summ_qt <- env_filt %>%  dplyr::mutate(qtr = lubridate::quarter(dailytmi))%>%
                                    dplyr::group_by(site_id, qtr) %>% 
                                    dplyr::summarise(mean_en = mean(envelope, na.rm = T), 
                                                     median_en = median(envelope, na.rm = T),
                                                     q80_en = quantile(envelope,na.rm = T,probs = 0.80),
                                                     q85_en = quantile(envelope,na.rm = T,probs = 0.85),
                                                     q90_en = quantile(envelope,na.rm = T,probs = 0.90), 
                                                     mod_en = modeest::mfv1(round(envelope,1)), 
                                                     median_g1 = median(env_s1, na.rm = T),
                                                     median_g2 = median(env_s2, na.rm = T),
                                                     median_g3 = median(env_s3, na.rm = T),
                                                     count = dplyr::n())

envelope_summ_month <- env_filt %>%  dplyr::mutate(mnth = lubridate::floor_date(dailytmi, 'month'))%>%
                                      dplyr::group_by(site_id, mnth) %>% 
                                      dplyr::summarise(mean_en = mean(envelope, na.rm = T), 
                                                       median_en = median(envelope, na.rm = T),
                                                       q80_en = quantile(envelope,na.rm = T,probs = 0.80),
                                                       q85_en = quantile(envelope,na.rm = T,probs = 0.85),
                                                       q90_en = quantile(envelope,na.rm = T,probs = 0.90), 
                                                       mod_en = modeest::mfv1(round(envelope,1)),
                                                       median_g1 = median(env_s1, na.rm = T),
                                                   median_g2 = median(env_s2, na.rm = T),
                                                   median_g3 = median(env_s3, na.rm = T),
                                                       count = dplyr::n())




# test_df <- data.frame(site = NA, 
#                       
#                         g1_g2pval = NA,
#                        
#                         g2_g3pval = NA, 
#                        
#                         g1_g3pval = NA)
# 
# for (site in unique(env_filt$site_id)) {
# 
# print(site)
# df_filt = env_filt %>% filter(site_id == site)
# print(nrow(df_filt))
# t1 <- t.test(df_filt$env_s1-df_filt$env_s3)
# t2 <- t.test(df_filt$env_s2-df_filt$env_s3)
# t3 <- t.test(df_filt$env_s1-df_filt$env_s2)
# 
# test_list <- c(site = site,
#                g1_g2pval = t1$p.value,
#                g2_g3pval = t2$p.value,
#                g1_g3pval = t3$p.value)
# 
# test_df <- rbind(test_df, test_list) 
# 
# }

# #all but two sites fail for check of differences in envelope from gauge to gauge
# test_df$g1_g2pval <- as.numeric(test_df$g1_g2pval)
# test_df$g2_g3pval <- as.numeric(test_df$g2_g3pval)
# test_df$g1_g3pval <- as.numeric(test_df$g1_g3pval)
# 
# 
# 
# test_df %>% filter(g1_g2pval < 0.05| g2_g3pval < 0.05 |  g1_g3pval < 0.05 ) -> site_fail_any
# test_df %>% filter(g1_g2pval < 0.05& g2_g3pval < 0.05 & g1_g3pval < 0.05 ) -> site_fail_all
# test_df %>% filter(g1_g2pval > 0.05) %>% filter( g2_g3pval > 0.05 ) %>% filter(g1_g3pval > 0.05 ) -> site_pass
# 



###median by site, monthh
# 
# test_df <- data.frame(site = NA, 
#                       g1_g2pval = NA,
#                       
#                       g2_g3pval = NA, 
#                       
#                       g1_g3pval = NA
# )
# 
# 
# 
# for (site in unique(envelope_summ_month$site_id)) {
#   
#   print(site)
#   df_filt = envelope_summ_month %>% filter(site_id == site)
#   print(nrow(df_filt))
#   t1 <- t.test(df_filt$median_g1-df_filt$median_g2)
#   t2 <- t.test(df_filt$median_g2-df_filt$median_g3)
#   t3 <- t.test(df_filt$median_g1-df_filt$median_g3)
#   # t4 <- t.test(env_filt$envelope-env_filt$env_s1)
#   # t5 <- t.test(env_filt$envelope-env_filt$env_s2)
#   # t6 <- t.test(env_filt$envelope-env_filt$env_s3)
#   
#   test_list <- c(site = site, g1_g2pval = as.numeric(t1$p.value), g2_g3pval = as.numeric(t2$p.value),  g1_g3pval =  as.numeric(t3$p.value) )
#   
#   test_df <- rbind(test_df, test_list) 
#   
# }
# 

# #all but two sites fail for check of differences in envelope from gauge to gauge
# test_df$g1_g2pval <- as.numeric(test_df$g1_g2pval)
# test_df$g2_g3pval <- as.numeric(test_df$g2_g3pval)
# test_df$g1_g3pval <- as.numeric(test_df$g1_g3pval)
# 
# 
# test_df %>% filter(g1_g2pval < 0.05| g2_g3pval < 0.05 |  g1_g3pval < 0.05 ) -> site_fail_any
# test_df %>% filter(g1_g2pval < 0.05& g2_g3pval < 0.05 & g1_g3pval < 0.05 ) -> site_fail_all
# test_df %>% filter(g1_g2pval > 0.05) %>% filter( g2_g3pval > 0.05 ) %>% filter(g1_g3pval > 0.05 ) -> site_pass


ggplot(env_filt) +
  geom_histogram(aes(x = absdiff_g1g2 )) + 
  facet_wrap(.~site_id,  scales="free")

ggplot(env_filt) +
  geom_histogram(aes(x = absdiff_g1g3 )) + 
  facet_wrap(.~site_id,  scales="free")

for (site in unique(env_filt$site_id)[1:5]){
p<- env_filt %>% filter(envelope <=20) %>% 
  dplyr::mutate(mnth = lubridate::floor_date(dailytmi, 'month')) %>%
  filter(site_id == site) %>% 
ggplot() +
  geom_histogram(aes(x = envelope )) + 
  facet_wrap(.~as.factor(mnth))+
  ggtitle(paste0("envelope by month for  " , site))
print(p)}

for (site in unique(envelope_summ_month$site_id)){
  p<- envelope_summ_month %>% 
    filter(site_id == site) %>% 
    ggplot() +
    geom_line(aes(y = median_en, x = mnth), color = 'red') + 
    #geom_line(aes( y = q80_en, x = mnth), color = 'darkgreen')+
    geom_line(aes( y = mod_en, x = mnth), color = 'black')+
    ggtitle(paste0("median envelope by month for  " , site))+ 
    ylim(0,6)
  print(p)}

env <- data.frame(site = unique(envelope_summ_month$site_id),
                  envelope = NA, 
                  start_day_of_year = 1, 
                  end_day_of_year = 365, 
                  start_date =  '2012-01-01',
                  end_date = NA) 

#TALL, not seasonal
env$envelope[env$site == 'TALL'] <- 2.0 

env$envelope[env$site == 'SRER'] <- 0.4

env$envelope[env$site == 'PUUM'] <- 2.0

env$envelope[env$site == 'OSBS'] <- 2.3

env$envelope[env$site == 'GUAN'] <- 0.8

env$envelope[env$site == 'WREF'] <- 2.0

env$envelope[env$site == 'WOOD'] <- 0.9

env$envelope[env$site == 'UNDE'] <- 0.9

#TOOL, seasonal likely 1.0 winter, 2.0 summer?
env$envelope[env$site == 'TOOL'] <- 1.0

env$envelope[env$site == 'SJER'] <- 2.0

#SCBI much higher in 2024, could be noisier gauge?
env$envelope[env$site == 'SCBI'] <- 0.8

#REDB (skip? trash data)

#prin, higher early 2023

env$envelope[env$site == 'PRIN'] <- 0.9

env$envelope[env$site == 'ORNL'] <- 1.0

env$envelope[env$site == 'ONAQ'] <- 0.9

#KONZ. high noise, higher in summer maybe? aim for the middle for now
env$envelope[env$site == 'KONZ'] <- 3.0

#HARV, maybe seasonal, higher in both summers, aim for the middle for now
env$envelope[env$site == 'HARV'] <- 1.5

#CPER, pretty noisy, maybe seasonal?
env$envelope[env$site == 'CPER'] <- 3.0

env$envelope[env$site == 'CLBJ'] <- 0.8

env$envelope[env$site == 'BONA'] <- 1.7

env$envelope[env$site == 'BLUE'] <- 1.8

env$envelope[env$site == 'ARIK'] <- 1.4

#MISSING NIWO, REDB, YELL


readr::write_csv(env, file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope_threshold.csv')
readr::read_csv( file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope_threshold.csv')

