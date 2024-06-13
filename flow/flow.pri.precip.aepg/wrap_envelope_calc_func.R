library(magrittr)
  
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
for (cfg in cfgs){
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

readr::write_csv(df, file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope.csv')

envelope_df <-  readr::read_csv( file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope.csv', col_names = T)
# calculate mode, filter out -inf

envelope_summ <- envelope_df %>% dplyr::filter(envelope != -Inf) %>% 
                                 dplyr::group_by(site_id) %>% 
                                 dplyr::summarise(mean_en = mean(envelope, na.rm = T), 
                                                   median_en = median(envelope, na.rm = T),
                                                   q80_en = quantile(envelope,na.rm = T,probs = 0.80),
                                                   q85_en = quantile(envelope,na.rm = T,probs = 0.85),
                                                   q90_en = quantile(envelope,na.rm = T,probs = 0.90), 
                                                   count = dplyr::n())

envelope_summ_qt <- envelope_df %>% dplyr::filter(envelope != -Inf) %>% 
                                    dplyr::mutate(qtr = lubridate::quarter(dailytmi))%>%
                                    dplyr::group_by(site_id, qtr) %>% 
                                    dplyr::summarise(mean_en = mean(envelope, na.rm = T), 
                                                     median_en = median(envelope, na.rm = T),
                                                     q80_en = quantile(envelope,na.rm = T,probs = 0.80),
                                                     q85_en = quantile(envelope,na.rm = T,probs = 0.85),
                                                     q90_en = quantile(envelope,na.rm = T,probs = 0.90), 
                                                     count = dplyr::n())

envelope_summ_month <- envelope_df %>% dplyr::filter(envelope != -Inf) %>% 
                                      dplyr::mutate(mnth = lubridate::month(dailytmi))%>%
                                      dplyr::group_by(site_id, mnth) %>% 
                                      dplyr::summarise(mean_en = mean(envelope, na.rm = T), 
                                                       median_en = median(envelope, na.rm = T),
                                                       q80_en = quantile(envelope,na.rm = T,probs = 0.80),
                                                       q85_en = quantile(envelope,na.rm = T,probs = 0.85),
                                                       q90_en = quantile(envelope,na.rm = T,probs = 0.90), 
                                                       count = dplyr::n())

