##some prism comps with output from smoothing function 

### pull in prism data
site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
prism_files <- list.files('/scratch/prism', full.names = T)

file <- stringr::str_subset(prism_files, pattern = site)
prism <- readr::read_csv(file)
strainGaugeDepthAgr_prism <- strainGaugeDepthAgr %>%
  #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
  mutate(startDateTime = startDateTime + 12*60*60,
          endDateTime = startDateTime + 12*60*60) %>%
  mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
  group_by(startDate) %>%
  summarise(dailyPrecipNEON = sum(weighPrecipBulk))

dfpr <- left_join(strainGaugeDepthAgr_prism, prism, by = 'startDate')

#trim first and last days of data set because comparison is likely skewed due to time shifts

dfpr <- dfpr[2:(nrow(dfpr)-1), ]
dfpr_long <- data.table::melt(dfpr,id.vars=c('startDate'))

#daily comps
plotly::plot_ly(data=dfpr_long,x=~startDate,y=~value,color=~variable, type = 'bar', mode = 'markers') %>% 
  plotly::layout(title = paste0('PRISM vs NEON at ', site))

plotly::plot_ly(data=dfpr,x=~ppt,y=~dailyPrecipNEON, type = 'scatter', mode = 'markers') %>% 
  plotly::layout(title = paste0('PRISM vs NEON at ', site)) 

#sum by month 
#### this won't be useful until we are past the point of the 21 day pad and have a longer TS
dfpr_mnth <- dfpr %>% 
  dplyr::mutate(mnth = lubridate::floor_date(startDate, 'month')) %>% 
  dplyr::group_by(mnth) %>% 
  dplyr::summarise(prism = base::sum(ppt, na.rm = T),
                  neon = base::sum(dailyPrecipNEON, na.rm = T))

dfpr_mnth_long <- data.table::melt(dfpr_mnth,id.vars=c('mnth'))

#monthly comps
plotly::plot_ly(data=dfpr_mnth_long,x=~mnth,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site))
plotly::plot_ly(data=dfpr_mnth,x=~prism,y=~neon, type = 'scatter', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site))

#weekly comp
#### this won't be useful until we are past the point of the 21 day pad and have a longer TS
dfpr_week <- dfpr %>% 
  dplyr::mutate(week = lubridate::floor_date(startDate, 'week')) %>% 
  dplyr::group_by(week) %>% 
  dplyr::summarise(prism = base::sum(ppt, na.rm = T),
                   neon = base::sum(dailyPrecipNEON, na.rm = T))

dfpr_week_long <- data.table::melt(dfpr_week,id.vars=c('week'))

#weekly comps
plotly::plot_ly(data=dfpr_week_long,x=~week,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site))
plotly::plot_ly(data=dfpr_week,x=~prism,y=~neon, type = 'scatter', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site))


