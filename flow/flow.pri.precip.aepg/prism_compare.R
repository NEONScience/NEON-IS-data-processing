##some prism comps with output from smoothing function 
site <- 'OSBS'
dirSmooth <- '/scratch/pfs/precipWeighing_compute_precip'

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='*.parquet',recursive=TRUE,full.names=TRUE)
ptrnSite <- paste0('*/precip-weighing_',site,'*')
filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
VarKeep=c('startDateTime','endDateTime','precipBulk','precipType')
strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,Var=VarKeep,VarTime='startDateTime',Df=TRUE)


### pull in prism data
# site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
prism_files <- list.files('/scratch/prism', full.names = T)

file <- stringr::str_subset(prism_files, pattern = site)
prism <- readr::read_csv(file)
strainGaugeDepthAgr_prism <- strainGaugeDepthAgr %>%
  #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
  mutate(startDateTime = startDateTime + 12*60*60,
          endDateTime = startDateTime + 12*60*60) %>%
  mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
  group_by(startDate) %>%
  summarise(dailyPrecipNEON = sum(precipBulk))

dfpr <- left_join(strainGaugeDepthAgr_prism, prism, by = 'startDate')

#trim first and last days of data set because comparison is likely skewed due to time shifts

dfpr <- dfpr[2:(nrow(dfpr)-1), ]
dfpr_long <- data.table::melt(dfpr,id.vars=c('startDate'))

# Find calibration events
setRecharge <- which(strainGaugeDepthAgr$precipType=="ExcludeBeforeRecharge")
setCal <- unique(c(setRecharge[diff(setRecharge) > 1],tail(setRecharge,1)))
# dfCal <- strainGaugeDepthAgr[setCal,]

#daily comps
library(plotly)
vline <- function(x = 0,y=1, color = "red") {
  list(
    type = "line",
    y0 = 0,
    y1 = y,
    yref = "paper",
    x0 = x,
    x1 = x,
    line = list(color = color, dash="dot")
  )
}

p <- plotly::plot_ly(data=dfpr_long,x=~startDate,y=~value,color=~variable, type = 'bar', mode = 'markers') %>% 
  plotly::layout(title = paste0('PRISM vs NEON at ', site,' - daily')) 
for (i in seq_len(length(setCal))){
  print(i)
  p<-p %>% 
    plotly::add_trace(x =strainGaugeDepthAgr$startDateTime[setCal[i]], type = 'scatter', mode = 'lines',
                      line = list(color = "red", dash = "dash"))
}
print(p)

plotly::plot_ly(data=dfpr,x=~ppt,y=~dailyPrecipNEON, type = 'scatter', mode = 'markers', hoverinfo= 'text', text = format(dfpr$startDate,'%Y-%m-%d')) %>% 
  plotly::layout(title = paste0('PRISM vs NEON at ', site,' - daily')) %>%
  plotly::layout(shapes = list(list(
    type = "line", 
    x0 = 0, 
    x1 = ~max(ppt, dailyPrecipNEON,na.rm=TRUE), 
    xref = "x",
    y0 = 0, 
    y1 = ~max(ppt, dailyPrecipNEON,na.rm=TRUE), 
    yref = "y",
    line = list(color = "black")
  )))


#weekly comp
dfpr_week <- dfpr %>% 
  dplyr::mutate(week = lubridate::floor_date(startDate, 'week')) %>% 
  dplyr::group_by(week) %>% 
  dplyr::summarise(prism = base::sum(ppt, na.rm = T),
                   neon = base::sum(dailyPrecipNEON, na.rm = T))

dfpr_week_long <- data.table::melt(dfpr_week,id.vars=c('week'))

#weekly plots
if (TRUE) {
  p <- plotly::plot_ly(data=dfpr_week_long,x=~week,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
        plotly::layout(title = paste0('PRISM vs NEON at ', site,' - Weekly'))
  for (i in seq_len(length(setCal))){
    print(i)
    p<-p %>% 
      plotly::add_trace(x =strainGaugeDepthAgr$startDateTime[setCal[i]], type = 'scatter', mode = 'lines',
                        line = list(color = "red", dash = "dash"))
  }
  print(p)
  
plotly::plot_ly(data=dfpr_week,x=~prism,y=~neon, type = 'scatter', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site,' - Weekly')) %>%
  plotly::layout(shapes = list(list(
    type = "line", 
    x0 = 0, 
    x1 = ~max(prism, neon,na.rm=TRUE), 
    xref = "x",
    y0 = 0, 
    y1 = ~max(prism, neon,na.rm=TRUE), 
    yref = "y",
    line = list(color = "black")
  )))
}

#sum by month 
dfpr_mnth <- dfpr %>% 
  dplyr::mutate(mnth = lubridate::floor_date(startDate, 'month')) %>% 
  dplyr::group_by(mnth) %>% 
  dplyr::summarise(prism = base::sum(ppt, na.rm = T),
                  neon = base::sum(dailyPrecipNEON, na.rm = T))

dfpr_mnth_long <- data.table::melt(dfpr_mnth,id.vars=c('mnth'))

#monthly comps
if (TRUE) {
  p <- plotly::plot_ly(data=dfpr_mnth_long,x=~mnth,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site,' - monthly'))
  for (i in seq_len(length(setCal))){
    print(i)
    p<-p %>% 
      plotly::add_trace(x =strainGaugeDepthAgr$startDateTime[setCal[i]], type = 'scatter', mode = 'lines',
                        line = list(color = "red", dash = "dash"))
  }
  print(p)
  
plotly::plot_ly(data=dfpr_mnth,x=~prism,y=~neon, type = 'scatter', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site,' - monthly')) %>%
  plotly::layout(shapes = list(list(
    type = "line", 
    x0 = 0, 
    x1 = ~max(prism, neon,na.rm=TRUE), 
    xref = "x",
    y0 = 0, 
    y1 = ~max(prism, neon,na.rm=TRUE), 
    yref = "y",
    line = list(color = "black")
  )))
}



# Annual sums
dfpr_year <- dfpr %>% 
  dplyr::mutate(year = lubridate::floor_date(startDate, 'year')) %>% 
  dplyr::group_by(year) %>% 
  dplyr::summarise(prism = base::sum(ppt, na.rm = T),
                   neon = base::sum(dailyPrecipNEON, na.rm = T))

dfpr_year_long <- data.table::melt(dfpr_year,id.vars=c('year'))

#annual plots
if (FALSE) {
  plotly::plot_ly(data=dfpr_year_long,x=~year,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
    plotly::layout(title = paste0('PRISM vs NEON at ', site,' - yearly'))
  plotly::plot_ly(data=dfpr_year,x=~prism,y=~neon, type = 'scatter', mode = 'markers') %>%
    plotly::layout(title = paste0('PRISM vs NEON at ', site,' - yearly')) %>%
    plotly::layout(shapes = list(list(
      type = "line", 
      x0 = 0, 
      x1 = ~max(prism, neon,na.rm=TRUE), 
      xref = "x",
      y0 = 0, 
      y1 = ~max(prism, neon,na.rm=TRUE), 
      yref = "y",
      line = list(color = "black")
    )))
  
}
