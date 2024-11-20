##some prism comps with output from smoothing function 
# library(dplyr)

site <- 'OSBS'

# dirSmooth <- '/scratch/pfs/precipWeighing_compute_precip_dynamic_minEvap_15.5'
# Div <- .75 # compensates for difference in slope of 0.25 lower for NEON cal. Set to 1 for no compensation.
dirSmooth <- '/scratch/pfs/precipWeighing_combine_precip'
Div <- 1 # compensates for difference in slope of 0.25 lower for NEON cal. Set to 1 for no compensation.

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE)
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.
ptrnSite <- paste0('*/precip-weighing_',site,'*')
filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
ptrnHourly <- paste0('stats_060')
filesSite <- filesSite[grepl(pattern=ptrnHourly,filesSite)]

strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,VarTime='startDateTime',Df=TRUE)

### pull in prism data
# site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
prism_files <- list.files('/scratch/prism/current', full.names = T)
prism_files <- list.files('/scratch/prism/current', full.names = T)

file <- stringr::str_subset(prism_files, pattern = site)
prism <- readr::read_csv(file)

#add flags to strainGaugeDepthAgr
#consolidate flags to daily
#remove if flagged


#removing data if any flagged for 3 key vars. 
# flags_sub <- strainGaugeDepthAgr[,c('insuffDataQF','extremePrecipQF')]
# flagVar <- 'finalQFTest'
# strainGaugeDepthAgr[[flagVar]] <- NA
# flag_0 <- rowSums(flags_sub == 0, na.rm = T)
# strainGaugeDepthAgr[[flagVar]][flag_0 == ncol(flags_sub)] <- 0
# flag_1 <- rowSums(flags_sub == 1, na.rm = T)
# strainGaugeDepthAgr[[flagVar]][flag_1 >=1] <- 1
# flags_neg1 <- rowSums(flags_sub == -1, na.rm = T)
# strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]]) & flags_neg1 >=1] <- -1
# strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]])] <- -1
# 

# strainGaugeDepthAgr_prism <- strainGaugeDepthAgrRglr %>%
strainGaugeDepthAgr_prism <- strainGaugeDepthAgr %>%
  #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
  mutate(startDateTime = startDateTime - 12*60*60) %>%
  mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
  group_by(startDate) %>%
  summarise(dailyPrecipNEON = sum(precipBulk)/Div,
            finalQF = max(finalQF,na.rm=T)) %>%
  mutate(startDate=as.Date(startDate)) %>% 
  #filter(heaterFlag < 0.1) %>% 
  filter(finalQF < 1)


dfpr <- dplyr::inner_join(strainGaugeDepthAgr_prism, prism, by = 'startDate')

#trim first and last days of data set because comparison is likely skewed due to time shifts
dfpr <- dfpr[2:(nrow(dfpr)-1), ]

# Get rid of any rows in which either NEON or prism is NA
setKeep <- !is.na(dfpr$dailyPrecipNEON) & !is.na(dfpr$ppt)
dfpr <- dfpr[setKeep,]

# Compute daily regression
regrDaily <- lm(dailyPrecipNEON ~ ppt,dfpr)
rsqDaily <- summary(regrDaily)$r.squared
regrLine <- data.frame(x = c(0,max(dfpr$ppt,na.rm=TRUE)),
                       y=c(regrDaily$coefficients[1],max(dfpr$ppt,na.rm=TRUE)*regrDaily$coefficients[2]+regrDaily$coefficients[1]))

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

dfpr_long <- data.table::melt(dfpr,id.vars=c('startDate'))
p <- plotly::plot_ly(data=dfpr_long,x=~startDate,y=~value,color=~variable, type = 'bar', mode = 'markers') %>% 
  plotly::layout(title = paste0('PRISM vs NEON at ', site,' - daily')) 
for (i in seq_len(length(setCal))){
  print(i)
  p<- p %>% 
    plotly::add_trace(x =strainGaugeDepthAgr$startDateTime[setCal[i]], type = 'scatter', mode = 'lines',
                      line = list(color = "red", dash = "dash"))
}
print(p)

plotly::plot_ly(data=dfpr,x=~ppt,y=~dailyPrecipNEON, type = 'scatter', mode = 'markers', hoverinfo= 'text', text = format(dfpr$startDate,'%Y-%m-%d')) %>% 
plotly::layout(title = paste0('PRISM vs NEON at ', site,' - daily'),
               showlegend = FALSE) %>%
  plotly::layout(shapes = list(
    list(
      type = "line", 
      x0 = 0, 
      x1 = ~max(ppt, dailyPrecipNEON,na.rm=TRUE), 
      xref = "x",
      y0 = 0, 
      y1 = ~max(ppt, dailyPrecipNEON,na.rm=TRUE), 
      yref = "y",
      line = list(color = "black")
    ),
    list(
      type = "line", 
      x0 = regrLine$x[1], 
      x1 = regrLine$x[2], 
      xref = "x",
      y0 = regrLine$y[1], 
      y1 = regrLine$y[2], 
      yref = "y",
      line = list(color = "red")
    )
  )
) %>%
  plotly::add_trace(x=~max(ppt, dailyPrecipNEON,na.rm=TRUE)*.75,
                    y=~max(ppt, dailyPrecipNEON,na.rm=TRUE)*.25,
                    name = 'fit',
                    type = 'scatter',
                    mode = 'text',
                    text=paste0('y = ',round(regrDaily$coefficients[2],2),'x + ',round(regrDaily$coefficients[1],2),'; R2 = ', round(rsqDaily,2)),
                    textposition = 'middle center',
                    textfont = list(color = 'red', size = 14)
  )
  


#weekly comp
dfpr_week <- dfpr %>% 
  dplyr::mutate(week = lubridate::floor_date(startDate, 'week')) %>% 
  dplyr::group_by(week) %>% 
  dplyr::summarise(prism = base::sum(ppt, na.rm = T),
                   neon = base::sum(dailyPrecipNEON, na.rm = T))

dfpr_week_long <- data.table::melt(dfpr_week,id.vars=c('week'))

#weekly plots
if (FALSE) {
  p <- plotly::plot_ly(data=dfpr_week_long,x=~week,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
        plotly::layout(title = paste0('PRISM vs NEON at ', site,' - Weekly'))
  for (i in seq_len(length(setCal))){
    print(i)
    p<-p %>% 
      plotly::add_trace(x =strainGaugeDepthAgr$startDateTime[setCal[i]], type = 'scatter', mode = 'lines',
                        line = list(color = "red", dash = "dash"))
  }
  print(p)
  
plotly::plot_ly(data=dfpr_week,x=~prism,y=~neon, type = 'scatter', mode = 'markers', hoverinfo= 'text', text = format(dfpr_week$week,'%Y-%m-%d')) %>%
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
if (FALSE) {
  p <- plotly::plot_ly(data=dfpr_mnth_long,x=~mnth,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
  plotly::layout(title = paste0('PRISM vs NEON at ', site,' - monthly'))
  for (i in seq_len(length(setCal))){
    print(i)
    p<-p %>% 
      plotly::add_trace(x =strainGaugeDepthAgr$startDateTime[setCal[i]], type = 'scatter', mode = 'lines',
                        line = list(color = "red", dash = "dash"))
  }
  print(p)
  
plotly::plot_ly(data=dfpr_mnth,x=~prism,y=~neon, type = 'scatter', mode = 'markers', hoverinfo= 'text', text = format(dfpr_mnth$mnth,'%Y-%m')) %>%
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
if (TRUE) {
  plotly::plot_ly(data=dfpr_year_long,x=~year,y=~value,color=~variable, type = 'bar', mode = 'markers') %>%
    plotly::layout(title = paste0('PRISM vs NEON at ', site,' - yearly'))
  
  plotly::plot_ly(data=dfpr_year,x=~prism,y=~neon, type = 'scatter', mode = 'markers', hoverinfo= 'text', text = format(dfpr_year$year,'%Y')) %>%
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

