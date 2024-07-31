##some prism comps with output from smoothing function 
dirSmooth <- '/scratch/pfs/precipWeighing_compute_precip'

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='*.parquet',recursive=TRUE,full.names=TRUE)
sites <- stringr::str_sub(unique(unlist(stringr::str_extract_all(filesAll, pattern = '[A-Z]{4}9'))), start = 1, end = 4)

slopes <- as.data.frame(sites)
slopes$prism_NEON <- NA
slopes$rsq_prism_NEON <- NA
slopes$daymet_NEON <- NA
slopes$rsq_daymet_NEON<- NA
slopes$daymet_prism <- NA
slopes$pri_sec <- NA
slopes$rsq_pri_sec <- NA

totalz <- as.data.frame(sites)
totalz$totalNEON <- NA
totalz$prism <- NA

#prism vs NEON
for (site in slopes$sites){
    tryCatch (
      {
    ptrnSite <- paste0('*/precip-weighing_',site,'*')
    filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
    VarKeep=c('startDateTime','endDateTime','precipBulk')
    strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,Var=VarKeep,VarTime='startDateTime',Df=TRUE)
    
    ### pull in prism data
    # site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
    prism_files <- list.files('/scratch/prism', full.names = T)
    
    file <- stringr::str_subset(prism_files, pattern = site)
    prism <- readr::read_csv(file)
    
    prism_wk <- prism %>% mutate(startDate = lubridate::floor_date(startDate, '1 day')) %>%
      group_by(startDate) %>%
      summarise(ppt = sum(ppt))
    
    strainGaugeDepthAgr_prism <- strainGaugeDepthAgr %>%
      #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
       mutate(startDateTime = startDateTime + 12*60*60,
               endDateTime = startDateTime + 12*60*60) %>%
      mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
      group_by(startDate) %>%
      summarise(dailyPrecipNEON = sum(precipBulk),
                  na_length= length(which(is.na(precipBulk))))
    
    
    dfpr <- left_join(strainGaugeDepthAgr_prism, prism_wk, by = 'startDate')
    
    #grab regressions
    modl <- lm(dailyPrecipNEON~ppt, data = dfpr)
    rsq <- summary(modl)
    
    slopes$prism_NEON[slopes$site==site] <- modl$coefficients[[2]]
    slopes$rsq_prism_NEON[slopes$site==site] <- rsq$r.squared[[1]]
    
    totalz$totalNEON <- sum(dfpr$dailyPrecipNEON)
    totalz$prism <- sum(dfpr$ppt)
  },
error = function(e) {slopes$reg[slopes$site==site] <-NA
  })
}

#daymet vs NEON
for (site in slopes$sites){
  tryCatch (
    {
    ptrnSite <- paste0('*/precip-weighing_',site,'*')
    filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
    VarKeep=c('startDateTime','endDateTime','precipBulk')
    strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,Var=VarKeep,VarTime='startDateTime',Df=TRUE)
    
    ### pull in prism data
    # site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
    daymet_files <- list.files('/scratch/daymet', full.names = T)
    
    file <- stringr::str_subset(daymet_files, pattern = site)
    daymet <- readr::read_csv(file)
    dates <-lubridate::ymd(daymet$`system:index`, format = "%y%m%d")
    daymet$startDate <- dates[1:(length(dates)-1)]
    daymet_wk <- daymet %>% mutate(startDate = lubridate::floor_date(startDate, '1 week')) %>%
      group_by(startDate) %>%
      summarise(prcp = sum(prcp))
    
    strainGaugeDepthAgr_daymet <- strainGaugeDepthAgr %>%
      mutate(startDate = lubridate::floor_date(startDateTime, '1 week')) %>%
      group_by(startDate) %>%
      summarise(dailyPrecipNEON = sum(precipBulk))
    
    dfdm <- left_join(strainGaugeDepthAgr_daymet, daymet_wk[,c('startDate', 'prcp')], by = 'startDate')
    
    #grab regressions
    modl <- lm(dailyPrecipNEON~prcp, data = dfdm)
    slopes$daymet_NEON[slopes$site==site] <- modl$coefficients[[2]]
    rsq <- summary(modl)
    slopes$rsq_daymet_NEON[slopes$site==site] <- rsq$r.squared[[1]]
    
    },
    error = function(e) {slopes$daymet_NEON[slopes$site==site] <-NA
    })
}

#daymet vs prism
for (site in slopes$sites){
  tryCatch (
    {
      ### pull in prism data
      # site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
      daymet_files <- list.files('/scratch/daymet', full.names = T)
      
      file <- stringr::str_subset(daymet_files, pattern = site)
      daymet <- readr::read_csv(file)
      dates <-lubridate::ymd(daymet$`system:index`, format = "%y%m%d")
      daymet$startDate <- dates[1:(length(dates)-1)]
      daymet_wk <- daymet %>% mutate(startDate = lubridate::floor_date(startDate, '1 week')) %>%
        group_by(startDate) %>%
        summarise(prcp = sum(prcp))
      
      ### pull in prism data
      prism_files <- list.files('/scratch/prism', full.names = T)
      
      file <- stringr::str_subset(prism_files, pattern = site)
      prism <- readr::read_csv(file)
      
      prism_wk <- prism %>% mutate(startDate = lubridate::floor_date(startDate, '1 week')) %>%
        group_by(startDate) %>%
        summarise(ppt = sum(ppt))
      
      dfdmpr <- left_join( daymet_wk[,c('startDate', 'prcp')], prism_wk, by = 'startDate')
      
      #grab regressions
      modl <- lm(prcp~ppt, data = dfdmpr)
      slopes$daymet_prism[slopes$site==site] <- modl$coefficients[[2]]

    },
    error = function(e) {slopes$daymet_prism[slopes$site==site] <-NA
    })
  
}

sec_sites <- c('UNDE', 'WREF', 'OSBS', 'ORNL', 'HARV', 'TALL', 'SCBI')
sec_data <- neonUtilities::loadByProduct(dpID = 'DP1.00006.001',
                                         startdate = '2023-01',
                                         enddate = '2024-06', 
                                         site = sec_sites,
                                         tabl = 'SECPRE_30min')
sec <- sec_data$SECPRE_30min %>% 
  mutate(secPrecipBulk = ifelse(secPrecipSciRvwQF == 1|secPrecipSciRvwQF==2, NA,secPrecipBulk ))
secAgr <- sec %>%
  mutate(startDate = lubridate::floor_date(startDateTime, '1 week')) %>%
  group_by(startDate, siteID) %>%
  summarise(secNEON = sum(secPrecipBulk))

#prism vs NEON
for (site in sec_sites){
  tryCatch (
    {
      ptrnSite <- paste0('*/precip-weighing_',site,'*')
      filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
      VarKeep=c('startDateTime','endDateTime','precipBulk')
      strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,Var=VarKeep,VarTime='startDateTime',Df=TRUE)
      
      strainGaugeDepthAgr <- strainGaugeDepthAgr %>%
        mutate(startDate = lubridate::floor_date(startDateTime, '1 week')) %>%
        group_by(startDate) %>%
        summarise(dailyPrecipNEON = sum(precipBulk))
      
      secAgr_site <- secAgr %>% dplyr::filter(siteID == site)
      
      dfneon <- left_join(strainGaugeDepthAgr, secAgr_site, by = 'startDate')
      
      #grab regressions
      modl <- lm(dailyPrecipNEON~secNEON, data = dfneon)
      rsq <- summary(modl)
      
      slopes$pri_sec[slopes$site==site] <- modl$coefficients[[2]]
      slopes$rsq_pri_sec[slopes$site==site] <- rsq$r.squared[[1]]
    },
    error = function(e) {slopes$pri_sec[slopes$site==site] <- NA
    })
}

