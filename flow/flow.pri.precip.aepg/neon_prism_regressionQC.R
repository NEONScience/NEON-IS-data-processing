##some prism comps with output from smoothing function 
dirSmooth <- '/scratch/pfs/precipWeighing_combine_precip'

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE)
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.
filesFlagsAll <- list.files(path=dirSmooth,pattern='flagsSmooth.parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.
filesFlagsAll <- list.files(path=dirSmooth,pattern='flagsSmooth.parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.

sites <- stringr::str_sub(unique(unlist(stringr::str_extract_all(filesAll, pattern = '[A-Z]{4}9'))), start = 1, end = 4)

  slopes <- as.data.frame(sites)
  slopes$prism_NEON <- NA
  slopes$rsq_prism_NEON <- NA
  slopes$prism_NEON_QC <- NA
  slopes$rsq_prism_NEON_QC<- NA


#prism vs NEON
for (site in slopes$sites[2:25]){
  print(site)
    tryCatch (
      {
        ptrnSite <- paste0('*/precip-weighing_',site,'*')
        filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
        filesFlagsSite <- filesFlagsAll[grepl(pattern=ptrnSite,filesFlagsAll)]
        
        VarKeep=c('startDateTime','endDateTime','precipBulk','precipType')
        strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,Var=VarKeep,VarTime='startDateTime',Df=TRUE)
        flagsAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesFlagsSite,VarTime='startDateTime',Df=TRUE)
        
        ### pull in prism data
        # site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
        prism_files <- list.files('/scratch/prism/current', full.names = T)
        prism_files <- list.files('/scratch/prism/current', full.names = T)
        
        file <- stringr::str_subset(prism_files, pattern = site)
        prism <- readr::read_csv(file)
        
        #add flags to strainGaugeDepthAgr
        #consolidate flags to daily
        #remove if flagged
      strainGaugeDepthAgr <- full_join(strainGaugeDepthAgr, flagsAgr, by = c('startDateTime', 'endDateTime'))
        
      strainGaugeDepthAgr_prism <- strainGaugeDepthAgr %>%
        #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
        mutate(startDateTime = startDateTime - 12*60*60) %>%
        mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
        group_by(startDate) %>%
        summarise(dailyPrecipNEON = sum(precipBulk))
        
        #removing data if any flagged for 3 key vars. 
        #flags_sub <- strainGaugeDepthAgr[,c('insuffDataQF','extremePrecipQF','dielNoiseQF', 'heaterErrorQF')]
        #flags_sub <- strainGaugeDepthAgr[,c('insuffDataQF','extremePrecipQF','dielNoiseQF')]
        flags_sub <- strainGaugeDepthAgr[,c('insuffDataQF','extremePrecipQF', 'heaterErrorQF')]
        
        
        flagVar <- 'finalQFTest'
        strainGaugeDepthAgr[[flagVar]] <- NA
        flag_0 <- rowSums(flags_sub == 0, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_0 == ncol(flags_sub)] <- 0
        flag_1 <- rowSums(flags_sub == 1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_1 >=1] <- 1
        flags_neg1 <- rowSums(flags_sub == -1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]]) & flags_neg1 >=1] <- -1
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]])] <- -1
        
        strainGaugeDepthAgr_prismQC <- strainGaugeDepthAgr %>%
          #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
          mutate(startDateTime = startDateTime - 12*60*60) %>%
          mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
          group_by(startDate) %>%
          summarise(dailyPrecipNEON = sum(precipBulk),
                    maxFlag = max(finalQFTest, na.rm = T)) %>%
          mutate(startDate=as.Date(startDate)) %>% 
          filter(maxFlag < 1)

###################non QCd    
    dfpr <- dplyr::inner_join(strainGaugeDepthAgr_prism, prism, by = 'startDate')
    #trim first and last days of data set because comparison is likely skewed due to time shifts
    dfpr <- dfpr[2:(nrow(dfpr)-1), ]
    
    # Get rid of any rows in which either NEON or prism is NA
    setKeep <- !is.na(dfpr$dailyPrecipNEON) & !is.na(dfpr$ppt)
    dfpr <- dfpr[setKeep,]
  
    #grab regressions
    modl <- lm(dailyPrecipNEON~ppt, data = dfpr)
    rsq <- summary(modl)
    
    slopes$prism_NEON[slopes$site==site] <- modl$coefficients[[2]]
    slopes$rsq_prism_NEON[slopes$site==site] <- rsq$r.squared[[1]]
    slopes$numPts[slopes$site==site] <- nrow(dfpr)
    
      
###################QC'd
    dfpr <- dplyr::inner_join(strainGaugeDepthAgr_prismQC, prism, by = 'startDate')
    #trim first and last days of data set because comparison is likely skewed due to time shifts
    dfpr <- dfpr[2:(nrow(dfpr)-1), ]
    
    # Get rid of any rows in which either NEON or prism is NA
    setKeep <- !is.na(dfpr$dailyPrecipNEON) & !is.na(dfpr$ppt)
    dfpr <- dfpr[setKeep,]
    
    #grab regressions
    modl <- lm(dailyPrecipNEON~ppt, data = dfpr)
    rsq <- summary(modl)   
    
    slopes$prism_NEON_QC[slopes$site==site] <- modl$coefficients[[2]]
    slopes$rsq_prism_NEON_QC[slopes$site==site] <- rsq$r.squared[[1]]
    slopes$numPts_QC[slopes$site==site] <- nrow(dfpr)

  },
error = function(e) {slopes$reg[slopes$site==site] <-NA
  })
}
