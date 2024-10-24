##some prism comps with output from smoothing function 
old = T
if (old){
  dirSmooth <- '/scratch/pfs/precipWeighing_combine_precip'
  

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='*.parquet',recursive=TRUE,full.names=TRUE)
filesAll <- list.files(path=dirSmooth,pattern='*.parquet',recursive=TRUE,full.names=TRUE)

sites <- stringr::str_sub(unique(unlist(stringr::str_extract_all(filesAll, pattern = '[A-Z]{4}9'))), start = 1, end = 4)

slopes <- as.data.frame(sites)
slopes$prism_NEON_old <- NA
slopes$rsq_prism_NEON_old <- NA


}else{
  dirSmooth <- '/scratch/pfs/precipWeighing_combine_precip'
  

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='*.parquet',recursive=TRUE,full.names=TRUE)
filesAll <- list.files(path=dirSmooth,pattern='*.parquet',recursive=TRUE,full.names=TRUE)

sites <- stringr::str_sub(unique(unlist(stringr::str_extract_all(filesAll, pattern = '[A-Z]{4}9'))), start = 1, end = 4)

slopes$prism_NEON <- NA
slopes$rsq_prism_NEON <- NA
}

#prism vs NEON
for (site in slopes$sites[2:25]){
    tryCatch (
      {
    ptrnSite <- paste0('*/precip-weighing_',site,'*')
    filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
    VarKeep=c('startDateTime','endDateTime','precipBulk')
    strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,Var=VarKeep,VarTime='startDateTime',Df=TRUE)
    
    ### pull in prism data
    # site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
    prism_files <- list.files('/scratch/prism/current', full.names = T)
    prism_files <- list.files('/scratch/prism/current', full.names = T)
    
    
    file <- stringr::str_subset(prism_files, pattern = site)
    prism <- readr::read_csv(file)
    
    strainGaugeDepthAgr_prism <- strainGaugeDepthAgr %>%
      #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
       mutate(startDateTime = startDateTime - 12*60*60,
               endDateTime = startDateTime - 12*60*60) %>%
      mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
      group_by(startDate) %>%
      summarise(dailyPrecipNEON = sum(precipBulk))
    
    
    dfpr <- inner_join(strainGaugeDepthAgr_prism, prism, by = 'startDate')
    
    #trim first and last days of data set because comparison is likely skewed due to time shifts
    dfpr <- dfpr[2:(nrow(dfpr)-1), ]
    
    # Get rid of any rows in which either NEON or prism is NA
    setKeep <- !is.na(dfpr$dailyPrecipNEON) & !is.na(dfpr$ppt)
    dfpr <- dfpr[setKeep,]
    
    
    #grab regressions
    modl <- lm(dailyPrecipNEON~ppt, data = dfpr)
    rsq <- summary(modl)
    
    if (old){
      slopes$prism_NEON_old[slopes$site==site] <- modl$coefficients[[2]]
      slopes$rsq_prism_NEON_old[slopes$site==site] <- rsq$r.squared[[1]]
    }else{
      slopes$prism_NEON[slopes$site==site] <- modl$coefficients[[2]]
      slopes$rsq_prism_NEON[slopes$site==site] <- rsq$r.squared[[1]]
    }


  },
error = function(e) {slopes$reg[slopes$site==site] <-NA
  })
}

slopes$diff <- slopes$prism_NEON - slopes$prism_NEON_old
slopes$rsqdiff <- slopes$rsq_prism_NEON - slopes$rsq_prism_NEON_old