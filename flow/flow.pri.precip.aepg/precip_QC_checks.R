##some prism comps with output from smoothing function 
dirSmooth <- '/scratch/pfs/precipWeighing_combine_precip'

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE)
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.
filesFlagsAll <- list.files(path=dirSmooth,pattern='flagsSmooth.parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.
filesFlagsAll <- list.files(path=dirSmooth,pattern='flagsSmooth.parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.

sites <- stringr::str_sub(unique(unlist(stringr::str_extract_all(filesAll, pattern = '[A-Z]{4}9'))), start = 1, end = 4)

  flagChk <- as.data.frame(sites)
  flagChk$perc_flagged <- NA
  flagChk$perc_heater <- NA
  flagChk$perc_diel <- NA
  flagChk$perc_exp <- NA
  flagChk$perc_insuff <- NA

for (site in flagChk$sites[2:25]){
    tryCatch (
      {
        ptrnSite <- paste0('*/precip-weighing_',site,'*')
        filesSite <- filesAll[grepl(pattern=ptrnSite,filesAll)]
        filesFlagsSite <- filesFlagsAll[grepl(pattern=ptrnSite,filesFlagsAll)]
        
        VarKeep=c('startDateTime','endDateTime','precipBulk','precipType')
        strainGaugeDepthAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesSite,Var=VarKeep,VarTime='startDateTime',Df=TRUE)
        flagsAgr <- NEONprocIS.base::def.read.parq.ds(fileIn = filesFlagsSite,VarTime='startDateTime',Df=TRUE)
        
        #add flags to strainGaugeDepthAgr
        #consolidate flags to daily
        #remove if flagged
      strainGaugeDepthAgr <- full_join(strainGaugeDepthAgr, flagsAgr, by = c('startDateTime', 'endDateTime'))
        
        #removing data if any flagged for 3 key vars. 
        flags_sub <- strainGaugeDepthAgr[,c('insuffDataQF','extremePrecipQF','dielNoiseQF', 'heaterErrorQF')]
        flagVar <- 'finalQFTest'
        strainGaugeDepthAgr[[flagVar]] <- NA
        flag_0 <- rowSums(flags_sub == 0, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_0 == ncol(flags_sub)] <- 0
        flag_1 <- rowSums(flags_sub == 1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_1 >=1] <- 1
        flags_neg1 <- rowSums(flags_sub == -1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]]) & flags_neg1 >=1] <- -1
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]])] <- -1
        
        flags_sub <- strainGaugeDepthAgr[,c('insuffDataQF','extremePrecipQF', 'heaterErrorQF')]
        flagVar <- 'finalQFTestnoDiel'
        strainGaugeDepthAgr[[flagVar]] <- NA
        flag_0 <- rowSums(flags_sub == 0, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_0 == ncol(flags_sub)] <- 0
        flag_1 <- rowSums(flags_sub == 1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_1 >=1] <- 1
        flags_neg1 <- rowSums(flags_sub == -1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]]) & flags_neg1 >=1] <- -1
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]])] <- -1
        
        flags_sub <- strainGaugeDepthAgr[,c('insuffDataQF','extremePrecipQF', 'dielNoiseQF')]
        flagVar <- 'finalQFTestnoHeater'
        strainGaugeDepthAgr[[flagVar]] <- NA
        flag_0 <- rowSums(flags_sub == 0, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_0 == ncol(flags_sub)] <- 0
        flag_1 <- rowSums(flags_sub == 1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][flag_1 >=1] <- 1
        flags_neg1 <- rowSums(flags_sub == -1, na.rm = T)
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]]) & flags_neg1 >=1] <- -1
        strainGaugeDepthAgr[[flagVar]][is.na(strainGaugeDepthAgr[[flagVar]])] <- -1
        
        
        qc_percents <- strainGaugeDepthAgr %>%
          #prism day is 12:00 UTC DATE - 24HR so adjust time window on NEON data to make comparison
          #mutate(startDateTime = startDateTime - 12*60*60) %>%
          mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
          group_by(startDate) %>%
          summarise(dailyPrecipNEON = sum(precipBulk),
                    maxFlag = max(finalQFTest, na.rm = T),
                    maxNoDiel = max(finalQFTestnoDiel, na.rm = T),
                    maxNoHeater = max(finalQFTestnoHeater, na.rm = T),
                    HeaterFlag = max(heaterErrorQF, na.rm = T),
                    dielFlag = max(dielNoiseQF, na.rm = T),
                    exPFlag = max(extremePrecipQF, na.rm = T),
                    insuffFlag = max(insuffDataQF, na.rm = T))
        
        
        %>%
          mutate(startDate=as.Date(startDate)) %>% 
          summarise(perc_flagged = (length(which(maxFlag == -1))/dplyr::n())*100, 
                    perc_flagged_noDiel = (length(which(maxNoDiel == -1))/dplyr::n())*100, 
                    perc_flagged_noHeater = (length(which(maxNoHeater == -1))/dplyr::n())*100, 
                    perc_heater = (length(which(HeaterFlag == -1))/dplyr::n())*100, 
                    perc_diel = (length(which(dielFlag == -1))/dplyr::n())*100,
                    perc_exp =  (length(which(exPFlag == -1))/dplyr::n())*100,
                    perc_insuff = (length(which(insuffFlag == -1))/dplyr::n())*100)
        
        flagChk$perc_flagged[flagChk$site==site] <- qc_percents$perc_flagged
        flagChk$perc_flaggedNoDiel[flagChk$site==site] <- qc_percents$perc_flagged_noDiel
        flagChk$perc_flagged_noHeater[flagChk$site==site] <- qc_percents$perc_flagged_noHeater
        flagChk$perc_heater[flagChk$site==site] <- qc_percents$perc_heater
        flagChk$perc_diel[flagChk$site==site] <- qc_percents$perc_diel
        flagChk$perc_exp[flagChk$site==site] <- qc_percents$perc_exp
        flagChk$perc_insuff[flagChk$site==site] <- qc_percents$perc_insuff

  },
error = function(e) {flagChk$perc_flagged[flagChk$site==site] <-NA
  })
}
