site = 'CPER'

##some prism comps with output from smoothing function 
dirSmooth <- '/scratch/pfs/precipWeighing_combine_precip'

# Get list of applicable data files
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE)
filesAll <- list.files(path=dirSmooth,pattern='[0-9].parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.
filesFlagsAll <- list.files(path=dirSmooth,pattern='flagsSmooth.parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.
filesFlagsAll <- list.files(path=dirSmooth,pattern='flagsSmooth.parquet',recursive=TRUE,full.names=TRUE) # Keep this second one. Needed to consistent get all years.


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
  mutate(startDateTime = startDateTime - 12*60*60) %>%
  mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
  mutate(heaterErrorQF = case_when( heaterErrorQF == -1 ~ 0,
                                    TRUE ~ heaterErrorQF)) %>% 
  group_by(startDate) %>%
  summarise(dailyPrecipNEON = sum(precipBulk),
            heaterFlag = sum(heaterErrorQF, na.rm = T)/n()) 

qc_percents %>% filter(heaterFlag > 0) %>% 
ggplot()+
  geom_histogram(aes(x = heaterFlag ))




            