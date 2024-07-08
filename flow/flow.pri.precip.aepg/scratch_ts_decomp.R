library(dplyr)
library(lubridate)
library(TTR)


files <- list.files('/scratch/pfs/aepg600m_fill_date_gaps_and_regularize', recursive = T, full.names = T, pattern = '[0-9].parquet')

#pick one
#cfg <- "CFGLOC102875" #osbs
#cfg <- "CFGLOC112933" #WREF
#cfg <- "CFGLOC113591" #Yell
#cfg <- "CFGLOC104646" #srer
cfg <- "CFGLOC109787" #konz



#works for one site. 
site_files <- files[grepl(files, pattern = cfg)]

precip <- NEONprocIS.base::def.read.parq.ds(
  fileIn = site_files[486:507], #may 9 w padding is ~486 to 507
  Var = c('readout_time',
          'source_id',
          'site_id', 
          "strainGauge1Depth",
          "strainGauge2Depth",
          "strainGauge3Depth"),
  VarTime = 'readout_time',
  RmvDupl = FALSE,
  Df = T)


WndwAgr <- '5 min'

# Aggregate depth streams into a single depth. 
precipdf <- precip %>% dplyr::mutate(strainGaugeDepth = base::rowMeans(x=base::cbind(strainGauge1Depth, strainGauge2Depth, strainGauge3Depth), na.rm = F))  

# Do time averaging
strainGaugeDepthAgr <- precipdf %>%
  dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr)) %>%
  dplyr::mutate(endDateTime = lubridate::ceiling_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr,change_on_boundary=TRUE)) %>%
  dplyr::group_by(startDateTime,endDateTime) %>%
  dplyr::summarise(strainGaugeDepth = mean(strainGaugeDepth, na.rm = T))

nona <- strainGaugeDepthAgr[!is.na(strainGaugeDepthAgr$strainGaugeDepth),]

#288 = 1 day of 5 min data
tspcp <- ts(nona$strainGaugeDepth, start = nona$startDateTime, frequency = 288)


#daily smoothing algo
smth <- SMA(tspcp, n = 288)
plot.ts(smth)

#decomp
dc <-decompose(tspcp)
plot(dc)
plot.ts(tspcp)
plot(dc$trend)

