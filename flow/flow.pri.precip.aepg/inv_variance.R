#read and rbind files at /scratch/pfs/aepg600m/tb/aepg600m/2023/10/*/16768/data
#from aepg600m_calibration_group_and_convert
#maybe try fill date gaps and regularize?

library(magrittr)
library(dplyr)

files <- list.files('/scratch/pfs/aepg600m/tb/regl/aepg600m/2023/', recursive = T, pattern = 'CFGLOC102875.*[0-9].parquet')

precip <- data.frame()
for (file in files){
  df <- NEONprocIS.base::def.read.parq(paste0('/scratch/pfs/aepg600m/tb/regl/aepg600m/2023/', file))
  precip <- rbind(precip, df)
}

#not sure if it matters but pachy seems to not add source_id or site_id to the filled in data
precip$source_id = precip$source_id[1] 
precip$site_id = precip$site_id[1]

#nevermind I just needed to go further in the pipeline! 
########################################################
#This fudges the data a bit because the inconsistent time stamps made it hard to fill in NAs for the missing times
#I thought pachyderm did this?
#Cove probably has a function
#rounding data to the 10 second then throwing out about 2000 'dupes' 

# precip$readout_time <- lubridate::round_date(precip$readout_time, unit = '10 second')
# 
# precip <- distinct(precip, readout_time, .keep_all = TRUE)
# 
# complete_time <- seq(from = min(precip$readout_time), to = max(precip$readout_time), by = "10 sec", tz = 'UTC')
#  
# # Create a new data frame with this complete sequence
# df_complete <- data.frame(readout_time = complete_time,
#                           site_id = precip$site_id[1],
#                           source_id = precip$source_id[1])
# 
# # Merge the new data frame with the original one
# precip_rgl <- df_complete %>% dplyr::left_join(precip, by = c('readout_time', 'source_id', 'site_id'))

########################################################


precip$avgtmi <-  lubridate::floor_date(as.POSIXct(precip$readout_time, tz = 'UTC'), unit = '5 min')

precip5min <- precip %>% dplyr::group_by(avgtmi, source_id, site_id) %>%
  dplyr::summarise(sensor_depth = mean(total_precipitation_depth, na.rm = T), # reported from sensor
                  gauge1_depth = mean(strain_gauge1_depth, na.rm = T),
                  gauge2_depth = mean(strain_gauge2_depth, na.rm = T),
                  gauge3_depth = mean(strain_gauge3_depth, na.rm = T),
                  stability_1PassQM = round(((length(which(strain_gauge1_stability==1)))/30)*100), 
                  stability_2PassQM = round(((length(which(strain_gauge1_stability==1)))/30)*100), 
                  stability_3PassQM = round(((length(which(strain_gauge1_stability==1)))/30)*100), 
                  stability_1NAQM = round(((length(which(is.na(strain_gauge1_stability)))+length(which(is.nan(strain_gauge1_stability))))/30)*100),
                  stability_2NAQM = round(((length(which(is.na(strain_gauge2_stability)))+length(which(is.nan(strain_gauge2_stability))))/30)*100),
                  stability_3NAQM = round(((length(which(is.na(strain_gauge3_stability)))+length(which(is.nan(strain_gauge3_stability))))/30)*100), 
                  stability_1FailQM = round(((length(which(strain_gauge1_stability==0)))/30)*100), 
                  stability_2FailQM = round(((length(which(strain_gauge2_stability==0)))/30)*100),
                  stability_3FailQM = round(((length(which(strain_gauge3_stability==0)))/30)*100)) %>% 
  #or more strain gauges are unstable for the entire averaging period then no depth information will be reported for that time interval (i.e., 𝐷̅1,2,3 = 𝑵𝑼𝑳𝑳)
  # PS when looking at grafana there are very few instances of stability = 0
  dplyr::mutate(gauge1_depth = dplyr::case_when(stability_1FailQM + stability_2FailQM + stability_3FailQM > 200 ~ NA,
                                                 TRUE ~ gauge1_depth),
                gauge2_depth = dplyr::case_when(stability_1FailQM + stability_2FailQM + stability_3FailQM > 200 ~ NA,
                                                 TRUE ~ gauge2_depth),
                gauge3_depth = dplyr::case_when(stability_1FailQM + stability_2FailQM + stability_3FailQM > 200 ~ NA,
                                                 TRUE ~ gauge3_depth),
                unstableQF = dplyr::case_when(stability_1FailQM + stability_2FailQM + stability_3FailQM > 200 ~ NA,
                                              TRUE ~ 0))

                  

                  
### calculate delta variances (adopted from dsmith r code)

# cut into 3 hour intervals according to ATBD
precip5min$interval <- cut(precip5min$avgtmi, breaks = "3 hours")

# Now you can split your data frame into a list of data frames, each representing a 3-hour chunk
list_of_df <- split(precip5min, precip5min$interval)

scaled_pcp <- data.frame()

for(i in seq_along(list_of_df)) {

df <- list_of_df[[i]]

#avg of three gauges
df$avg_depth <- apply(df[,c('gauge1_depth', 'gauge2_depth', 'gauge3_depth')], MARGIN = 1, FUN = mean)

#how many samples? 
##Determine numer of complete cases for the delta variance
d1n<-ifelse(sum(complete.cases(df$gauge1_depth))>=sum(complete.cases(df$avg_depth)),sum(complete.cases(df$avg_depth)),sum(complete.cases(df$gauge1_depth)))

d2n<-ifelse(sum(complete.cases(df$gauge2_depth))>=sum(complete.cases(df$avg_depth)),sum(complete.cases(df$avg_depth)),sum(complete.cases(df$gauge2_depth)))

d3n<-ifelse(sum(complete.cases(df$gauge3_depth))>=sum(complete.cases(df$avg_depth)),sum(complete.cases(df$avg_depth)),sum(complete.cases(df$gauge3_depth)))

#inv variance calc
deltaVar1<-(d1n-1)/(sum((df$gauge1_depth-df$avg_depth)^2,na.rm=TRUE))

deltaVar2<-(d2n-1)/(sum((df$gauge2_depth-df$avg_depth)^2,na.rm=TRUE))

deltaVar3<-(d3n-1)/(sum((df$gauge3_depth-df$avg_depth)^2,na.rm=TRUE))


totWeights<-(deltaVar1+deltaVar2+deltaVar3)

##Scale the weight for wire 1

scaledWireWeight1<-deltaVar1/totWeights

##Scale the weight for wire 2

scaledWireWeight2<-deltaVar2/totWeights

##Scale the weight for wire 3

scaledWireWeight3<-deltaVar3/totWeights

#apply to gauge depths
df$scaled_gauge1_depth <- scaledWireWeight1*df$gauge1_depth
df$scaled_gauge2_depth <- scaledWireWeight2*df$gauge2_depth
df$scaled_gauge3_depth <- scaledWireWeight3*df$gauge3_depth

#sum weighted values
df$scaled_total_depth <- apply(df[,c('scaled_gauge1_depth', 'scaled_gauge2_depth', 'scaled_gauge3_depth')], MARGIN = 1, FUN = sum)

#append 3 hr chunk and repeat
scaled_pcp <- rbind(scaled_pcp, df)
}


### next calculate lag values of the sensor vs the calculated with cal coefficients to see if they are similar. 

#need to add more testing here, just a couple visuals for now. 
# avg
# sensor
# scaled

scaled_pcp <- as.data.frame(scaled_pcp) %>% 
              arrange(avgtmi) %>% 
              mutate(avg_5min_pcp =  avg_depth - lag(avg_depth), 
                     scaled_5min_pcp = scaled_total_depth - lag(scaled_total_depth), 
                     sensor_5min_pcp = sensor_depth - lag(sensor_depth))

library(ggplot2)

ggplot(scaled_pcp, aes(x = avgtmi)) +
  geom_line(aes(y = avg_5min_pcp, color = 'Average Depth')) +
  geom_line(aes(y = sensor_5min_pcp, color = 'Sensor Depth')) +
  geom_line(aes(y = scaled_5min_pcp, color = 'Scaled Depth')) +
  ggtitle('Compare diffs of calculated depths') -> p

plotly::ggplotly(p)              
              
ggplot(scaled_pcp, aes(x = avgtmi)) +
  geom_line(aes(y = avg_depth, color = 'Average Depth')) +
  geom_line(aes(y = sensor_depth, color = 'Sensor Depth')) +
  geom_line(aes(y = scaled_total_depth, color = 'Scaled Depth')) + 
  #sensor started offset from our calc values. See what it looks like without it. 
  #scaled_pcp$sensor_depth[1]-scaled_pcp$scaled_total_depth[1] = 40.83
  geom_line(aes(y = sensor_depth - 40.83, color = 'Sensor Depth minus initial offset')) +
  ggtitle('Compare calculated depths') -> p2

plotly::ggplotly(p2)   


