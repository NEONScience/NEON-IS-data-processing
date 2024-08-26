#testing wrap precip smooth
DirOutBase = '/scratch/pfs/out_tb/'

env <- readr::read_csv( file = '/scratch/pfs/aepg600m_fill_date_gaps_and_regularize/envelope_threshold.csv')
env$DirIn <- NA
env$Date <- NA 
env$Envelope <- NA 
env$ThshCountHour <- NA
env$Quant <- NA
env$RangeSizeHour <- NA
env$Notes <- NA

ThshChange = 0.2
ChangeFactor = 1
Recharge = 25
ChangeFactorEvap = 0.5

#Yell (envelope is a guess here)
#messy messy

#DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_YELL900000/aepg600m_heated/CFGLOC113591/'
#DirIn<- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_WREF900000/aepg600m_heated/CFGLOC112933/'
#DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2023/08/30/precip-weighing_WOOD900000/aepg600m_heated/CFGLOC107003/'
#DirIn <-'/scratch/pfs/precipWeighing_ts_pad_smoother/2023/08/30/precip-weighing_UNDE900000/aepg600m_heated/CFGLOC107634/'
#DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_TOOL900000/aepg600m_heated/CFGLOC106786/'
#DirIn <-'/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_SJER900000/aepg600m_heated/CFGLOC113350/'
#DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2023/08/30/precip-weighing_SRER900000/aepg600m/CFGLOC104646/'
#DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_SCBI900000/aepg600m_heated/CFGLOC103160/'
DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2023/05/09/precip-weighing_REDB900000/aepg600m_heated/CFGLOC112599/'


  
Envelope <- 4
ThshCountHour  <- 6#how long for increased value to be considered precip
Quant  <- 0.5 #quantile for bench baseline
RangeSizeHour <-24 #num hours for i:currRow
WndwAgr <- "5 min"

# #WREF
# # talk with Cove about reseting volume counter. High env takes chop away
# env$DirIn[env$site == 'WREF'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_WREF900000/aepg600m_heated/CFGLOC112933/'
# env$Date[env$site == 'WREF']  <- '05-09-2024'
# env$Envelope[env$site == 'WREF']  <- 2
# env$ThshCountHour[env$site == 'WREF']  <- 6 #how long for increased value to be considered precip
# env$Quant[env$site == 'WREF']  <- 0.8 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'WREF']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'WREF']  <- 'Defaults good, but the reset of the counter for volume makes it choppier than necessary (may 4-7)'
# 
# #WOOD 
# 
# env$DirIn[env$site == 'WOOD'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_WOOD900000/aepg600m_heated/CFGLOC107003/'
# env$Date[env$site == 'WOOD']  <- '05-09-2024'
# env$Envelope[env$site == 'WOOD']  <- 0.9
# env$ThshCountHour[env$site == 'WOOD']  <- 18 #how long for increased value to be considered precip
# env$Quant[env$site == 'WOOD']  <- 0.8 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'WOOD']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'WOOD']  <- 'counter choppy, couple small bumps with 6hr and 12hr on may 18. Maybe real rain'
# 
# 
# #UNDE 
# env$DirIn[env$site == 'UNDE'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_UNDE900000/aepg600m_heated/CFGLOC107634/'
# env$Date[env$site == 'UNDE']  <- '05-09-2024'
# env$Envelope[env$site == 'UNDE']  <- 0.9
# env$ThshCountHour[env$site == 'UNDE']  <- 18 
# env$Quant[env$site == 'UNDE']  <- 0.8 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'UNDE']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'UNDE']  <- ' needed large threshold for count to get it to stop walking up the diel pattern briefly'
# 
# #TOOL envelope wrong, TOOL has higher noise
# env$DirIn[env$site == 'TOOL'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_TOOL900000/aepg600m_heated/CFGLOC106786/'
# env$Date[env$site == 'TOOL']  <- '05-09-2024'
# env$Envelope[env$site == 'TOOL']  <- 3
# env$ThshCountHour[env$site == 'TOOL']  <- 12 
# env$Quant[env$site == 'TOOL']  <- 0.8 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'TOOL']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'TOOL']  <- 'very noisy, will likely need flag after correction, original envelope too small'
# 
# #TALL
# env$DirIn[env$site == 'TALL'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_TALL900000/aepg600m/CFGLOC108877/'
# env$Date[env$site == 'TALL']  <- '05-09-2024'
# env$Envelope[env$site == 'TALL']  <- 3
# env$ThshCountHour[env$site == 'TALL']  <- 12 
# env$Quant[env$site == 'TALL']  <- 0.8 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'TALL']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'TALL']  <- 'lots of gaps, this data had some strange noise, looks pretty good with 12hr'
# 
# 
# #SRER
# env$DirIn[env$site == 'SRER'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_SRER900000/aepg600m/CFGLOC104646/'
# env$Date[env$site == 'SRER']  <- '05-09-2024'
# env$Envelope[env$site == 'SRER']  <- 0.4
# env$ThshCountHour[env$site == 'SRER']  <- 18
# env$Quant[env$site == 'SRER']  <- 0.5 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'SRER']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'SRER']  <- 'small envelope, small quantile. longer thshcount seems to do better job of catching whole precip event. Reset is sensitive to arid sites'
# 
# readr::write_csv(env, file = '~/threshold_test_tb.csv')
# 
# #SJER super noisy too! 
# 
# env$DirIn[env$site == 'SJER'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_SJER900000/aepg600m/CFGLOC113350/'
# env$Date[env$site == 'SJER']  <- '05-09-2024'
# env$Envelope[env$site == 'SJER']  <- 2.0
# env$ThshCountHour[env$site == 'SJER']  <- 18
# env$Quant[env$site == 'SJER']  <- 0.8 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'SJER']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'SJER']  <- 'small envelope, small quantile. longer thshcount seems to do better job of catching whole precip event. Reset is sensitive to arid sites'
# 
# 
# #SCBI 
# 
# env$DirIn[env$site == 'SCBI'] <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_SCBI900000/aepg600m_heated/CFGLOC103160/'
# env$Date[env$site == 'SCBI']  <- '05-09-2024'
# env$Envelope[env$site == 'SCBI']  <- 3 #adjusted up
# env$ThshCountHour[env$site == 'SCBI']  <- 12
# env$Quant[env$site == 'SCBI']  <- 0.85 #quantile for bench baseline
# env$RangeSizeHour[env$site == 'SCBI']  <- 24 #num hours for i:currRow
# env$Notes[env$site == 'SCBI']  <- 'May 5 rain ends high and delays capture of precip happening late May 06, hard to adjust, needed envelope boost.'
# 
# 
# 
# DirIn <-   '/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_SCBI900000/aepg600m_heated/CFGLOC103160/'
# ThshCountHour <-6
# Quant <- 0.8
# RangeSizeHour <- 24
# 
# #wouldn't let me write to ts pad smoother?
# readr::write_csv(env, file = '~/threshold_test_tb.csv')
