#read and rbind files at /scratch/pfs/aepg600m/tb/aepg600m/2023/10/*/16768/data
#from aepg600m_calibration_group_and_convert
#maybe try fill date gaps and regularize?

library(magrittr)
library(dplyr)

#pick a site
#OSBS
files <- list.files('/scratch/pfs/aepg600m/tb/regl/aepg600m/2023/', recursive = T, pattern = 'CFGLOC102875.*[0-9].parquet')

#SRER 
#files <- list.files('/scratch/pfs/aepg600m/tb/regl/aepg600m/2023/', recursive = T, pattern = 'CFGLOC104646.*[0-9].parquet')


## THESE SITES REQUIRE line 40-60

#PUUM 
#files <- list.files('/scratch/pfs/aepg600m/tb/aepg600m/2023/', recursive = T, pattern = '46912.*[0-9].parquet')

#GUAN
#files <- list.files('/scratch/pfs/aepg600m/tb/aepg600m/2023/', recursive = T, pattern = '46911.*[0-9].parquet')

#TALL
#files <- list.files('/scratch/pfs/aepg600m/tb/aepg600m/2023/', recursive = T, pattern = '26991.*[0-9].parquet')


#ready in pachy files
precip <- data.frame()
for (file in files){
  df <- tryCatch (
    {NEONprocIS.base::def.read.parq(paste0('/scratch/pfs/aepg600m/tb/aepg600m/2023/', file))},
    error = function(e) { NEONprocIS.base::def.read.parq(paste0('/scratch/pfs/aepg600m/tb/regl/aepg600m/2023/', file))}
  )
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
#rounding data to the 10 second then throwing out some 'dupes' 
if (precip$site_id[1] %in% c('PUUM', 'TALL', 'GUAN')){
  precip$readout_time <- lubridate::round_date(precip$readout_time, unit = '10 second')
  
  precip <- distinct(precip, readout_time, .keep_all = TRUE)
  
  complete_time <- seq(from = min(precip$readout_time), to = max(precip$readout_time), by = "10 sec", tz = 'UTC')
  
  # Create a new data frame with this complete sequence
  df_complete <- data.frame(readout_time = complete_time,
                            site_id = precip$site_id[1],
                            source_id = precip$source_id[1])
  
  # Merge the new data frame with the original one
  precip <- df_complete %>% dplyr::left_join(precip, by = c('readout_time', 'source_id', 'site_id'))
}

########################################################

#set all frequencies to NA if the gauge is not stable, set all to NA if any NA, average to one stream

preciptst <- precip[1:300,]

preciptst$strain_gauge1_depth[1:10]<- NA

preciptst <- preciptst %>% mutate(strain_gauge1_depth = dplyr::case_when(strain_gauge1_stability != 1 ~ NA_real_,
                                                                   TRUE ~ strain_gauge1_depth),
                            strain_gauge2_depth = dplyr::case_when(strain_gauge2_stability != 1~ NA_real_,
                                                                   TRUE ~ strain_gauge2_depth),
                            strain_gauge3_depth = dplyr::case_when(strain_gauge3_stability != 1 ~ NA_real_,
                                                                   TRUE ~ strain_gauge3_depth)) %>% 
                      mutate(strain_gauge1_depth = case_when(sum(is.na(c(strain_gauge1_depth, strain_gauge2_depth, strain_gauge3_depth))) >= 1 ~ NA_real_,
                                                                     TRUE ~ strain_gauge1_depth),
                             strain_gauge2_depth = case_when(sum(is.na(c(strain_gauge1_depth, strain_gauge2_depth, strain_gauge3_depth))) >= 1 ~ NA_real_,
                                                                     TRUE ~ strain_gauge2_depth),
                             strain_gauge3_depth = case_when(sum(is.na(c(strain_gauge1_depth, strain_gauge2_depth, strain_gauge3_depth))) >= 1 ~ NA_real_,
                                                                     TRUE ~ strain_gauge3_depth))


df$scaled_total_depth <- apply(df[,c('scaled_gauge1_depth', 'scaled_gauge2_depth', 'scaled_gauge3_depth')], MARGIN = 1, FUN = sum, na.rm = T)

#NA flags next                             
                                     