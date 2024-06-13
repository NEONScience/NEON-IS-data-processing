#read in pachyderm data that has been saved to /scratch
#NA out unstable
#na out all gauges when missing
# average the 3 gauges
# average to 5 minutes
# calculated daily ranges (envelopes)
# returns single CFGLOC of daily envelopes

envelope_calc_func <- function(files, cfg){
  
  site_files <- files[grepl(files, pattern = cfg)]
  
  precip <- NEONprocIS.base::def.read.parq.ds(
    fileIn = site_files,
    Var = c('readout_time',
            'source_id',
            'site_id', 
            "strain_gauge1_depth",
            "strain_gauge2_depth",
            "strain_gauge3_depth", 
            "strain_gauge1_stability", 
            "strain_gauge2_stability",
            "strain_gauge3_stability"),
    VarTime = 'readout_time',
    RmvDupl = FALSE,
    Df = T)
  
  #not sure if it matters but pachy seems to not add source_id or site_id to the filled in data
  precip$source_id = unique(precip$source_id)[1] 
  precip$site_id = unique(precip$site_id)[1]
  
  ########################################################
  
  #set all frequency to NA if the gauge is not stable
  #set all to NA if any NA
  #average to one stream
  #QF = 1 if any strain gauge is not reading stable. 
  precipqf <- precip %>% 
    dplyr::mutate(strain_gauge1_depth =  ifelse(strain_gauge1_stability != 1,  NA_real_, strain_gauge1_depth),
                  strain_gauge1_stabQF = ifelse(strain_gauge1_stability != 1, 1, 0),
                  strain_gauge2_depth =  ifelse(strain_gauge2_stability != 1, NA_real_, strain_gauge2_depth),
                  strain_gauge2_stabQF = ifelse(strain_gauge2_stability != 1, 1, 0),
                  strain_gauge3_depth =  ifelse(strain_gauge3_stability != 1 ,  NA_real_, strain_gauge3_depth),
                  strain_gauge3_stabQF = ifelse(strain_gauge3_stability != 1, 1, 0)) %>% 
    dplyr::mutate(strain_gauge1_depth = ifelse(is.na(strain_gauge1_depth) | is.na(strain_gauge2_depth) | is.na(strain_gauge3_depth), NA_real_, strain_gauge1_depth),
                  strain_gauge2_depth = ifelse(is.na(strain_gauge1_depth) | is.na(strain_gauge2_depth) | is.na(strain_gauge3_depth), NA_real_, strain_gauge2_depth),
                  strain_gauge3_depth = ifelse(is.na(strain_gauge1_depth) | is.na(strain_gauge2_depth) | is.na(strain_gauge3_depth), NA_real_, strain_gauge3_depth)) %>% 
    dplyr::mutate(precip_depth  = rowMeans(x = cbind(strain_gauge1_depth, strain_gauge2_depth, strain_gauge3_depth), na.rm = F),
                  stabilityQF = ifelse(strain_gauge1_stabQF == 1 |strain_gauge2_stabQF == 1  | strain_gauge3_stabQF == 1 , 1, 0)) %>%
    dplyr::mutate(nullQF = ifelse(is.na(precip_depth), 1, 0)) %>%
    dplyr::select(source_id, 
                  site_id,
                  readout_time,
                  strain_gauge1_depth,
                  strain_gauge1_stabQF,
                  strain_gauge2_depth, 
                  strain_gauge2_stabQF,
                  strain_gauge3_depth,
                  strain_gauge3_stabQF,
                  precip_depth,
                  stabilityQF, 
                  nullQF) 
  
  precip_5min <- precipqf %>% 
    dplyr::mutate(avgtmi = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = '5 min')) %>% 
    dplyr::group_by(avgtmi, source_id, site_id) %>%
    dplyr::summarise(precip_depth_5min = mean(precip_depth, na.rm = T),
                     stabilityQF   = sum(stabilityQF, na.rm = T), # just doing a percent and a sum for now
                     stabilityQM = round((sum(stabilityQF, na.rm = T)/dplyr::n())*100,0),
                     nullQF = sum(nullQF, na.rm = T), # might not keep this
                     nullQM = round((sum(nullQF, na.rm = T)/dplyr::n())*100,0))
  
  #calculate envelope
  precip_daily <- precip_5min %>% 
    dplyr::mutate(dailytmi = lubridate::floor_date(as.POSIXct(avgtmi, tz = 'UTC'), unit = '1 day')) %>%
    dplyr::group_by(dailytmi, source_id, site_id) %>%
    dplyr::summarise(envelope = max(precip_depth_5min, na.rm = T) - min(precip_depth_5min, na.rm = T)) 
  
  return(precip_daily)
}

