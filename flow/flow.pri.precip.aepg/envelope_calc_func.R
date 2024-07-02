#read in pachyderm data that has been saved to /scratch
#NA out unstable
#na out all gauges when missing
# average the 3 gauges
# average to 5 minutes
# calculated daily ranges (envelopes)
# returns single CFGLOC of daily envelopes

#todo needs value change with new data for Vars
envelope_calc_func <- function(files, cfg){
  
  site_files <- files[grepl(files, pattern = CFG)]

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
  precip$source_id = unique(precip$source_id)[unique(!is.na(precip$source_id))][1] 
  precip$site_id = unique(precip$site_id)[unique(!is.na(precip$site_id))][1] 
  
  ########################################################
  
  #set all frequency to NA if the gauge is not stable
  #set all to NA if any NA
  #average to one stream
  #QF = 1 if any strain gauge is not reading stable. 
  precipqf <- precip %>% 
    dplyr::mutate(strain_gauge1_depth =  ifelse(strain_gauge1_stability != 1,  NA_real_, strain_gauge1_depth),
                  strain_gauge2_depth =  ifelse(strain_gauge2_stability != 1, NA_real_, strain_gauge2_depth),
                  strain_gauge3_depth =  ifelse(strain_gauge3_stability != 1 ,  NA_real_, strain_gauge3_depth)) %>% 
    dplyr::mutate(precip_depth  = rowMeans(x = cbind(strain_gauge1_depth, strain_gauge2_depth, strain_gauge3_depth), na.rm = F)) %>% 
    dplyr::select(source_id, 
                  site_id,
                  readout_time,
                  strain_gauge1_depth,
                  strain_gauge2_depth, 
                  strain_gauge3_depth,
                  precip_depth)

 
  #calculate envelope
  precip_daily <- precipqf %>% 
    dplyr::mutate(dailytmi = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = '1 day')) %>%
    dplyr::group_by(dailytmi, source_id, site_id) %>%
    dplyr::summarise(envelope = max(precip_depth, na.rm = T) - min(precip_depth, na.rm = T),
                     env_s1 = max(strain_gauge1_depth, na.rm = T) - min(strain_gauge1_depth, na.rm = T),
                     env_s2 = max(strain_gauge2_depth, na.rm = T) - min(strain_gauge2_depth, na.rm = T),
                     env_s3 = max(strain_gauge3_depth, na.rm = T) - min(strain_gauge3_depth, na.rm = T)) 
  
  return(precip_daily)
}

