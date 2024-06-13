library(magrittr)

files <- list.files('/scratch/pfs/aepg600m_fill_date_gaps_and_regularize', recursive = T, full.names = T, pattern = '[0-9].parquet')

# Regular expression to match CFGLOC##### pattern
pattern <- "CFGLOC\\d{6}"  

# Apply the function to each filename and store results in a vector
cfgs <- lapply(files, stringr::str_extract, pattern)

# Remove missing values (NA)
cfgs <- unlist(unique(cfgs))

#works for one site. 
site_files <- files[grepl(files, pattern = cfgs[1])]
  
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

      new <- Sys.time() - old # calculate difference
      print(new) # print in nice format
      
      #not sure if it matters but pachy seems to not add source_id or site_id to the filled in data
      precip$source_id = unique(precip$source_id)[1] 
      precip$site_id = unique(precip$site_id)[1]
      
      ########################################################
      
      #set all frequency to NA if the gauge is not stable
      #set all to NA if any NA
      #average to one stream
      #QF = 1 if any strain gauge is not reading stable. 
      
      old <- Sys.time()
      precipqf <- precip %>% 
                   dplyr::mutate(strain_gauge1_depth =  ifelse(strain_gauge1_stability != 1,  NA_real_, strain_gauge1_depth),
                          strain_gauge1_stabQF = ifelse(strain_gauge1_stability != 1, 1, 0),
                          strain_gauge2_depth =  ifelse(strain_gauge2_stability != 1, NA_real_, strain_gauge2_depth),
                          strain_gauge2_stabQF = ifelse(strain_gauge2_stability != 1, 1, 0),
                          strain_gauge3_depth =  ifelse(strain_gauge3_stability != 1 ,  NA_real_, strain_gauge3_depth),
                          strain_gauge3_stabQF = ifelse(strain_gauge3_stability != 1, 1, 0)) %>% 
                    # dplyr::mutate(strain_gauge1_depth = ifelse(is.na(strain_gauge1_depth) | is.na(strain_gauge2_depth) | is.na(strain_gauge3_depth), NA_real_, strain_gauge1_depth),
                    #        strain_gauge2_depth = ifelse(is.na(strain_gauge1_depth) | is.na(strain_gauge2_depth) | is.na(strain_gauge3_depth), NA_real_, strain_gauge2_depth),
                    #        strain_gauge3_depth = ifelse(is.na(strain_gauge1_depth) | is.na(strain_gauge2_depth) | is.na(strain_gauge3_depth), NA_real_, strain_gauge3_depth)) %>% 
                    dplyr::mutate(precip_depth  = rowMeans(x = cbind(strain_gauge1_depth, strain_gauge2_depth, strain_gauge3_depth), na.rm = F),
                           stabilityQF = ifelse(strain_gauge1_stabQF == 1 |strain_gauge2_stabQF == 1  | strain_gauge3_stabQF == 1 , 1, 0)) %>%
                    # dplyr::mutate(nullQF = ifelse(is.na(precip_depth), 1, 0)) %>%
                    dplyr::select(source_id, 
                           site_id,
                           readout_time,
                           precip_depth,
                           stabilityQF) 
### Max Diff across 3 gauges (range) may be a quality flag (eg TALL gauges sept 2023 as example)      
### heater streams 
### inlet temps vs orifice temp
### orifice heater flag

      
      
      

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
      
                                                               
