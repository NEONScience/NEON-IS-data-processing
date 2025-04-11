library(dplyr)
library(magrittr)


dat <- NEONprocIS.base::def.read.parq('/scratch/pfs/precipWeighingv2_thresh_select_ts_pad/2025/03/29/precip-weighing-v2_HQTW900000/pluvio/CFGLOC114405/data/pluvio_CFGLOC114405_2025-03-29.parquet')

dat$sensorErrorQF <- 0
dat$heaterErrorQF <- 0

for (i in seq_along(dat$sensorErrorQF)) {
  if (is.na(dat$sensorStatus[i])) {
    dat$sensorErrorQF[i] <- -1
  } else {
      if (dat$sensorStatus[i] == 0) {
      dat$sensorStatus[i] <- 0
    }
    
    if ((dat$sensorStatus[i] / 2^6) %% 2 >= 1) { # unstable
      dat$sensorErrorQF[i] <- 1
    }
    
    if ((dat$sensorStatus[i] / 2^7) %% 2 >= 1) { # defective
    
      dat$sensorErrorQF[i] <- 1
    }
    
    if ((dat$sensorStatus[i] / 2^8) %% 2 >= 1) { # weight less minimum
      dat$sensorErrorQF[i] <- 1
    }
    
    if ((dat$sensorStatus[i] / 2^9) %% 2 >= 1) { # weight greater maximum
      dat$sensorErrorQF[i] <- 1
    }
    
    if ((dat$sensorStatus[i] / 2^10) %% 2 >= 1) { # no calibration
      dat$sensorErrorQF[i] <- 1
    }
  }
}

#heater status for bit vals of interest
for (i in seq_along(dat$heater_status)) {
  if (is.na(dat$heater_status[i])) {
    dat$sensorErrorQF[i] <- -1
  } else {
    if (dat$heater_status[i] == 0) {
      dat$heaterErrorQF[i] <- 0
    }
    if ((dat$heater_status[i] / 2^5) %% 2 >= 1) { #functional check failed
      dat$heaterErrorQF[i] <- 1
    }
    if ((dat$heater_status[i] / 2^7) %% 2 >= 1) { #heater deactivated or not present
      dat$heaterErrorQF[i] <- 1
    }
  }
}

# Do time averaging
precip5min <- dat %>%
  dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = '5 min')) %>%
  dplyr::mutate(endDateTime = lubridate::ceiling_date(as.POSIXct(readout_time, tz = 'UTC'), unit = '5 min',change_on_boundary=TRUE)) %>%
  dplyr::group_by(startDateTime,endDateTime) %>%
  dplyr::summarise(precipBulk = sum(accu_nrt, na.rm = T),
                   precipIntensityMean = mean(intensity_rt, na.rm = T), #max or min intensity?
                   numPts = length(!which(is.na(accu_nrt))), 
                   heaterErrorQFMax = max(heaterErrorQF), 
                   heaterErrorQFMin = min(heaterErrorQF),
                   sensorErrorQFMax = max(sensorErrorQF),
                   sensorErrorQFMin = min(sensorErrorQF)) 

#flag 1 if anything flagged else
#flag -1 if it is the max or minimum
precip5min$heaterErrorQF <- 0 
precip5min$heaterErrorQF[precip5min$heaterErrorQFMax == 1] <- 1
precip5min$heaterErrorQF[precip5min$heaterErrorQFMax == -1] <- -1
precip5min$heaterErrorQF[precip5min$heaterErrorQFMax == 0 & precip5min$heaterErrorQFMin== -1] <- -1

precip5min$sensorErrorQF <- 0 
precip5min$sensorErrorQF[precip5min$sensorErrorQFMax == 1] <- 1
precip5min$sensorErrorQF[precip5min$sensorErrorQFMax == -1] <- -1
precip5min$sensorErrorQF[precip5min$sensorErrorQFMax == 0 & precip5min$sensorErrorQFMin== -1] <- -1

# same logic for 30 min? 


#uncertainty?

