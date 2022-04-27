##############################################################################################
#' @title Determine excitation (Abs_ex) and emission (Abs_em) correction factors for using
#' SUNA data to correct sonde fDOM data. Also populates spectrumCount.

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Alternative calibration uncertainty function. Create file (dataframe) with
#' uncertainty information based off of the L0 dissolved oxygen (DO) concentration data values
#' according to NEON.DOC.004931 - NEON Algorithm Theoretical Basis Document (ATBD): Water Quality.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @param sunav2Filenames SUNA data filenames used to determine absorbance [character]
#' @param sunav2CalFilenames Calibration filenames for the SUNA data used to determine absorbance [character]
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return dataframe with L0 uncertatinty column(s) [dataframe]

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @export

#' @keywords Currently none

#' @examples
#' # TBD

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
#   Kaelin Cawley (2020-09-15)
#     updated for reading parquet files
##############################################################################################
def.wq.abs.corr <-
  function(sunav2Filenames,
           sunav2CalFilenames,
           log = NULL) {
    # Start logging, if needed
    if (is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    msg <- NULL
    
    # Numeric Constants
    Abs_ex_start <- 351 #nm
    Abs_ex_end <- 361 #nm
    Abs_em_wav <- 480 #nm
    
    #Read in all SUNA data in the folder since it's padded
    sunav2DataList <-
      base::lapply(
        sunav2Filenames,
        NEONprocIS.base::def.read.parq,
        log = log
      )
    sunav2Data <-
      dplyr::bind_rows(sunav2DataList)

    #Clean up NA SUNA data
    sunav2Data <- sunav2Data[!is.na(sunav2Data$header_light_frame), ]
    
    timeBgn <-
      base::format(base::min(sunav2Data$readout_time, na.rm = TRUE), format = "%Y-%m-%d")
    timeEnd <-
      base::format(base::max(sunav2Data$readout_time, na.rm = TRUE), format = "%Y-%m-%d")
    
    #Read in all SUNA calibration data
    #Pull fDOM A1 uncertainty, rho_fdom, and pathlength from the appropriate cal file
    metaCal <-
      NEONprocIS.cal::def.cal.meta(fileCal = sunav2CalFilenames, log = log)
    # Determine the calibrations that apply for this day
    calSlct <- NEONprocIS.cal::def.cal.slct(
      metaCal = metaCal,
      TimeBgn = as.POSIXct(timeBgn, format = "%Y-%m-%d", tz = "GMT"),
      TimeEnd = (
        as.POSIXct(timeEnd, format = "%Y-%m-%d", tz = "GMT") + base::as.difftime(1, units = 'days')
      ),
      log = log
    )
    fileCalSlct <- base::setdiff(base::unique(calSlct$file), 'NA')
    numFileCalSlct <- base::length(fileCalSlct)
    
    #Create an outputDF with timestamps of all the dark frames, which indicate the start of a burst
    outputNames <-
      c("readout_time",
        "Abs_ex",
        "Abs_em",
        "ucrt_A_ex",
        "ucrt_A_em",
        "spectrumCount",
        "fDOMAbsQF")
    outputDF <- base::as.data.frame(base::matrix(
      data = NA,
      nrow = base::nrow(sunav2Data[sunav2Data$header_light_frame == FALSE,]),
      ncol = base::length(outputNames)
    ))
    base::names(outputDF) <- outputNames
    
    #Default values for fDOMAbsQF and spectrumCount
    outputDF$fDOMAbsQF <- -1
    outputDF$spectrumCount <- 0
    
    outputDF$readout_time <-
      sunav2Data$readout_time[sunav2Data$header_light_frame == FALSE]
    
    #Need to loop through the calibration files in case there are more than one
    for (idxCal in calSlct$file[!base::is.na(calSlct$file)]) {
      NameCal <-
        sunav2CalFilenames[base::grepl(idxCal, sunav2CalFilenames)]
      calTable <-
        try(NEONprocIS.wq::def.pars.cal.tab.suna(calFilename = NameCal, log = log))
      
      # Fail the transition now when these can't be read, but might just want to keep going if they all seem to be older files
      if (base::class(calTable) == 'try-error') {
        # outputDF$Abs_ex <- NA
        # outputDF$Abs_em <- NA
        # outputDF$spectrumCount <- 0
        log$error(base::paste0('File: ', NameCal, ' is unreadable.'))
        stop()
      }
      
      calTimeBgn <- calSlct$timeBgn[calSlct$file == idxCal]
      calTimeEnd <- calSlct$timeEnd[calSlct$file == idxCal]
      
      outputDFIdxRange <-
        base::which(outputDF$readout_time >= calTimeBgn &
                      outputDF$readout_time < calTimeEnd)
      
      #If the cal table has more or less than 256 wavelengths the absorbance corrections shouldn't be performed
      if (base::nrow(calTable) != 256) {
        # outputDF$Abs_ex <- NA
        # outputDF$Abs_em <- NA
        # outputDF$spectrumCount <- 0
        log$error(base::paste0(
          'File: ',
          NameCal,
          ' has more or less than 256 rows in the cal table.'
        ))
        stop()
      } else{
        #Start calculating average absorbance values
        for (i in outputDFIdxRange) {
          burstStartTime <- outputDF$readout_time[i]
          if (i < base::nrow(outputDF)) {
            burstEndTime <- outputDF$readout_time[i + 1]
          } else{
            burstEndTime <-
              sunav2Data$readout_time[nrow(sunav2Data)] + base::as.difftime(1, units = 'mins')
          }
          
          burstData <-
            sunav2Data[sunav2Data$readout_time > burstStartTime &
                         sunav2Data$readout_time < burstEndTime,]
          
          #According to the nitrate testing the lamp's data is best for the 10-20th frames, throw the others on the ground
          if (base::nrow(burstData) < 10) {
            log$error(
              base::paste0(
                "SUNA nitrate data present, but fewer than 10 light frames found for: ",
                base::unique(burstData$site_id),
                ", ",
                burstStartTime
              )
            )
            outputDF$Abs_ex[i] <- NA
            outputDF$Abs_em[i] <- NA
            outputDF$spectrumCount[i] <- 0
          } else{
            maxBurstIdx <- base::nrow(burstData)
            
            if (maxBurstIdx > 20) {
              log$debug(
                base::paste0(
                  "SUNA nitrate data present, greater than 20 light frames found for: ",
                  base::unique(burstData$site_id),
                  ", ",
                  burstStartTime
                )
              )
            }
            
            burstEnd <-
              base::ifelse(maxBurstIdx > 20, 20, maxBurstIdx)
            burstData <- burstData[10:burstEnd,]
            outputDF$spectrumCount[i] <- burstEnd - (10 - 1)
            avgBurst <-
              NEONprocIS.wq::def.pars.data.suna(sunaBurst = burstData$spectrum_channels,
                                                log = log)
            #Eyeball Check
            #plot(calTable$wavelength,avgBurst)
            
            #Check the length of the SUNA burst data to make sure it's 256 wavelengths
            #If it isn't populate NAs and 0 and more on to the next burst of data
            if (base::length(avgBurst) != 256) {
              outputDF$Abs_ex[i] <- NA
              outputDF$Abs_em[i] <- NA
              outputDF$spectrumCount[i] <- 0
              log$info(base::paste0("Length of the SUNA burst data is not 256, it is ",base::length(avgBurst)))
              next
            } else{
              # Warning when there are negative data
              if (any(avgBurst < 0) |
                  any(calTable$transmittance < 0)) {
                log$debug(
                  base::paste0(
                    "Absorbance could not be calculated due to negative values."
                  )
                )
                outputDF$Abs_ex[i] <- NA
                outputDF$Abs_em[i] <- NA
                outputDF$spectrumCount[i] <- 0
              }
              absorbance <-
                base::log10(avgBurst / calTable$transmittance)
              absorbance[absorbance == -Inf] <- 0
              #Eyeball Check
              #plot(calTable$wavelength,absorbance)
              
              outputDF$Abs_ex[i] <-
                base::mean(absorbance[calTable$wavelength > 351 &
                                        calTable$wavelength < 361])
              outputDF$ucrt_A_ex[i] <-
                stats::sd(absorbance[calTable$wavelength > 351 &
                                       calTable$wavelength < 361])
              
              test <- log10(absorbance)
              #Eyeball Check
              #plot(calTable$wavelength,test)
              
              #Perfomr calculations to extrapolate to 480 nm
              log_abs <- try(base::log10(absorbance), silent = TRUE)
              log_waves <-
                try(base::log10(calTable$wavelength), silent = TRUE)
              abs_model <-
                try(stats::lm(log_abs[!is.nan(log_abs) &
                                        log_abs != -Inf] ~ log_waves[!is.nan(log_abs) &
                                                                       log_abs != -Inf]))
              
              if (base::class(abs_model) == 'try-error') {
                log$error(base::paste0('Linear fit for extrapolating to 480 nm failed'))
                stop()
              }
              
              slope <- abs_model$coefficients[2]
              intercept <- abs_model$coefficients[1]
              #Eyeball Check
              #plot(log_waves[!is.nan(log_abs)&log_abs!=-Inf],log_abs[!is.nan(log_abs)&log_abs!=-Inf])
              #lines(log_waves[!is.nan(log_abs)&log_abs!=-Inf], (slope*log_waves[!is.nan(log_abs)&log_abs!=-Inf]+intercept), col = "blue")
              #plot(calTable$wavelength, absorbance)
              #lines(calTable$wavelength, absorbance, col = "red")
              
              outputDF$Abs_em[i] <-
                10 ^ (slope * log10(480) + intercept)
              outputDF$ucrt_A_em[i] <-
                stats::sd(abs_model$residuals)
            }
          }
          
        }
      }
    }
    
    #Update flags and correction factors for high and low values
    
    #High values...
    outputDF$fDOMAbsQF[outputDF$Abs_ex > 0.6] <- 2
    
    #Low values...
    outputDF$Abs_ex[outputDF$Abs_ex <= 0] <- 0
    outputDF$Abs_em[outputDF$Abs_ex <= 0] <- 0
    outputDF$fDOMAbsQF[outputDF$Abs_ex <= 0] <- 3
    
    #Update flags for intermediate values
    outputDF$fDOMAbsQF[outputDF$Abs_ex > 0 & outputDF$Abs_ex <= 0.6] <- 0
    
    return(outputDF)
    
  }
