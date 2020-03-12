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

#' @return dataframe with L0 uncertatinty column(s) [dataframe]

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' #Written to potentially plug in line 120 of def.cal.conv.R
#' ucrt <- def.ucrt.wq.do.conc(data = data, cal = NULL)

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
##############################################################################################
def.wq.abs.corr <- function(sunav2Filenames, sunav2CalFilenames) {
  # Start logging, if needed
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  msg <- NULL
  
  #Numeric Constants
  Abs_ex_start <- 351 #nm
  Abs_ex_end <- 361 #nm
  Abs_em_wav <- 480 #nm
  
  #Read in all SUNA data in the folder since it's padded
  sunav2DataList <- try(base::lapply(sunav2Filenames,NEONprocIS.base::def.read.avro.deve,NameLib = ravroLib,log = log), silent = FALSE)
  sunav2Data <- try(do.call("rbind",sunav2DataList), silent = FALSE)
  
  timeBgn <- base::format(base::min(sunav2Data$readout_time), format = "%Y-%m-%d")
  timeEnd <- base::format(base::max(sunav2Data$readout_time), format = "%Y-%m-%d")
  
  #Read in all SUNA calibration data
  #Pull fDOM A1 uncertainty, rho_fdom, and pathlength from the appropriate cal file
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal = sunav2CalFilenames,log = log)
  # Determine the calibrations that apply for this day
  calSlct <- NEONprocIS.cal::def.cal.slct(metaCal = metaCal,
                                          TimeBgn = as.POSIXct(timeBgn, format = "%Y-%m-%d", tz = "GMT"),
                                          TimeEnd = (as.POSIXct(timeEnd, format = "%Y-%m-%d", tz = "GMT") + base::as.difftime(1, units = 'days')),
                                          log = log)
  fileCalSlct <- base::setdiff(base::unique(calSlct$file), 'NA')
  numFileCalSlct <- base::length(fileCalSlct)
  
  #Create an outputDF with timestamps of all the dark frames, which indicate the start of a burst
  outputNames <- c("readout_time","Abs_Ex","Abs_Em","ucrt_A_ex","ucrt_A_em","spectrumCount")
  outputDF <- base::as.data.frame(base::matrix(data = NA, 
                                               nrow = base::nrow(sunav2Data[sunav2Data$header_light_frame == FALSE,]), 
                                               ncol = base::length(outputNames)))
  base::names(outputDF) <- outputNames
  
  outputDF$readout_time <- sunav2Data$readout_time[sunav2Data$header_light_frame == FALSE]
  
  #Need to loop through the calibration files in case there are more than one
  for(idxCal in calSlct$file[!base::is.na(calSlct$file)]){
    NameCal <- sunav2CalFilenames[base::grepl(idxCal,sunav2CalFilenames)]
    calTable <- NEONprocIS.wq::def.pars.cal.tab.suna(calFilename = NameCal)
    
    calTimeBgn <- calSlct$timeBgn[calSlct$file == idxCal]
    calTimeEnd <- calSlct$timeEnd[calSlct$file == idxCal]
    
    outputDFIdxRange <- base::which(outputDF$readout_time >= calTimeBgn & outputDF$readout_time < calTimeEnd)
    
    #If the cal table has more of less than 256 wavelengths the absorbance corrections shouldn't be performed
    if(base::nrow(calTable) != 256){
      outputDF$Abs_Ex <- NA
      outputDF$Abs_Em <- NA
      outputDF$spectrumCount <- 0
    }else{
      #Start calculating average absorbance values
      for(i in outputDFIdxRange){
        burstStartTime <- outputDF$readout_time[i]
        if(i < base::nrow(outputDF)){
          burstEndTime <- outputDF$readout_time[i+1]
        }else{
          burstEndTime <- sunav2Data$readout_time[nrow(sunav2Data)] + base::as.difftime(1, units = 'mins')
        }
        
        burstData <- sunav2Data[sunav2Data$readout_time>burstStartTime & sunav2Data$readout_time<burstEndTime,]
        
        #According to the nitrate testing the lamp's data is best for the 10-20th frames, throw the others on the ground
        if(base::nrow(burstData) < 10){
          log$error(base::paste0("SUNA nitrate data present, but fewer than 10 light frames found for: ", base::unique(burstData$site_id), ", ", burstStartTime))
          outputDF$Abs_Ex <- NA
          outputDF$Abs_Em <- NA
          outputDF$spectrumCount <- 0
        }else{
          maxBurstIdx <- base::nrow(burstData)
          
          if(maxBurstIdx > 20){
            log$debug(base::paste0("SUNA nitrate data present, greater than 20 light frames found for: ", base::unique(burstData$site_id), ", ", burstStartTime))
          }

          burstEnd <- base::ifelse(maxBurstIdx > 20, 20, maxBurstIdx)
          burstData <- burstData[10:burstEnd,]
          avgBurst <- def.pars.data.suna(sunaBurst = burstData$spectrum_channels)
          #Eyeball Check
          #plot(calTable$wavelength,avgBurst)
          
          #Check the length of the SUNA burst data to make sure it's 256 wavelengths
          #If it isn't populate NAs and 0 and more on to the next burst of data
          if(base::length(avgBurst) != 256){
            outputDF$Abs_Ex[i] <- NA
            outputDF$Abs_Em[i] <- NA
            outputDF$spectrumCount[i] <- 0
            next
          }else{
            #absorbance <- base::log10(avgBurst/calTable$transmittance)
            absorbance <- base::log10(avgBurst/calTable$transmittance)
            #Eyeball Check
            #plot(calTable$wavelength,absorbance)
            
            outputDF$Abs_Ex[i] <- mean(absorbance[calTable$wavelength>351 & calTable$wavelength<361])
            outputDF$ucrt_A_ex[i] <- sd(absorbance[calTable$wavelength>351 & calTable$wavelength<361])
            
            #Looks like an exponential fit might make sense for calculating the Abs_ex and Abs_em
            
            #Do like what's in the ATBD
            absFit <- ""
            
            outputDF$Abs_Em[i] <- ""
            outputDF$ucrt_A_em[i] <- ""
          }
        }

      }
    }
  }
  
  return(outputDF)
  
}
