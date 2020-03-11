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

#' @param NameFileSUNA Filename of the SUNA data used to determine absorbance [character]
#' @param NameCalSUNA Calibration filename of the SUNA data used to determine absorbance [character]

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
def.wq.abs.corr <- function(NameFileSUNA, NameCalSUNA) {
  # Start logging, if needed
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  msg <- NULL
  
  ##### For Testing #####
  repo <- "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/water-quality-001/"
  NameFileSUNA <- "sunav2/CFGLOC23456/data/sunav2_23352_2019-01-01.avro"
  NameCalSUNA <- "sunav2/CFGLOC23456/calibration/rawNitrateSingleCompressedStream/30000000005365_WO33177_170813.xml"
  
  #Read in the SUNA data
  sunav2Data <- NEONprocIS.base::def.read.avro.deve(NameFile = base::paste0(repo,NameFileSUNA),NameLib = "/ravro.so")
  
  #Numeric Constants
  Abs_ex_start <- 351 #nm
  Abs_ex_end <- 361 #nm
  Abs_em_wav <- 480 #nm
  
  #Get rid of all the data that we don't need
  #sunav2Data <- sunav2Data[,c(3,6,17)]
  
  #Create an outputDF with timestamps of all the dark frames, which indicate the start of a burst
  outputNames <- c("readout_time","Abs_Ex","Abs_Em","spectrumCount")
  outputDF <- base::as.data.frame(base::matrix(data = NA, 
                                               nrow = base::nrow(sunav2Data[sunav2Data$header_light_frame == FALSE,]), 
                                               ncol = base::length(outputNames)))
  names(outputDF) <- outputNames
  
  outputDF$readout_time <- sunav2Data$readout_time[sunav2Data$header_light_frame == FALSE]
  
  #Get the calibration table with the wavelengths and blank intensities
  #NameCal for testing
  NameCal <- base::paste0(repo,NameCalSUNA)
  #calTable <- NEONprocIS.wq::def.pars.cal.tab.suna(calFilename = NameCal)
  calTable <- def.pars.cal.tab.suna(calFilename = NameCal)
  
  #If the cal table has more of less than 256 wavelengths the absorbance corrections shouldn't be performed
  if(base::nrow(calTable) != 256){
    outputDF$Abs_Ex <- NA
    outputDF$Abs_Em <- NA
    outputDF$spectrumCount <- 0
  }else{
    #Start calculating average absorbance values
    for(i in 1:nrow(outputDF)){
      burstStartTime <- outputDF$readout_time[i]
      if(i<nrow(outputDF)){
        burstEndTime <- outputDF$readout_time[i+1]
      }else{
        burstEndTime <- sunav2Data$readout_time[nrow(sunav2Data)]
      }
      
      burstData <- sunav2Data[sunav2Data$readout_time>burstStartTime & sunav2Data$readout_time<burstEndTime,]
      
      #According to the nitrate testing the lamp's data is best for the 10-20th frames, throw the others on the ground
      burstData <- burstData[10:20,]
      avgBurst <- def.pars.data.suna(sunaBurst = burstData$spectrum_channels)
      #Eyeball Check
      #plot(calTable$wavelength,avgBurst)
      #plot(calTable$wavelength[2:255],avgBurst)
      
      #Check the length of the SUNA burst data to make sure it's 256 wavelengths
      #If it isn't populate NAs and 0 and more on to the next burst of data
      if(base::length(avgBurst) != 256){
        outputDF$Abs_Ex[i] <- NA
        outputDF$Abs_Em[i] <- NA
        outputDF$spectrumCount[i] <- 0
        next
      }else{
        #absorbance <- base::log10(avgBurst/calTable$transmittance)
        absorbance <- base::log10(avgBurst/calTable$transmittance[2:255])
        #Eyeball Check
        #plot(calTable$wavelength,absorbance)
        #plot(calTable$wavelength[2:255],absorbance)
        
        #Looks like an exponential fit might make sense for calculating the Abs_ex and Abs_em
        
      }
      
    }
    
  return(outputDF)
  
  }
}
