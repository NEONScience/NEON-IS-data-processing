##############################################################################################
#' @title Determine excitation (Abs_ex) and emission (Abs_em) correction factors for using 
#' SUNA data to correct sonde fDOM data

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Alternative calibration uncertainty function. Create file (dataframe) with 
#' uncertainty information based off of the L0 dissolved oxygen (DO) concentration data values 
#' according to NEON.DOC.004931 - NEON Algorithm Theoretical Basis Document (ATBD): Water Quality.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @param nameFile Filename of the SUNA data used to determine absorbance [character]
#' @param NameCal

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
def.wq.abs.corr <- function(NameFile, NameCal) {
  # Start logging, if needed
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  msg <- NULL
  
  #Read in the SUNA data
  sunav2Data <- NEONprocIS.base::def.read.avro.deve(NameFile = paste0(repo,sunaDataName),NameLib = "/home/NEON/kcawley/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")
  
  #Get rid of all the data that we don't need
  sunav2Data <- sunav2Data[,c(3,6,17)]
  
  #Create an outputDF with timestamps of all the dark frames, which indicate the start of a burst
  outputNames <- c("readout_time","Abs_Ex","Abs_Em")
  outputDF <- base::as.data.frame(base::matrix(data = NA, nrow = nrow(sunav2Data[sunav2Data$header_light_frame == FALSE,]), ncol = length(outputNames)))
  names(outputDF) <- outputNames
  
  outputDF$readout_time <- sunav2Data$readout_time[sunav2Data$header_light_frame == FALSE]
  
  #Get the calibration table with the wavelengths and blank intensities
  #NameCal for testing
  NameCal <- paste0(repo,calNameSUNA)
  calTable <- def.pars.cal.tab.suna(calFilename = NameCal)
  
  #Start calculating average absorbance values
  startIdx <- 1
  j <- 1
  sunaData$burstTest <- NA
  for(i in 1:nrow(darkFrames)){
    burstStartTime <- outputDF$readout_time[i]
    if(i<nrow(darkFrames)){
      burstEndTime <- outputDF$readout_time[i+1]
    }else{
      burstEndTime <- sunav2Data$readout_time[nrow(sunav2Data)]
    }
    
    burstData <- sunav2Data[sunav2Data$readout_time>burstStartTime & sunav2Data$readout_time<burstEndTime,]
    #According to the nitrate testing the lamp's data is best for the 10-20th frames, throw the others on the ground
    burstData <- burstData[10:20,]
    
    #Parse the crazy strings
    test <- burstData$spectrum_channels[114]
    strsplit(test,'\\[\\{\"spectrometer_intensity\": \\{\"int\": |\\}\\}, \\{\"spectrometer_intensity\": \\{\"int\": |\\}\\}\\]')
    
    
    sunaData$burstTest[i] <- difftime(sunaData$time[i+1],sunaData$time[i], units = "mins")
    
    if(sunaData$burstTest[i] > 1){
      sunaDataToAvg <- NA
      outputDF$startDate[j] <- format(sunaData$time[startIdx+1], format = "%Y-%m-%d %H:%M:%S")
      outputDF$endDate[j] <- format(sunaData$time[i], format = "%Y-%m-%d %H:%M:%S")
      sunaDataToAvg <- sunaData$data[(startIdx+1):i]
      sunaDataToAvg <- gsub("[0-9]{,3}:","", sunaDataToAvg)
      sunaDataToAvg <- strsplit(sunaDataToAvg, ",")
      #Fix the incomplete data frame
      if(length(sunaDataToAvg[[41]]) < 286){
        sunaDataToAvg[[41]] <- NULL
      }
      sunaDataToAvg <- data.frame(matrix(unlist(sunaDataToAvg), ncol = 286, byrow = T), stringsAsFactors = F)
      outputDF[j, 3] <- sum(!grepl("1",sunaDataToAvg[,1])) #Make sure they're all light frames, should be 0
      sunaDataToAvg[, 12:267] <- apply(sunaDataToAvg[, 12:267], 2, as.numeric)
      #Baseline correct if the last point is below zero
      baselineCorr <- 0 - log10(as.numeric(DI_scan$Reference[length(DI_scan$Reference)])/sunaDataToAvg[,267])
      baselineCorr[baselineCorr < 0] <- 0
      outputDF[j, 10] <- mean(baselineCorr, na.rm = T)
      
      #Turn the responses into absorbance values
      for(k in 12:267){
        wavelength <- names(outputDF)[k]
        trans <- as.numeric(sunaDataToAvg[,k])
        rec <- as.numeric(DI_scan$Reference[DI_scan$Wavelength == wavelength])
        sunaDataToAvg[,k] <- log10(rec/trans)
        #Add baseline correction
        sunaDataToAvg[,k] <- sunaDataToAvg[,k] + baselineCorr
        #Throw frames on the floor if between 205 and 380 and <= 0
        if(wavelength > 205 && wavelength < 350){
          if(sum(sunaDataToAvg[,k] <= 0) > 0){
            framesToDrop <- which(sunaDataToAvg[,k] <= 0)
            print("frames: ", framesToDrop, "wavelength: ", wavelength)
            sunaDataToAvg <- sunaDataToAvg[-framesToDrop,]
            #baselineCorr <- baselineCorr[-framesToDrop]
          }
        }
      }
      
      #populate the mean spectrum values
      outputDF[j, 12:267] <- apply(sunaDataToAvg[, 12:267], 2, mean)
      outputDF[j, 9] <- length(sunaDataToAvg$X1)
      startIdx <- i+1
      j <- j+1
    }
  }
  
  #Calculate the mean abs_ex
  names(outputDF)[4] <- "A_ex"
  exStart <- which(DI_scan$Wavelength == min(DI_scan$Wavelength[DI_scan$Wavelength > 351])) + 11
  exEnd <- which(DI_scan$Wavelength == max(DI_scan$Wavelength[DI_scan$Wavelength < 361])) + 11
  for(i in seq(along = outputDF$startDate)){
    outputDF$A_ex[i] <- mean(as.numeric(outputDF[i, exStart:exEnd]))
  }
  
  #Calculate the mean abs_em
  names(outputDF)[5] <- "A_em"
  names(outputDF)[6] <- "em_m"
  names(outputDF)[7] <- "em_b"
  em_wav = 480

  #Use stats::lm instead of fitting your own model with a loop
  
  for(i in seq(along = outputDF$startDate)){
    sum1 = 0
    sum2 = 0
    #sum3 = 0
    sum4 = 0
    sum5 = 0
    #Column 32 is 205.03 nm
    #Column 247 is 379.45 nm
    for(j in 32:247){
      xi <- as.numeric(names(outputDF)[j])
      yi <- as.numeric(outputDF[i,j])
      sum1 = sum1 + xi * log(yi)
      sum2 = sum2 + xi
      #sum3 = sum3 + yi
      sum4 = sum4 + xi * xi
      sum5 = sum5 + log(yi)
    }
    outputDF$em_m[i] <- (length(12:267) * sum1 - sum2 * sum5)/(length(12:267) * sum4 - sum2 * sum2)
    outputDF$em_b[i] <- (sum5 - outputDF$em_m[i] * sum2)/(length(12:267))
    outputDF$A_em[i] <- exp(outputDF$em_m[i] * em_wav + outputDF$em_b[i])
  }
  
}
