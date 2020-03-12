##############################################################################################
#' @title Parse SUNA cal table using calibration filename

#' @author
#' Kaelin M. Cawley \email{kcawley@battelleecology.org}

#' @description
#' Definition function. Given a calibration filename, the code parses the calibration table 
#' if it exists. Some older SUNA files do not have a calibration table. As of the development 
#' of this function no other sensors, besides the SUNA, have a calibraiton table.

#' @param calFilename The filename assiciated with the desired calibration table [character]
#' @param calTableName The calibration table name, defaults to "CVALTABLEA1" [character]
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of \cr
#' \code{wavelength} units = nanometer. \cr
#' \code{transmittance} units = unitless. \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples
#' # TBD

#' @export

# changelog and author contributions / copyrights
#   Kaelin M. Cawley (2020-02-28)
#     original creation
##############################################################################################
def.pars.cal.tab.suna <-
  function(calFilename = NULL,
           calTableName = "CVALTABLEA1",
           log = NULL) {
    # Intialize logging if needed
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    # Read in the calibration file
    calFile <-
      NEONprocIS.cal::def.read.cal.xml(NameFile = calFilename, Vrbs = TRUE)
    
    # Parse out the calibration table from the file
    calTable <-
      calFile$file$StreamCalVal$CalibrationTable[calFile$file$StreamCalVal$CalibrationTable$.attrs == calTableName]
    
    calStartIdx <- min(which(names(calTable)=="Row"))
    calEndIdx <- max(which(names(calTable)=="Row"))
    
    wavelength <- calTable[calStartIdx]$Row$Independent$text
    transmittance <- calTable[calStartIdx]$Row$Column$Dependent$text
    outputDF <-
      base::data.frame(wavelength, transmittance, stringsAsFactors = FALSE)
    
    for (i in (calStartIdx+1):calEndIdx) {
      wavelength <- calTable[i]$Row$Independent$text
      transmittance <- calTable[i]$Row$Column$Dependent$text
      newRows <-
        base::data.frame(wavelength, transmittance, stringsAsFactors = FALSE)
      outputDF <- base::rbind(outputDF, newRows)
    }
    
    outputDF$wavelength <- base::as.numeric(outputDF$wavelength)
    outputDF$transmittance <- base::as.numeric(outputDF$transmittance)
    #Eyeball Check
    #plot(outputDF$wavelength,outputDF$transmittance)
    
    return(outputDF)
  }
