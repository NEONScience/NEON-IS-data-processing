##############################################################################################
#' @title Read NEON calibration XML file

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read in a NEON calibration XML file.

#' @param NameFile String. Name (including relative or absolute path) of calibration file.
#' @param Vrbs (Optional) Logical. If TRUE, returns the full contents of the calibration file as an additional output.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

 
#' @return A named list:\cr
#' timeVali = a list of POSIXct valid start & end date-times for the calibration 
#' cal = a data frame of calibration coefficients
#' ucrt = a data frame of uncertainty coefficients
#' file = a list of the contents of the full calibration file. Only returned if Vrbs = TRUE.

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-02-25)
#     original creation
#   Mija Choi (2020-03-03)
#     Added xml validation
#   Mija Choi (2020-03-25)
#     Modified to add a read-only file, inst/extdata/calibration.xsd, in NEONprocIS.cal package 
#   Cove Sturtevant (2020-09-21)
#     added logging
##############################################################################################
def.read.cal.xml <- function(NameFile,Vrbs=TRUE,log=NULL){
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Check the file against the expected schema. THis issues logs only, because
  # although a file may fail a schema check, it may still have the info we need.
  xsd1 <- system.file("extdata", "calibration.xsd", package = "NEONprocIS.cal")
  xmlchk <- try(NEONprocIS.base::def.validate.xml.schema(NameFile, xsd1),silent = TRUE)
  
  # Read contents of xml file 
  xml <- try(XML::xmlParse(NameFile),silent=TRUE) 
  if(class(xml)[1] == "try-error") {
    log$error(base::paste0("Calibration XML file: ",NameFile," does not exist or is unreadable"))
    base::stop()
  }
  
  # XML file as a list
  listXml <- XML::xmlToList(xml)
  
  # Grab valid date range
  timeVali <- base::lapply(listXml$ValidTimeRange,as.POSIXct,format='%Y-%m-%dT%H:%M:%OS',tz="GMT")
  
  # Grab calibration coefficients and turn into a data frame
  idxCal <- base::which(base::names(listXml$StreamCalVal) == "CalibrationCoefficient")
  coefCal <- base::Reduce(base::rbind,lapply(listXml$StreamCalVal[idxCal],
                                          base::as.data.frame,stringsAsFactors=FALSE))

  # Grab uncertainty coefficients and turn into a data frame
  idxUcrt <- base::which(base::names(listXml$StreamCalVal) == "Uncertainty")
  coefUcrt <- base::Reduce(base::rbind,lapply(listXml$StreamCalVal[idxUcrt],
                                          base::as.data.frame,stringsAsFactors=FALSE))

  if (Vrbs == TRUE){
    rpt <- base::list(timeVali=timeVali,cal=coefCal,ucrt=coefUcrt,file=listXml)
  } else {
    rpt <- base::list(timeVali=timeVali,cal=coefCal,ucrt=coefUcrt)
  }
  
  return(rpt)
}
