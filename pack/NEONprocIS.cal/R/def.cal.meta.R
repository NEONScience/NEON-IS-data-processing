##############################################################################################
#' @title Compile metadata for calibrations

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read in any number of calibration files and compile their metadata, including
#' file name, certificate number, and valid date range valid date range.

#' @param fileCal Character vector of the full or relative paths to each calibration file 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of calibration metadata

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' # Not run
#' # fileCal <- c('/path/to/file1.xml','/path/to/file2.xml')
#' # metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=fileCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-04)
#     original creation
#   Cove Sturtevant (2020-12-08)
#     added path to calibration directory in output
##############################################################################################
def.cal.meta <- function(fileCal, log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  numCal <- base::length(fileCal)
  
  # Get the filenames of the calibration files without path information
  fileCalSplt <- strsplit(fileCal,'/')
  nameFileCal <- base::unlist(base::lapply(fileCalSplt,utils::tail,n=1))
  pathFileCal <- base::unlist(base::lapply(fileCalSplt,FUN=function(idxFileCalSplt){
    
    if(base::length(idxFileCalSplt) > 1){
      pathIdx <- base::paste0(
                  c(utils::head(idxFileCalSplt,n=-1),''),
                  collapse='/'
                  )
    } else {
      pathIdx <- ""
    }

  }))
  
  # intialize output
  metaCal <- base::vector(mode = "list", length = numCal)
  
  # Retrieve metadata for each calibration file
  for (idxFileCal in base::seq_len(numCal)){
    
    # Read in the cal
    infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal[idxFileCal],Vrbs=TRUE)
    
    # Error check
    if(!NEONprocIS.cal::def.validate.info.cal(infoCal,NameList=c('cal','ucrt','file','timeVali'),log=log)){
      stop()
    } else if (base::is.null(infoCal$file$StreamCalVal$CertificateNumber)) {
      log$error(base::paste0('Cannot find calibration certificate number in ',fileCal[idxFileCal],
                             '. It is not returned in infoCal$file$StreamCalVal$CertificateNumber output from NEONprocIS.cal::def.read.cal.xml'))
    stop()
    }
    
    # Look for whether this calibration is marked as suspect
    
    
    # output metadata
    metaCal[[idxFileCal]] <- base::data.frame(path=pathFileCal[idxFileCal],
                                              file=nameFileCal[idxFileCal],
                                              timeValiBgn=infoCal$timeVali$StartTime,
                                              timeValiEnd=infoCal$timeVali$EndTime,
                                              id=base::as.numeric(infoCal$file$StreamCalVal$CertificateNumber),
                                              stringsAsFactors=FALSE)
    
  }
  metaCal <- base::Reduce(f=base::rbind,x=metaCal)
  
  return(metaCal)
}
