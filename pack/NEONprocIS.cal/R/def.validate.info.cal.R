###################################################################################################
#' @title Validate calibration information returned by NEONprocIS.cal::def.read.cal.xml

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Validate the list elements and coefficients expected in the cal and/or ucrt data frames returned
#' from NEONprocIS.cal::def.read.cal.xml. Returns True if passes all checks. FALSE otherwise.

#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml).
#' @param NameList A character vector of names of the minimum list elements in infoCal. Defaults to 
#' "cal" and "ucrt".
#' @param CoefCal A character vector of coefficient names expected to be present in the "Name" column
#' of data frame infoCal$cal. Defaults to NULL, which will not check for any expected coefficients.
#' @param CoefUcrt A character vector of coefficient names expected to be present in the "Name" column
#' of data frame infoCal$ucrt. Defaults to NULL, which will not check for any expected coefficients.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return Boolean. True if passes all checks. FALSE otherwise. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2010-02-04)
#     initial creation
##############################################################################################

def.validate.info.cal <- function(infoCal, 
                                  NameList=c('cal','ucrt'), 
                                  CoefCal=NULL, 
                                  CoefUcrt=NULL, 
                                  log=NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  a <- TRUE

  if (!base::is.list(infoCal) || (base::is.data.frame(infoCal))) {
    a <- FALSE
    log$error('Input "infoCal" must be a list.')
    
  } else if (!base::all(NameList %in% base::names(infoCal)) ) {
    a <- FALSE
    log$error(base::paste0('Input "infoCal" must contain (at a minimum) list elements ',base::paste0(NameList,collapse=',')))
    
  } else if ('cal' %in% NameList && !base::is.data.frame(infoCal$cal)) {
    a <- FALSE
    log$error('List element infoCal$cal must be a data frame.')
    
  } else if ('ucrt' %in% NameList && !base::is.data.frame(infoCal$ucrt)) {
    a <- FALSE
    log$error('List element infoCal$ucrt must be a data frame.')
    
  } else if ('cal' %in% NameList && !base::all(c('Name','Value') %in% base::names(infoCal$cal))) {
    a <- FALSE
    log$error('The data frame contained in "infoCal$cal" must have columns "Name" and "Value"')

  } else if ('ucrt' %in% NameList && !base::all(c('Name','Value') %in% base::names(infoCal$ucrt))) {
    a <- FALSE
    log$error('The data frame contained in "infoCal$ucrt" must have columns "Name" and "Value"')
    
  } else if (!base::is.null(CoefCal) && !base::all(CoefCal %in% infoCal$cal$Name)) {
    a <- FALSE
    log$error(base::paste0('Missing at least one expected coefficient (',base::paste0(CoefCal,collapse=','),') in "infoCal$cal"'))
  
  } else if (!base::is.null(CoefUcrt) && !base::all(CoefUcrt %in% infoCal$ucrt$Name)) {
    a <- FALSE
    log$error(base::paste0('Missing at least one expected coefficient (',base::paste0(CoefUcrt,collapse=','),') in "infoCal$ucrt"'))
    
  }

  return (a)
}
