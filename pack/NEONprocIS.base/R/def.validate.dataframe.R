###################################################################################################
#' @title Validate data frame not empty or invalid

#' @author
#' Mija Choi \email{choim@battelleecology.org}
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Validate a data frame to check it is empty or has invalid values.
#' Returns True if not empty AND has vaild values. FALSE otherwise.

#' @param dfIn Input data frame to be validated
#' @param TestNa Boolean where TRUE results in testing for any NA values (resulting in FALSE output). Defaults to FALSE.
#' @param TestNumc Boolean where TRUE results in testing for any non-numeric values (resulting in FALSE output). Defaults to FALSE.
#' @param TestNameCol A character vector of minimum expected column names in dfIn. Defaults to zero-length character (none expected).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return Boolean. True if the input data frame is not empty AND does not have invalid values. FALSE otherwise. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-01-07)
#     original creation
#   Cove Sturtevant (2020-02-03)
#     added options for testing non-numeric values, NA values, and expected columns
#     added logging of failures
##############################################################################################

def.validate.dataframe <-
  function(dfIn,
           TestNa = FALSE,
           TestNumc = FALSE,
           TestNameCol = base::character(0),
           log = NULL) {
    # Initialize logging if necessary
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    msg <- NULL
    
    b <- TRUE
    
    if (!is.data.frame(dfIn)) {
      b <- FALSE
      log$error('Input is not a data frame.')
    }
    
    else if (nrow(dfIn) == 0) {
      b <- FALSE
      log$error('Data frame is empty (zero rows).')
      
    }
    
    else  if (TestNa == TRUE && any(is.na(dfIn))) {
      b <- FALSE
      log$error('No NA values are allowed in the data frame.')
    }
    
    else if (TestNumc == TRUE &&
             any(!unlist(lapply(dfIn, is.numeric)))) {
      b <- FALSE
      log$error('Data frame is required to be numeric.')
    } 
    
    else if (!base::all(TestNameCol %in% base::names(dfIn))){
      b <- FALSE
      log$error(base::paste0('Columns ', base::paste0(TestNameCol[!(TestNameCol %in% base::names(dfIn))],collapse=','),' are missing from the data frame.'))
    }
    
    return (b)
  }
