##############################################################################################
#' @title Convert CVALA0 to CVALA1 and to a 1 degree polynomial 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Definition function. Apply polynomial to data. If CVAL uses A0 when it should be A1, it needs conversion to
#' level 1 polynomial before calculating. A0 is traditionally an offset, but in the calibration it 
#' is treated as the multiplier. (eg #bucket tips* A0 = mm precip)

#' @param data Numeric data frame of raw measurements. 
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml.
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients.
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALA[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case converted data will be retured as NA.
#' @param varConv A character string of the target variable (column) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Note that for other
#' uncertainty functions this variable may not need to be in the input data frame. Defaults to the first
#' column in \code{data}.
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A  Numeric vector of calibrated data\cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' data=data.frame(data=c(1,2,3))
#' infoCal <- data.frame(Name=c('CVALA0'),Value=c(.4985),stringsAsFactors=FALSE)
#' def.cal.conv.poly.tip(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.m}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv}

#' @export

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-06-12)
#    Original creation

##############################################################################################
def.cal.conv.poly.a0.as.a1 <- function(data = data.frame(data=base::numeric(0)),
                              infoCal = NULL,
                              varConv = base::names(data)[1],
                              calSlct=NULL,
                              log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Ensure input is data frame
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=varConv,TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Check to see if data to be calibrated is a numeric array
  chk <-
    NEONprocIS.base::def.validate.vector(data[[varConv]], TestEmpty = FALSE, TestNumc = TRUE, log = log)
  if (!chk) {
    stop()
  }
  
  # If infoCal is NULL, return NA data
  if (is.null(infoCal)) {
    log$warn('No calibration information supplied, returning NA values for converted data.')
    dataConv <- NA * data[[varConv]]
    return(dataConv)
  } else {
    # Check to see if infoCal is a list
    chkList <-
      NEONprocIS.base::def.validate.list(infoCal, log = log)
    if (!chkList) {
      chk <- c(chk, chkList)
    }
    
  }
  
  # Validate calibration information
  if (!NEONprocIS.cal::def.validate.info.cal(infoCal,NameList='cal',log=log)) {
    stop()
  }
  
  #add 0 level to coefficient list to place A0 at A1 position
  coefPoly <- c(0, infoCal$cal$Value[infoCal$cal$Name == 'CVALA0'])
  
  # Construct the polynomial calibration function
  func <- polynom::polynomial(coef = coefPoly)
  
  # Convert data using the calibration function
  dataConv <- stats::predict(object = func, newdata = data[[varConv]]) # Bucket tip converted to precip. 

  return(dataConv)
  
}
