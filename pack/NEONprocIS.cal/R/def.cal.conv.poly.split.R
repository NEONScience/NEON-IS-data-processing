##############################################################################################
#' @title Convert raw to calibrated data using NEON polynomial calibration coefficients that are split based on input range

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration polynomial function contained in coefficients 
#' CVALM0, CVALM1, CVALM2, CVALH0, CVALH1, CVALH2 etc. to convert raw data to calibrated data.

#' @param data Numeric data frame of raw measurements. 
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml.
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients.
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALM[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case converted data will be retured as NA.
#' @param varConv A character string of the target variable (column) in the data frame \code{data} for 
#' which the calibration will be applied (all other columns will be ignored). Note that for other
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
#' infoCal <- list(cal=data.frame(Name=c('CVALH0','CVALH1','CVALH2','CVALM0','CVALM1','CVALM2'),Value=c(-0.48,0.97,-0.000001,0.11,1,-0.00024),stringsAsFactors=FALSE))
#' def.cal.conv.poly.split(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv}

#' @export

# changelog and author contributions / copyrights
#   Nora Catolico (2023-03-02)
#     original creation, from def.cal.conv.poly.m by Cove Sturtevant
#
##############################################################################################
def.cal.conv.poly.split <- function(data = data.frame(data=base::numeric(0)),
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

  # Construct the polynomial calibration function
  func1 <-
    NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALH', log = log)
  func2 <-
    NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALM', log = log)
  
  # Convert data using the calibration function based on range
  for(i in 1:length(data[[varConv]])){
    if(data[[varConv]][i]>100){
      dataConv_i<-stats::predict(object = func1, newdata = data[[varConv]][i])
    }else{
      dataConv_i<-stats::predict(object = func1, newdata = data[[varConv]][i])
    }
    if(i==1){
      dataConv<-dataConv_i
    }else{
      dataConv<-append(dataConv,dataConv_i)
    }
  }
  
  return(dataConv)
  
}
