##############################################################################################
#' @title Create calibration polynomial function from NEON CVAL coefficients

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Creates a polynomical object of the calibration function from NEON 
#' calibration coefficients (from e.g. NEONprocIS.cal::def.read.cal.xml).

#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$cal, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A polynomial (model) object of the polynomial calibration function

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' infoCal <- list(cal=data.frame(Name=c('CVALA1','CVALA0'),Value=c(10,1),stringsAsFactors=FALSE))
#' def.cal.func.poly(infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-01-31)
#     original creation
##############################################################################################
def.cal.func.poly <- function(infoCal, log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Validate calibration information
  if (!NEONprocIS.base::def.validate.info.cal (infoCal,NameList='cal',log=log)) {
    stop()
  }
  
  # Reduce cal coefficients to ones we recognize
  cal <- infoCal$cal
  cal <- cal[grep('CVALA[0-9]', cal$Name), ]
  
  # Pull out the numeric polynomial level of each coefficient (a0,a1,a2,...)
  levlPoly <-
    base::as.numeric(base::unlist(base::lapply(
      base::strsplit(cal$Name, 'CVALA', fixed = TRUE),
      FUN = function(vect) {
        vect[2]
      }
    )))
  
  # Initialize vector of polynomial coefficients (a0,a1,a2,...)
  coefPoly <- base::rep(0, base::max(levlPoly) + 1)
  
  # Place the polynomial coefficients in the right place in our coefficient vector
  coefPoly[levlPoly + 1] <- base::as.numeric(cal$Value)
  
  # Create polynomial function from coefficients
  func <- polynom::polynomial(coef = coefPoly)
  
  # Return the polynomial object
  return(func)
  
}
