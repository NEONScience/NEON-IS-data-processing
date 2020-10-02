##############################################################################################
#' @title Uncertainty for dew/frost point from the relative humidity sensor as part of the relative
#' humidity transition

#' @author
#' Edward Ayres \email{eayres@battelleecology.org}

#' @description
#' Definition function. Alternative calibration uncertainty function. Create file (dataframe) with
#' uncertainty information based off of the L0 temperature and relative humidity data values from 
#' the relative humidity sensor according to NEON.DOC.000851 - NEON Algorithm Theoretical Basis   
#' Document (ATBD): Humidity and Temperature Sensor.
#' 
#' Accepts L0 data and NEON uncertainty information as produced
#' by NEONprocIS.cal::def.read.cal.xml and returns a vector of individual measurement
#' uncertainties for each data value. The uncertainty computed is the L0 value multipled by 
#' NEON calibration uncertainty coefficient U_CVALA1. 
#' 
#'
#' @param data Temperature, relative humidity, and dew/frost point data from the relative humidity sensor 
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param varUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which uncertainty data will be computed (all other columns will be ignored). Note that for other
#' uncertainty functions this variable may not need to be in the input data frame, so long as the function
#' knows that. Defaults to the first
#' column in \code{data}.
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.ucrt.dp0p for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrtMeas} - combined measurement uncertainty for an individual dew/frost point reading. Includes the
#' repeatability and reproducibility of the sensor and the lab DAS and uncertainty of the
#' calibration procedures and coefficients including uncertainty in the standard (truth).

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual
#' NEON.DOC.000851 NEON Algorithm Theoretical Basis Document (ATBD): Humidity and Temperature Sensor

#' @keywords calibration, uncertainty, L0', hmp155, dew point, frost point

#' @examples
#' data <- data.frame(data=c(1,6,7,0,10))
#' infoCal <- list(ucrt = data.frame(Name=c('U_CVALA1','U_CVALA3'),Value=c(0.1,5),stringsAsFactors=FALSE))
#' def.ucrt.meas.mult(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.base]{def.log.init}
#' @seealso \link[NEONprocIS.cal]{wrap.ucrt.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-03)
#     original creation
#   Cove Sturtevant (2020-09-02)
#     adjusted inputs to conform to new generic format 
#     This includes inputting the entire data frame, the 
#     variable to be generate uncertainty info for, and the (unused) argument calSlct
##############################################################################################
def.ucrt.meas.rh.dew.frst.pt <- function(data = data.frame(data=base::numeric(0)),
                          infoCal = NULL,
                          varUcrt = base::names(data)[1],
                          calSlct=NULL,
                          log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Ensure input is data frame with the target variable in it
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=varUcrt, log = log) # deleted argument: ,TestEmpty=FALSE
  if (!chk) {
    stop()
  }
  
  # Check data input is numeric
  if (!NEONprocIS.base::def.validate.vector(data[[varUcrt]],TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
    stop()
  }
  
  # Initialize output data frame
  dataUcrt <- data[[varUcrt]] # Target variable to compute uncertainty for
  ucrt <- base::data.frame(ucrtMeas = NA * dataUcrt)
  
  # If infoCal is NULL, return NA data
  if(base::is.null(infoCal)){
    log$debug('No calibration information supplied, returning NA values for individual measurement uncertainty.')
    return(ucrt)
  }
  
  # Check format of infoCal
  if (!NEONprocIS.cal::def.validate.info.cal(infoCal,CoefUcrt='U_CVALA1',log=log)){
    stop()
  }
  
  # # Uncertainty coefficient U_CVALA1 represents the combined measurement uncertainty for an
  # # individual reading. It includes the repeatability and reproducibility of the sensor and the
  # # lab DAS and ii) uncertainty of the calibration procedures and coefficients including
  # # uncertainty in the standard (truth).
  # ucrtCoef <- infoCal$ucrt[infoCal$ucrt$Name == 'U_CVALA1',]
  
  # Specify constants based on relative humidity ATBD
  absZero <- -273.15
  b0 <- -0.58002206*10^4
  b1 <- 1.3914993
  b2 <- -0.048640239
  b3 <- 0.41764768 * 10^-4
  b4 <- -0.14452093 * 10^-7
  b5 <- 6.5459673
  c0 <- 0.4931358
  c1 <- -0.46094296 * 10^-2
  c2 <- 0.13746454 * 10^-4
  c3 <- -0.12743214 * 10^-7
  a0 <- -0.56745359 * 10^4
  a1 <- 6.3925247
  a2 <- -0.96778430 * 10^-2
  a3 <- 0.62215701 * 10^-6
  a4 <- 0.20747825 * 10^-8
  a5 <- -0.94840240 * 10^-12
  a6 <- 4.1635019
  
  # Calculate saturation vapor pressure 
  # Identify rows with temperature above 0 degrees C
  waterRows <- which(data$temperature > 0)
  if(length(waterRows)>0){
    # Calculate virtual temperature at temperatures above 0 degrees C
    data$virtual_temperature[waterRows] <- (data$temperature[waterRows]-absZero-
                                             (data$temperature[waterRows]*c0+
                                                data$temperature[waterRows]*c1+
                                                data$temperature[waterRows]*c2+
                                                data$temperature[waterRows]*c3) )
    
    # Calculate saturation vapor pressure over water
    data$saturation_vapor_pressure[waterRows] <- exp((b0/data$virtual_temperature[waterRows])+
                                            (data$virtual_temperature[waterRows]^0*b1+
                                               data$virtual_temperature[waterRows]^1*b2+
                                               data$virtual_temperature[waterRows]^2*b3+
                                               data$virtual_temperature[waterRows]^3*b4)+
                                            log(data$virtual_temperature[waterRows])*b5)/100
  }
  # Identify rows with temperature above 0 degrees C
  iceRows <- which(data$temperature <= 0)
  if(length(iceRows>0)){
    # Calculate virtual temperature at temperatures at or below 0 degrees C
    data$virtual_temperature[iceRows] <- data$temperature[iceRows]-absZero
    
    # Calculate saturation vapor pressure over ice
    data$saturation_vapor_pressure[iceRows] <- exp((a0/data$virtual_temperature[iceRows])+
                                            (data$virtual_temperature[iceRows]^0*a1+
                                               data$virtual_temperature[iceRows]^1*a2+
                                               data$virtual_temperature[iceRows]^2*a3+
                                               data$virtual_temperature[iceRows]^3*a4+
                                               data$virtual_temperature[iceRows]^4*a5)+
                                            log(data$virtual_temperature[iceRows])*a6)/100
  }
  
  # Calculate partial derivative (degrees C hPa-1) of ATBD Eq. 1, substituting Eq. 3 for P_pw and Eq. 5 for P_pi, with respect to P_ws_w/i
  data$derivative_dfpt_t_part1 <- 4719.72/(data$saturation_vapor_pressure*(log10(data$saturation_vapor_pressure*data$relative_humidity)-30.605)^2 )
  
  # Calculate derivative (hpa K-1) of ATBD Eq.3 or 5, substituting Eq. 4 or 6 for virtual temperature, with respect to temperature
  data$derivative_dfpt_t_part2 ...........................
  
  # Calculate derivative of dew/frost point with respect to temperature (K)
  data$derivative_dfpt_t <- data$derivative_dfpt_t_part1*data$derivative_dfpt_t_part2
  
  # Calculate partial uncertainty (degrees C) of individual dew/frost point temperature measurements with respect to ambient temperature
  data$ucrtMeas_dfpt_t <- abs(data$derivative_dfpt_t)**u_cvalA1_t
  
  # Calculate partial uncertainty (degrees C) of individual dew/frost point temperature measurements with respect to ambient relative humidity
  data$ucrtMeas_dfpt_rh <- abs(4719.72/(data$relative_humidity*(log10(data$saturation_vapor_pressure*data$relative_humidity)-30.605)^2 ))*u_cvalA1_rh

  # # Calculate the combined uncertainty for each dew/frost point measurement
  # data$ <- (data$ucrtMeas_dfpt_t^2)*(data$ucrtMeas_dfpt_rh^2)
  
  
  
  
  
  
  # Issue warning if more than one matching uncertainty coefficient was found
  if(base::nrow(ucrtCoef) > 1){
    log$warn("More than one matching uncertainty coefficient was found for U_CVALA1. Using the first.")
  }
  
  # The individual measurement uncertainty is just U_CVALA1 multiplied by each measurement
  ucrt$ucrtMeas[] <- base::as.numeric(ucrtCoef$Value[1])*dataUcrt
  
  return(ucrt)
  
}
