##############################################################################################
#' @title Uncertainty for dew/frost point from the relative humidity sensor as part of the relative
#' humidity transition

#' @author
#' Edward Ayres \email{eayres@battelleecology.org}

#' @description
#' Definition function. Alternative calibration uncertainty function. Create file (dataframe) with
#' uncertainty information based off of the L0 temperature, relative humidity, and dew/frost point data values from 
#' the relative humidity sensor according to NEON.DOC.000851 - NEON Algorithm Theoretical Basis   
#' Document (ATBD): Humidity and Temperature Sensor.
#' 
#' Accepts L0 data and NEON uncertainty information as produced
#' by NEONprocIS.cal::def.read.cal.xml and returns a vector of individual measurement
#' uncertainties for each data value. 
#' 
#'
#' @param data Temperature, relative humidity, and dew/frost point data from the relative humidity sensor 
#' @param infoCal Not used in this function \cr
#' @param varUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which uncertainty data will be computed (dew_point in this function).
#' @param calSlct Defaults to NULL. See the inputs to NEONprocIS.cal::wrap.ucrt.dp0p for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrtMeas_dew_point} - combined measurement uncertainty for an individual dew/frost point reading. Includes the
#' repeatability and reproducibility of the sensor and the lab DAS and uncertainty of the
#' calibration procedures and coefficients including uncertainty in the standard (truth).

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual
#' NEON.DOC.000851 NEON Algorithm Theoretical Basis Document (ATBD): Humidity and Temperature Sensor

#' @keywords calibration, uncertainty, L0', hmp155, dew point, frost point

#' @examples
#' data <- data.frame(relative_humidity=c(1,6,7,0,10), temperature=c(2,3,6,8,5), dew_pont=c(1,-1,5,4,4.5))
#' calSlct=list("temperature"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),
#' timeEnd=as.POSIXct("2019-01-02",tz="GMT"),file = "30000000000080_WO29705_157555.xml",id = 157555, expi= FALSE),
#' "relative_humidity"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),
#' timeEnd=as.POSIXct("2019-01-02",tz="GMT"),file = "30000000000080_WO29705_157554.xml",id = 157554, expi= FALSE),
#' "dew_point"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),timeEnd=as.POSIXct("2019-01-02",tz="GMT"),
#' file = "30000000000080_WO29705_157556.xml",id = 157556, expi= FALSE))

#' def.ucrt.meas.rh.dew.frst.pt(data=data,calSlct=calSlct)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.base]{def.log.init}
#' @seealso \link[NEONprocIS.cal]{wrap.ucrt.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Edward Ayres (2020-10-07)
#     original creation
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
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=c('readout_time',varUcrt),TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Check data input is numeric
  if (!NEONprocIS.base::def.validate.vector(data[[varUcrt]],TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
    stop()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  # Initialize output data frame
  dataUcrt <- data[[varUcrt]] # Target variable to compute uncertainty for
  ucrt <- base::data.frame(ucrtMeas = NA * dataUcrt)
  
  # Specify constants based on relative humidity ATBD
  absZero <- -273.15 # (degrees C)
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
  data$virtual_temperature <- NA*data$temperature
  data$saturation_vapor_pressure <- data$virtual_temperature
  data$derivative_dfpt_t_part1 <- data$virtual_temperature
  data$derivative_dfpt_t_part2 <- data$virtual_temperature
  # Identify rows with temperature above 0 degrees C
  waterRows <- which(data$temperature > 0)
  if(length(waterRows)>0){
    # Calculate virtual temperature at temperatures above 0 degrees C
    data$virtual_temperature[waterRows] <- ((data$temperature[waterRows]-absZero)-
                                              ((data$temperature[waterRows]-absZero)*c0+
                                                 (data$temperature[waterRows]-absZero)*c1+
                                                 (data$temperature[waterRows]-absZero)*c2+
                                                 (data$temperature[waterRows]-absZero)*c3) )
    
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
  # Do this for temperatures >273.15 K
  if(length(waterRows)>0){
    data$derivative_dfpt_t_part2[waterRows] <- 1/100*
      (1-(0*c0*(data$temperature[waterRows]-absZero)^-1+
            1*c1*(data$temperature[waterRows]-absZero)^0+
            2*c2*(data$temperature[waterRows]-absZero)^1+
            3*c3*(data$temperature[waterRows]-absZero)^2))*
      (-(b0/data$virtual_temperature[waterRows]^2)+
         1*b2*data$virtual_temperature[waterRows]^0+ 
         2*b3*data$virtual_temperature[waterRows]^1+ 
         3*b4*data$virtual_temperature[waterRows]^2+ 
         b5/data$virtual_temperature[waterRows])*
      (exp((b0/data$virtual_temperature[waterRows])+
             b1*data$virtual_temperature[waterRows]^0+
             b2*data$virtual_temperature[waterRows]^1+
             b3*data$virtual_temperature[waterRows]^2+
             b4*data$virtual_temperature[waterRows]^3+
             b5*log(data$virtual_temperature[waterRows])))
  }
  # Do this for temperatures <=273.15 K
  if(length(iceRows)>0){
    data$derivative_dfpt_t_part2[iceRows] <- 1/100*
      ((-a0/(data$temperature[iceRows]-absZero)^2+
          a2+
          2*a3*(data$temperature[iceRows]-absZero)+
          3*a4*(data$temperature[iceRows]-absZero)^2+
          4*a5*(data$temperature[iceRows]-absZero)^3+
          (a6/(data$temperature[iceRows]-absZero)))*
         exp(a0/(data$temperature[iceRows]-absZero)+
               a1*(data$temperature[iceRows]-absZero)^0+
               a2*(data$temperature[iceRows]-absZero)^1+
               a3*(data$temperature[iceRows]-absZero)^2+
               a4*(data$temperature[iceRows]-absZero)^3+
               a5*(data$temperature[iceRows]-absZero)^4+
               a6*log(data$temperature[iceRows]-absZero)))
  }
  
  # Calculate derivative of dew/frost point with respect to temperature (K)
  data$derivative_dfpt_t <- data$derivative_dfpt_t_part1*data$derivative_dfpt_t_part2
  
  
  
  # Roll through the temperature and RH calibration files, applying the computations for the applicable time period(s)
  calSlctTemp <- calSlct$temperature
  for(idxRowTemp in base::seq_len(base::nrow(calSlctTemp))){
    
    # What points in the output correspond to this row?
    setCalTemp <- timeMeas >= calSlctTemp$timeBgn[idxRowTemp] & timeMeas < calSlctTemp$timeEnd[idxRowTemp]
    
    # Move on if no data points fall within this cal window
    if(base::sum(setCalTemp) == 0){
      next
    }
    
    # If a calibration file is available for this period, open it and get calibration information
    if(!base::is.na(calSlctTemp$file[idxRowTemp])){
      fileCal <- base::paste0(calSlctTemp$path[idxRowTemp],calSlctTemp$file[idxRowTemp])
      infoCalTemp <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
    } else {
      log$debug('No temperature calibration information supplied for at least a period of the data, returning NA values for individual measurement uncertainty during that interval.')
      next
    }
    
    # Check format of infoCalTemp
    if (!NEONprocIS.cal::def.validate.info.cal(infoCalTemp,CoefUcrt='U_CVALA1',log=log)){
      stop()
    }
    
    
    # Now roll through the relative humidity calibrations, applying the computations for the applicable time period(s)
    calSlctRh <- calSlct$relative_humidity
    for(idxRowRh in base::seq_len(base::nrow(calSlctRh))){
      
      # What points in the output correspond to this row?
      setCalRh <- timeMeas >= calSlctRh$timeBgn[idxRowRh] & timeMeas < calSlctRh$timeEnd[idxRowRh]
      
      # Merge with setCalTemp to find the data points applicable to both the temp and RH calibration files
      setCal <- setCalTemp & setCalRh
      
      # Move on if no data points fall within this joint cal window
      if(base::sum(setCal) == 0){
        next
      }
      
      # If a calibration file is available for this period, open it and get calibration information
      if(!base::is.na(calSlctRh$file[idxRowRh])){
        fileCal <- base::paste0(calSlctRh$path[idxRowRh],calSlctRh$file[idxRowRh])
        infoCalRh <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
      } else {
        log$debug('No relative humidity calibration information supplied for at least a period of the data, returning NA values for individual measurement uncertainty during that interval.')
        next
      }
      
      # Check format of infoCalRh
      if (!NEONprocIS.cal::def.validate.info.cal(infoCalRh,CoefUcrt='U_CVALA1',log=log)){
        stop()
      }
      
      # Uncertainty coefficient U_CVALA1 represents the combined measurement uncertainty for an
      # individual reading. It includes the repeatability and reproducibility of the sensor and the
      # lab DAS and ii) uncertainty of the calibration procedures and coefficients including
      # uncertainty in the standard (truth).
      # Get the uncertainty coefficients for temperature and relative humidity
      ucrtCoefTemp <- infoCalTemp$ucrt[infoCalTemp$ucrt$Name == 'U_CVALA1',]
      ucrtCoefRh <- infoCalRh$ucrt[infoCalRh$ucrt$Name == 'U_CVALA1',]
      
      # Issue warning if more than one matching uncertainty coefficient was found
      if(base::nrow(ucrtCoefTemp) > 1){
        log$warn("More than one matching uncertainty coefficient was found for temperature U_CVALA1. Using the first.")
      }
      if(base::nrow(ucrtCoefRh) > 1){
        log$warn("More than one matching uncertainty coefficient was found for relative humidity U_CVALA1. Using the first.")
      }
      
      # Calculate partial uncertainty (degrees C) of individual dew/frost point temperature measurements with respect to ambient temperature
      ucrt_dfpt_t <- abs(data$derivative_dfpt_t[setCal])*base::as.numeric(ucrtCoefTemp$Value[1])
      
      # Calculate partial uncertainty (degrees C) of individual dew/frost point temperature measurements with respect to ambient relative humidity
      ucrt_dfpt_rh <- abs(4719.72/(data$relative_humidity[setCal]*(log10(data$saturation_vapor_pressure[setCal]*data$relative_humidity[setCal])-30.605)^2 ))*base::as.numeric(ucrtCoefRh$Value[1])
      
      # Calculate the combined uncertainty for each dew/frost point measurement
      ucrt$ucrtMeas[setCal] <- sqrt((ucrt_dfpt_t^2)+(ucrt_dfpt_rh^2))
      
      
    } # End loop around RH calibration files
    
  } # End loop around Temperature calibration files

  
  return(ucrt)
  
}
