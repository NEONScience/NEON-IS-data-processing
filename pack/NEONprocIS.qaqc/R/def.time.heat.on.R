##############################################################################################
#' @title Determine discrete periods a heater was turned on based on heater events

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Determine discrete periods a heater was turned on based on heater events.

#' @param dataHeat Data frame of heater event data as returned by NEONprocIS.base::def.read.evnt.json.R
#' At a minimum, column variables include:
#' timestamp = POSIX time of heater status
#' status = logical (TRUE=heater on)
#'  
#' @param TimeOffAuto A difftime object indicating the timeout period after which to assume the heater
#' turned off even though there is no even indicating so (e.g. base::as.difftime(30,units='mins')).
#' Default is never (NULL).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than 
#' standard R error messaging will be used.
#' 
#' @return A data frame of:
#' timeOn POSIXct Time heater turned on
#' timeOff POSIXct Time heater turned off

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' TimeOffAuto <- base::as.difftime(30,units="mins")
#' timeOnHeat <- def.heat.time.on(dataHeat,TimeOffAuto=TimeOffAuto)
#'  

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}
#' @seealso \code{\link[NEONprocIS.base]{def.read.evnt.json}}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-12-19)
#     original creation
##############################################################################################
def.time.heat.on <- function(dataHeat,
                             TimeOffAuto=NULL,
                             log=NULL
){
  
  # Initialize
  dmmyTime <- base::as.POSIXct(base::numeric(0),origin='1970-01-01',tz='GMT')
  rpt <- base::data.frame(timeOn=dmmyTime,timeOff=dmmyTime,stringsAsFactors = FALSE)
  
  # Indices of on/off events
  setHeatOn <- base::which(dataHeat$state == TRUE)
  setHeatOff <- base::which(dataHeat$state == FALSE)
  
  # No on events
  if(base::length(setHeatOn) == 0){
    # Any off events?
    if(base::length(setHeatOff) > 0){
      timeOnIdx <- base::as.POSIXct(NA)
      idxTimeOff <- setHeatOff[1]
      timeOffIdx <- dataHeat$timestamp[idxTimeOff]
      rpt <- base::rbind(rpt,base::data.frame(timeOn=timeOnIdx,timeOff=timeOffIdx,stringsAsFactors = FALSE))
    }
    return(rpt)
  }
  
  idxTimeOn <- setHeatOn[1]
  flagStop <- FALSE
  while(flagStop == FALSE){
    
    # Time heater turned on for this period
    timeOnIdx <- dataHeat$timestamp[idxTimeOn]
    
    # Time heater turned off this period
    idxTimeOff <- utils::head(x=setHeatOff[setHeatOff > idxTimeOn],n=1)
    
    # Auto-timeout apply?
    if(!base::is.null(TimeOffAuto)){
      if(base::length(idxTimeOff) > 0){
        setOnOff <- idxTimeOn:idxTimeOff # Time off recorded
      } else {
        setOnOff <- idxTimeOn:utils::tail(x=setHeatOn[setHeatOn >= idxTimeOn],n=1) # Time off not recorded
      }
    
      timeDiffOnOff <- diff(dataHeat$timestamp[setOnOff]) # Time differences between set of on & off events for this period
      idxTimeOffAuto <- utils::head(setOnOff[timeDiffOnOff > TimeOffAuto],n=1) # Past the timeout?
      if(base::length(idxTimeOffAuto) > 0){
        timeOffIdx <- dataHeat$timestamp[idxTimeOffAuto]+TimeOffAuto # Timeout applied
      } else {
        timeOffIdx <- dataHeat$timestamp[utils::tail(setOnOff,n=1)]
      }
    } else {
      # No timeout applies
      if(base::length(idxTimeOff) > 0){
        timeOffIdx <- dataHeat$timestamp[idxTimeOff] # use time of 'off' event
      } else {
        timeOffIdx <- base::as.POSIXct(NA) # Still on!
      }
    }
    
    
    # if(base::length(idxTimeOff) > 0){
    #   
    #   # Last time it was reported on before turning off
    #   #idxTimeOnLast <- utils::tail(x=setHeatOn[setHeatOn >= idxTimeOn & setHeatOn < idxTimeOff],n=1)
    #   
    #   # Auto-timeout apply?
    #   if(!base::is.null(TimeOffAuto){
    #     setOnOff <- idxTimeOn:idxTimeOff
    #     timeDiffOnOff <- diff(dataHeat$timestamp[setOnOff])
    #     idxTimeOffAuto <- utils::head(setOnOff[timeDiffOnOff > TimeOffAuto],n=1)
    #     timeOffIdx <- dataHeat$timestamp[idxTimeOffAuto]+TimeOffAuto # Timeout applied
    #   } else {
    #     timeOffIdx <- dataHeat$timestamp[idxTimeOff] # use time of 'off' event
    #   }
    # } else {
    #   
    #   # No off time recorded. 
    #   
    #   # Last time it was reported on 
    #   setOnOff <- idxTimeOn:utils::tail(x=setHeatOn[setHeatOn >= idxTimeOn],n=1)
    #   #idxTimeOnLast <- utils::tail(x=setHeatOn[setHeatOn >= idxTimeOn],n=1)
    #   
    #   # Auto-timeout apply?
    #   if(!is.null(TimeOffAuto)){
    #     timeOffIdx <- dataHeat$timestamp[idxTimeOnLast]+TimeOffAuto # Timeout applied
    #   } else {
    #     timeOffIdx <- base::as.POSIXct(NA) # Still on!
    #   }
    # }
    
    # Record
    rpt <- base::rbind(rpt,base::data.frame(timeOn=timeOnIdx,timeOff=timeOffIdx,stringsAsFactors = FALSE))
    
    # Reset beginning index
    idxTimeOn <- utils::head(x=setHeatOn[dataHeat$timestamp[setHeatOn] > timeOffIdx],n=1)
    
    if(base::length(idxTimeOn) == 0){
      flagStop=TRUE
    }
    
  } # End while loop
  
  return(rpt)

  
}
