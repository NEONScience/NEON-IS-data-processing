##############################################################################################
#' @title Apply SRF records to data in a publication table

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Apply the science review flag to the data in a publication table (i.e. L1+).
#' Application of the science review flag involves:
#' 1) Populating the SRF field/column in the publication table with the SRF value in each SRF record for the time
#'    interval over which it applies,
#' 2) Forcing the final quality flag to 1 if the value of the SRF is 1 or 2. Note that a SRF value of 0 indicates
#' that a previous manual flag was removed and the final quality flag is computed normally, 
#' 3) Setting terms/columns to be redacted to NA if the SRF value is 2. The terms to redact are indicated in the 
#' publication workbook - the redactionFlag column for these terms is populated with the measurement stream 
#' of the SRF. 

#' @param srf Data frame of science review flag records to apply, as produced by NEONprocIS.pub::def.read.srf
#' with column qfFinl added by applying def.srf.qf.finl. Note that the srf records should already be filtered 
#' for those corresponding to the location/product instance of the data in dataTabl 
#' (i.e. (site, domain, HOR, VER).
#' 
#' @param dataTabl Data frame of data for a single publication workbook table. Note that the column names in the data frame
#' should match those in the pub workbook (need not be ordered the same).
#' 
#' @param pubWbTabl Data frame of the publication workbook filtered for the publication table corresponding to dataTabl.
#' 
#' @param NameVarTimeBgn Character string. The name of the time variable in dataTabl (and pubWbTabl) indicating 
#' the start time of the aggregation interval. Default is 'startDateTime'.
#' 
#' @param NameVarTimeEnd Character string. The name of the time variable in dataTabl (and pubWbTabl) indicating 
#' the end time of the aggregation interval. If the data are instantaneous output, set NameVarTimeEnd to the same variable as 
#' NameVarTimeBgn. If any part of the aggregation interval falls within the time range of the SRF, 
#' the SRF will be applied. Note that the aggregation end time and SRF end time are exclusive, meaning they are not considered 
#' part of the interval. Default is 'endDateTime'. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame with the same format and size of dataTabl, with applicable SRF actions applied.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none
#' NOT RUN.
#' Add the qfFinl column to the SRF table
#' srf$qfFinl <- NEONprocIS.pub::def.srf.term.qf.finl(termSrf=srf$srf_term_name,log=log)

#' @seealso \link[NEONprocIS.pub]{def.read.srf}
#' @seealso \link[NEONprocIS.pub]{def.read.pub.wb}
#' @seealso \link[NEONprocIS.pub]{def.srf.term.qf.finl}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-02-22)
#     original creation
##############################################################################################
def.srf.aply <- function(srf,
                         dataTabl,
                         pubWbTabl,
                         NameVarTimeBgn='startDateTime',
                         NameVarTimeEnd='endDateTime',
                         log = NULL
  ) {
    
  # Initialize log if not input
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Wildcard the DOM, SITE, HOR and VER in the SRF measurement stream name. Assume these apply (i.e. filtering of srf records
  # must occur prior to executing this function).
  nameVarTabl <- pubWbTabl$fieldName
  idDp <- srf$measurement_stream_name
  base::substr(idDp,start=6,stop=8) <- 'DOM'
  base::substr(idDp,start=10,stop=13) <- 'SITE'
  base::substr(idDp,start=35,stop=37) <- 'HOR'
  base::substr(idDp,start=39,stop=41) <- 'VER'
    
  setSrf <- idDp %in% pubWbTabl$DPNumber | idDp %in% pubWbTabl$redactionFlag # Which SRFs apply to data in this pub workbook
  numSrfAply <- base::sum(setSrf)
  if(numSrfAply == 0){
    log$warn('No SRF records are relevant to the publication table. Returning same data table as input.')
  } 
  
  for(idxSrf in base::which(setSrf)){
    
    # Populate the SRF for the applicable time period
    nameVarSrf <- pubWbTabl$fieldName[pubWbTabl$DPNumber == idDp[idxSrf]] # SRF term name
    setQf <- dataTabl[[NameVarTimeEnd]] >= srf$start_date[idxSrf] & dataTabl[[NameVarTimeBgn]] < srf$end_date[idxSrf] # Rows to flag
    numRcd <- base::sum(setQf)
    dataTabl[setQf,nameVarSrf] <- srf$srf[idxSrf] # Populate flag
    log$debug(base::paste0('Populated ',numRcd, ' timestamps for SRF term ',nameVarSrf, ' with SRF value ', srf$srf[idxSrf]))
    
    # Adjust the final quality flag
    if(srf$srf[idxSrf] %in% c(1,2) && srf$qfFinl[idxSrf] %in% nameVarTabl){
      dataTabl[setQf,srf$qfFinl[idxSrf]] <- 1
      log$debug(base::paste0('Final quality flag ',srf$qfFinl[idxSrf], ' set to 1 for ', numRcd,  ' timestamps.'))
      
    }
    
    # Redact applicable terms
    if(srf$srf[idxSrf] == 2){
      nameVarRdct <- pubWbTabl$fieldName[pubWbTabl$redactionFlag == idDp[idxSrf]] # variables to redact
      dataTabl[setQf,nameVarRdct] <- NA # Redact
      log$debug(base::paste0(base::length(nameVarRdct), ' terms redacted for ', numRcd,  ' timestamps.'))
    }
  }
  
  return(dataTabl)
}
