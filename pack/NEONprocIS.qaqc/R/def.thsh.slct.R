##############################################################################################
#' @title Determine set of applicable QA/QC thresholds for date, location, term, and context

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Given a json file of thresholds, return those that are applicable to the
#' date, term (variable), and context (all properties of the thresholds). The choice of 
#' constraint/threshold to use is determined by moving up the following hierarchy 
#' from finer to coarser constraints until one applies. Thus, the finest applicable level of constraint 
#' is chosen. Threshold selection order is as follows (1 being the finest possible contraint): 
#' 6. Realm, annual
#' 5. Realm, seasonal
#' 4. Site-specific, annual
#' 3. Site-specific, seasonal
#' 2. Sensor-spefific, annual
#' 1. Sensor-specific, seasonal

#' @param thsh List of thresholds, as returned from NEONprocIS.qaqc::def.read.thsh.qaqc.list
#' @param Time POSIXct value of the day to select thresholds for (assumes time resolution 
#' for thresholds is 1 day). Time should be at 00:00:00 GMT
#' @param Term Character value. The term for which to select thresholds for. 
#' @param Ctxt Character vector (optional) . The contexts for which to select thresholds for. Treated 
#' as an AND with \code{Term}, meaning that the thresholds are selected which match both the Term 
#' and all contexts. Defaults to NULL, in which case the criteria for threshold selection is limited
#'  to the term.
#' @param Site Character value. The NEON site code. (e.g. HARV). If NULL (default), the REALM 
#' thresholds will be selected.
#' @param NameLoc Character value. The specific named location of the sensor. If NULL (default), 
#' the REALM thresholds will be selected.
#' @param RptThsh Logical value. If TRUE, the filtered list of thresholds is output. If FALSE, the
#' indices of the selected thresholds in the input list is returned. Defaults to TRUE.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return If the RptThsh argument is TRUE, the filtered (selected) list of thresholds is output 
#' in the same format as input \code{thsh}. If RptThsh is false, the indices of the selected 
#' thresholds in the input list \code{thsh} is returned. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords quality control, quality assurance, QA/QC, QA/QC test
#' 
#' @examples
#' Currently none

#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.df}
#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.list}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-14)
#     original creation
##############################################################################################
def.thsh.slct <- function(thsh,
                          Time,
                          Term,
                          Ctxt = NULL,
                          Site=NULL,
                          NameLoc=NULL,
                          RptThsh = TRUE,
                          log = NULL) {
  #browser()
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  log$debug(base::paste0('Selecting thresholds for term: ', Term, ' and context(s): ', base::paste0(Ctxt,collapse=','),
                         ' for location: ', NameLoc, ' and date: ', Time))
  
  # Handle dates
  timeBgn <- Time
  timeEnd <-  timeBgn + base::as.difftime(1,units='days')
  timeDoy <- lubridate::yday(timeBgn) # Day of year - only need one since data encompasses only 1 day
  
  # Filter thresholds only for this term, context, location (sensor, site, or REALM), 
  # date range (absolute and seasonal). Note that seasonal logic assumes start DOY < end DOY. 
  # Discard all null entries.
  setFilt <- base::unlist(base::lapply(thsh,function(idxThsh){
    # This is one giant logical statement
      !base::is.null(idxThsh$threshold_name) && # Non-null threshold name
      idxThsh$term_name == Term && # Term
      base::sum(Ctxt %in% base::unlist(idxThsh$context)) == base::length(Ctxt) && # Context
      idxThsh$location_name %in% c(NameLoc,Site,'REALM') && # Location
      (!base::is.null(idxThsh$start_date) && idxThsh$start_date <= timeEnd) && # Hard date range, part 1
      (base::is.null(idxThsh$end_date) || idxThsh$end_date > timeBgn) && # Hard date range, part 2
      (base::is.null(idxThsh$start_day_of_year) || idxThsh$start_day_of_year <= timeDoy) && # Seasonal date range, part 1
      (base::is.null(idxThsh$end_day_of_year) || idxThsh$end_day_of_year >= timeDoy) && # Seasonal date range, part 2
      (!base::is.null(idxThsh$number_value) || !base::is.null(idxThsh$string_value)) # Threshold needs to have a value!
  }))
  
  # Error check
  if(base::length(setFilt) == 0){
    log$error(base::paste0('No thresholds match term: ', Term, ' and context(s): ', base::paste0(Ctxt,collapse=','),
                           ' for location: ', NameLoc, ' and date: ', timeBgn))
    stop()
  }
  
  # Make it pretty
  thshFilt <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(listThsh=thsh[setFilt])
  
  # Choose the correct value for each threshold. Note, this assumes that the absolute start/end dates and the 
  # seasonal start/end dates are on the daily resolution. No splitting days.
  nameThsh <- base::unique(thshFilt$threshold_name)
  setFiltUse <- base::numeric(0) # Initialize
  log$debug(base::paste0('The following thresholds were found in the threshold file. Selections will be made for each: ',base::paste0(nameThsh,collapse=',')))
  for(idxNameThsh in nameThsh){
    # Create some general text for warning messages
    txtGnl <- base::paste0('term = ',Term,', context =',base::paste0(Ctxt, collapse='|'),
                           ', threshold=',idxNameThsh,', named location=',NameLoc, ', site=', Site,'. ')
    
    # The choice of constraint to use is determined by moving up the hierarchy from finer to 
    # coarser constraints until one applies. Thus, the finest applicable level of constraint 
    # is chosen. Threshold selection order is as follows: 
    # 1. Sensor-specific, seasonal
    # 2. Sensor-spefific, annual
    # 3. Site-specific, seasonal
    # 4. Site-specific, annual
    # 5. Realm, seasonal
    # 6. Realm, annual
    
    # SENSOR & DOY-specific
    setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==NameLoc & 
                             !base::is.na(thshFilt$start_day_of_year) & !base::is.na(thshFilt$end_day_of_year))
    numUse <- base::length(setTest)
    if (numUse > 1){
      # There's a problem. There should never be more than one applicable threshold. The logic 
      # or the threshold information may be flawed.
      log$warn(base::paste0('There are ',numUse,' valid sensor & DOY specific thresholds for ',
                            txtGnl,'There is only supposed to be one. Going to take the first, ',
                            'but you should investigate...'))
      setTest <- setTest[1]
      numUse <- base::length(setTest)
    }
    if(numUse == 1){
      # Got it! Record and move on
      setFiltUse <- c(setFiltUse,setTest)
      next()
    } 
    
    # SENSOR-specific
    setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==NameLoc)
    numUse <- base::length(setTest)
    if (numUse > 1){
      # There's a problem. There should never be more than one applicable threshold. The logic 
      # or the threshold information may be flawed.
      log$warn(base::paste0('There are ',numUse,' valid sensor-specific annual thresholds for ',
                            txtGnl,'There is only supposed to be one. Going to take the first, ',
                            'but you should investigate...'))
      setTest <- setTest[1]
      numUse <- base::length(setTest)
    }
    if(numUse == 1){
      # Got it! Record and move on
      setFiltUse <- c(setFiltUse,setTest)
      next()
    }
    
    # SITE & DOY-specific
    setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==Site & 
                             !base::is.na(thshFilt$start_day_of_year) & !base::is.na(thshFilt$end_day_of_year))
    numUse <- base::length(setTest)
    if (numUse > 1){
      # There's a problem. There should never be more than one applicable threshold. The logic 
      # or the threshold information may be flawed.
      log$warn(base::paste0('There are ',numUse,' valid site & season-specific thresholds for ',
                            txtGnl,'There is only supposed to be one. Going to take the first, ',
                            'but you should investigate...'))
      setTest <- setTest[1]
      numUse <- base::length(setTest)
    }
    if(numUse == 1){
      # Got it! Record and move on
      setFiltUse <- c(setFiltUse,setTest)
      next()
    }
    
    # SITE-specific
    setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==Site)
    numUse <- base::length(setTest)
    if (numUse > 1){
      # There's a problem. There should never be more than one applicable threshold. The logic 
      # or the threshold information may be flawed.
      log$warn(base::paste0('There are ',numUse,' valid site-specific annual thresholds for ',
                            txtGnl,'There is only supposed to be one. Going to take the first, ',
                            'but you should investigate...'))
      setTest <- setTest[1]
      numUse <- base::length(setTest)
    }
    if(numUse == 1){
      # Got it! Record and move on
      setFiltUse <- c(setFiltUse,setTest)
      next()
    }
    
    # REALM & DOY-specific
    setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name=='REALM' & 
                             !base::is.na(thshFilt$start_day_of_year) & !base::is.na(thshFilt$end_day_of_year))
    numUse <- base::length(setTest)
    if (numUse > 1){
      # There's a problem. There should never be more than one applicable threshold. The logic 
      # or the threshold information may be flawed.
      log$warn(base::paste0('There are ',numUse,' valid site & season-specific thresholds for ',
                            txtGnl,'There is only supposed to be one. Going to take the first, ',
                            'but you should investigate...'))
      setTest <- setTest[1]
      numUse <- base::length(setTest)
    }
    if(numUse == 1){
      # Got it! Record and move on
      setFiltUse <- c(setFiltUse,setTest)
      next()
    }
    
    # REALM
    setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name=='REALM')
    numUse <- base::length(setTest)
    if (numUse > 1){
      # There's a problem. There should never be more than one applicable threshold. The logic 
      # or the threshold information may be flawed.
      log$warn(base::paste0('There are ',numUse,' valid REALM-level annual thresholds for ',
                            txtGnl,'There is only supposed to be one. Going to take the first, ',
                            'but you should investigate...'))
      setTest <- setTest[1]
      numUse <- base::length(setTest)
    }
    if(numUse == 1){
      # Got it! Record and move on
      setFiltUse <- c(setFiltUse,setTest)
      next()
    }
    
    # If we've reached the end without finding a threshold, error-out
    log$warn(base::paste0('No applicable thresholds found for ',txtGnl,'Downstream quality control may error.'))
    
  } # End loop around unique threshold names 
  
  # We have the set of thresholds for this term|context group, save it
  setThshSlct <- base::which(setFilt)[setFiltUse]
  
  # Compare what we ended up with in our pretty table with the original json
  cmpr <- base::all.equal(thshFilt[setFiltUse,],NEONprocIS.qaqc::def.read.thsh.qaqc.df(listThsh=thsh[setThshSlct]),check.attributes = FALSE)
  if(!base::isTRUE(cmpr)){
    log$fatal(base::paste0('Code error! Thresholds in filtered data frame do not match those from same index of JSON file. Indexing likely incorrect.')) 
    stop()
  }

  # Remove any duplicates
  setThshSlct <- base::unique(setThshSlct)
  
  # Are we returning the indices of the thresholds in the list, or the thresholds themselves?
  if(RptThsh == TRUE){
    # Return the thresholds
    rpt <- thsh[setThshSlct]
  } else {
    # Return the indices
    rpt <- setThshSlct
  }
  
  return(rpt)
}
