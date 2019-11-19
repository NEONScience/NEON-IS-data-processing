##############################################################################################
#' @title Threshold selection module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Threshold selection module for NEON IS data processing. The choice of 
#' constraint/threshold to use is determined by moving up the following hierarchy 
#' from finer to coarser constraints until one applies. Thus, the finest applicable level of constraint 
#' is chosen. Threshold selection order is as follows (1 being the finest possible contraint): 
#' 6. Realm, annual
#' 5. Realm, seasonal
#' 4. Site-specific, annual
#' 3. Site-specific, seasonal
#' 2. Sensor-spefific, annual
#' 1. Sensor-specific, seasonal
#' 
#' This script is run at the command line with 4 or more arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the path to input data directory.
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder. This 
#' date will be used in the threshold selection process.
#' 
#' Below this path is a directory named for the location identifier of the data included 
#' within it. (e.g. #/pfs/BASE_REPO/#/yyyy/mm/dd/#/CGFLOC12345/). The location identifier will be matched 
#' against the location information supplied in the location file(s) (see below). Further nested within the 
#' location identifier folder is the folder:
#'         location/ 
#' The location folder holds at least 1 json file with location data/properties specific to the location 
#' identifier in the directory path. If there is more than one file in this directory, only the first will 
#' be used, since the properties of the named location (i.e. site) should be the same across files. 
#'    
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "FileThsh=value", where value is the full path to the thresholds file from which to select thresholds. 
#' 
#' 4-N. "TermCtxtX=value", where X is a number beginning at 1 and value is a term-context group corresponding 
#' to QA/QC thresholds for the type of data at the named location defined in the location directory. Each 
#' term-context group is a single argument, where the term is listed first followed by any applicable context 
#' strings, separated by pipes. There may be multiple assignments of TermCtxtX, specified by incrementing the 
#' number X by 1 with each additional argument. For example, a 3-argument set of term-context groups could be: 
#' "TermCtxt1=resistance|soil|deep" "TermCtxt2=windspeed|air" "TermCtxt3=temperature". In the first case, 
#' thresholds will be selected for the term "resistance" and matching both context strings "soil" and "deep". 
#' The second argument indicates selection of thresholds for term "windspeed" and matching context string 
#' "air". The third argument indicates selection of thresholds for term "temperature" without context. 
#' Thresholds will be selected for all 3 and placed in the same file. TermCtxt1 must be an input, and there is
#' a limit of X=100 for additional TermCtxtX arguments. Be sure there is no overlap between 
#' term-context groups, as you will have to reapply the logic to select appropriate thresholds for each at a 
#' later time.
#' 
#' N+1. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the calibration folder in the input path that are to be copied with a 
#' symbolic link to the output path.
#' 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return A directory structure created in DirOut, where the terminal directory of DirOut 
#' replaces BASE_REPO but otherwise retains the child directory structure of the input path. The filtered 
#' threshold file will be placed in an additional subdirectory called 'thresholds' at the same level as the 
#' location directory. Be sure to specify "DirSubCopy=location" to pass the location directory through
#' to the output (along with any other desired subdirectories to copy over. Otherwise, they will be dropped. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.thsh.slct.R "DirIn=/pfs/prt_calibration/prt/2019/01/01" "DirOut=/pfs/out" "FileThsh=/pfs/prt_soil_threshold_filter/thresholds.json" "TermCtxt1=temp|soil" "DirSubCopy=location"

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-11)
#     original creation
#   Cove Sturtevant (2019-10-01)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut","FileThsh","TermCtxt1"),
                                      NameParaOptn=c(base::paste0("TermCtxt",2:100),"DirSubCopy"),log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Thresholds file to filter/select thresholds from
FileThsh <- Para$FileThsh
log$debug(base::paste0('Threshold file to select from: ',FileThsh))

# Retrieve the term-context groups we are going to select thresholds for. 
# These are input as subsequent arguments with term and context strings separated by pipes. 
nameParaTermCtxt <- base::names(Para)[names(Para) %in% base::paste0("TermCtxt",1:100)]
numTermCtxt <- base::length(nameParaTermCtxt)
ParaThsh <- base::list()
for(idx in base::seq_len(numTermCtxt)){
  splt <- Para[[nameParaTermCtxt[idx]]]
  ParaThsh[[idx]] <- base::list(Term=splt[1],Ctxt=NULL) # The term
  numSplt <- base::length(splt)
  if(numSplt > 1){
    ParaThsh[[idx]]$Ctxt <- splt[2:numSplt]
  }
}
log$debug(base::paste0('Term-context groupings to select thresholds for: ',base::paste0(ParaThsh,collapse=",")))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,'location')))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths. We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub)

if(base::length(DirIn) == 0){
  log$warn(base::paste0('No datums found for processing in parent directory ',DirBgn))
} else {
  log$info(base::paste0('Preparing to process ',base::length(DirIn),' datum(s).'))
}

# Process each file path
for(idxDirIn in DirIn){
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Get directory listing of input directory. 
  DirLoc <- base::paste0(idxDirIn,'/location')
  fileLoc <- base::dir(DirLoc)
  numFileLoc <- base::length(DirLoc)
  
  # If there is not at least one file for locations, quit
  if(numFileLoc == 0){
    log$error(base::paste0('No location data found in ',DirLoc,'. Skipping...'))
    stop()
  }
  
  if(numFileLoc > 1){
    log$info(base::paste0('There is more than location file in ',DirLoc,'. Using ',fileLoc[1]))
    fileLoc <- fileLoc[1]
  }
  
  # Create the base output directories 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutThsh <- base::paste0(idxDirOut,'/threshold')
  base::dir.create(idxDirOutThsh,recursive=TRUE)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    base::suppressWarnings(NEONprocIS.base::def.copy.dir.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut))
    log$info(base::paste0('Unmodified subdirectories: ',base::paste0(DirSubCopy,collapse=','),' of ',idxDirIn, ' copied to ',idxDirOut))
  }  
  
  # The time frame of the data is one day, and this day is indicated in the directory structure. 
  timeBgn <-  InfoDirIn$time
  timeEnd <-  timeBgn + base::as.difftime(1,units='days')

  # Error check
  if(base::is.na(timeBgn)){
    # Generate error and stop execution
    log$error(base::paste0('Cannot interpret date from directory structure of datum path: ',idxDirIn)) 
    stop()
  }    
  timeDoy <- lubridate::yday(timeBgn) # Day of year - only need one since data encompasses only 1 day
    
  # Read the location data
  loc <- geojsonsf::geojson_sf(base::paste0(DirLoc,'/',fileLoc))
  
  # Find the location id in the locations file
  nameLoc <- utils::tail(InfoDirIn$dirSplt,1) # Location identifier from directory path
  loc <- loc[loc$name==nameLoc,]
  numLoc <- base::nrow(loc)
  if(numLoc == 0){
    log$error(base::paste0('No locations match ',nameLoc,' in location file ', fileLoc, 
                           ' as part of processing datum path: ',idxDirIn,
                           '. Cannot determine site for this named location.')) 
    stop()
  }
  
  # Grab the site
  site <- loc$site[1]
  
  # Load the thresholds & turn dates to POSIX
  thshRaw <- base::try(rjson::fromJSON(file=FileThsh,simplify=TRUE),silent=FALSE)
  if(base::class(thshRaw) == 'try-error'){
    # Generate error and stop execution
    log$error(base::paste0('Threshold file ', FileThsh, ' is unreadable or contains no data. Aborting...')) 
    stop()
  }
  thshRaw <- thshRaw$thresholds
  thsh <- base::lapply(thshRaw,function(idxThsh){
    if(!base::is.null(idxThsh$start_date)){
      idxThsh$start_date <- base::as.POSIXct(idxThsh$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
    }
    if(!base::is.null(idxThsh$end_date)){
      idxThsh$end_date <- base::as.POSIXct(idxThsh$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
    }
    return(idxThsh)
  })

  # For each term|context grouping, select the appropriate thresholds
  setThshSlct <- base::lapply(ParaThsh,function(idxParaThsh){
  
    # Filter thresholds only for this term, context, location (sensor, site, or REALM), 
    # date range (absolute and seasonal). Note that seasonal logic assumes start DOY < end DOY. 
    # Discard all null entries.
    setFilt <- base::unlist(base::lapply(thsh,function(idxThsh){
      # This is one giant logical statement
      idxThsh$term_name == idxParaThsh$Term && # Term
      base::sum(idxParaThsh$Ctxt %in% base::unlist(idxThsh$context)) == base::length(idxParaThsh$Ctxt) && # Context
      idxThsh$location_name %in% c(nameLoc,site,'REALM') && # Location
      (!base::is.null(idxThsh$start_date) && idxThsh$start_date <= timeEnd) && # Hard date range, part 1
      (base::is.null(idxThsh$end_date) || idxThsh$end_date > timeBgn) && # Hard date range, part 2
      (base::is.null(idxThsh$start_day_of_year) || idxThsh$start_day_of_year <= timeDoy) && # Seasonal date range, part 1
      (base::is.null(idxThsh$end_day_of_year) || idxThsh$end_day_of_year >= timeDoy) && # Seasonal date range, part 2
      (!base::is.null(idxThsh$number_value) || !base::is.null(idxThsh$string_value)) # Threshold needs to have a value!
    }))
    
    # Error check
    if(base::length(setFilt) == 0){
      log$error(base::paste0('No thresholds match term: ', idxParaThsh$Term, ' and context(s): ', base::paste0(idxParaThsh$Ctxt,collapse=','),
          ' for location: ', nameLoc, ' and date: ', timeBgn))
      stop()
    }
    
    # Make it pretty
    thshFilt <- NEONprocIS.qaqc::def.read.thsh.qaqc.json(listThsh=base::list(thresholds=thshRaw[setFilt]))
    
    # Choose the correct value for each threshold. Note, this assumes that the absolute start/end dates and the 
    # seasonal start/end dates are on the daily resolution. No splitting days.
    nameThsh <- base::unique(thshFilt$threshold_name)
    setFiltUse <- base::numeric(0) # Initialize
    for(idxNameThsh in nameThsh){
      # Create some general text for warning messages
      txtGnl <- base::paste0('term|context=',base::paste0(base::unlist(idxParaThsh),collapse='|'),
      ', threshold=',idxNameThsh,', named location=',nameLoc, ', site=', site,'. ')
      
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
      setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==nameLoc & 
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
      setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==nameLoc)
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
      setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==site & 
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
      setTest <- base::which(thshFilt$threshold_name==idxNameThsh & thshFilt$location_name==site)
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
      log$error(base::paste0('No applicable thresholds found for ',txtGnl,'Downstream quality control may error.'))
      
    } # End loop around unique threshold names 
    
    # We have the set of thresholds for this term|context group, save it
    idxSetThshSlct <- base::which(setFilt)[setFiltUse]
    
    # Compare what we ended up with in our pretty table with the original json
    cmpr <- base::all.equal(thshFilt[setFiltUse,],NEONprocIS.qaqc::def.read.thsh.qaqc.json(
      listThsh=base::list(thresholds=thshRaw[idxSetThshSlct])),check.attributes = FALSE)
    if(!base::isTRUE(cmpr)){
      log$fatal(base::paste0('Code error! Thresholds in filtered data frame do not match those from same index of JSON file. Indexing likely incorrect.')) 
      stop()
    }
    
    return(idxSetThshSlct)
      
  }) # End lapply around ParaThsh
  
  # Combine selected thresholds from all term|context groupings
  setThshSlct <- base::unique(base::unlist(setThshSlct))
  
  # Write the new threshold file
  fileOutThsh <- base::paste0(idxDirOutThsh,'/thresholds.json')
  thshSlct <- base::list(thresholds=thshRaw[setThshSlct])
  base::write(rjson::toJSON(thshSlct,indent=3),file=fileOutThsh)
  log$info(base::paste0('Selected thresholds written to ',fileOutThsh))
  
} # End loop around directories to process
