##############################################################################################
#' @title Merge the temp chain stats and quality metrics files with location data contents of multiple files (avro or parquet) that share a common time variable.

#' @author
#' Guy Litt \email{glitt@battelleecology.org}

#' @description
#' Definition function. Merge the contents of multiple tchain parquet data files
#'  that share a common timestamp and time interval. Combine with location information
#'  on depth and convert datasets from wide to long format.
#'  
#'  
#' variable. Other than the time variable, the files should different columns. If any duplicate 
#' column names are found, only the first instance found will be retained. Any missing
#' timestamps among the files will be filled with NA values.

#' @details
#' Steps in this do-all function:
#' 1. identify location file & load in
#' 2. identify QM and stat files for e/ time interval
#' 3. Combine QM and stat files for e/ time interval
#' 4. Assign location to the combined QM & stat dataframe
#' 
#' 
#' # Example files that exist as \code{basename(file)}
#' "CFGLOC110702.json"
#' "tchain_32610_locations.json" 
#' "tchain_CFGLOC110702_2019-01-10_qualityMetrics_001.parquet"
#' "tchain_CFGLOC110702_2019-01-10_qualityMetrics_030.parquet"
#' "tchain_CFGLOC110702_2019-01-10_basicStats_001.parquet" 
#' "tchain_CFGLOC110702_2019-01-10_basicStats_030.parquet"  
#' #Example Files used for matching the '001' time interval:
# "CFGLOC110702.json"
# "tchain_CFGLOC110702_2019-01-10_qualityMetrics_001.parquet"
# "tchain_CFGLOC110702_2019-01-10_basicStats_001.parquet" 
#' 
#' 
#' @param file Character vector of full or relative file paths. Must be avro or parquet format.
#' @param nameVarTime Character value. The name(s) of the time variable common across QM and stats files. E.g. c("001", "030")
#' @param mrgeCols Character vector. The column names for merging stat and QM files.
#'  Default \code{c("startDateTime", "endDateTime")}
#' @param locDir The expected directory name containing the location file(s). Default "location"
#' @param statDir The expected directory name containing the basicStats file. Default "stats"
#' @param qmDir The expected directory name containing the qualityMetrics file. Default "quality_metrics"
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log.
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A list of each time interval containing a data frame with the merged file contents.

#' @references Currently none

#' @keywords Currently none

#' @examples
#' # Not run
#' file <- c('/path/to/qmDir/qmfile.avro','/path/to/statDir/statfile.avro', 'path/to/locDir/locfile.json)
#' NEONprocIS.base::def.file.comb.ts(file=file,nameVarTime=c('001','030')




#' @export

# TODO add SuspectCal to pub wb?
# TODO remove ConsistencyFail/Pass/NAQM from pub wb?
# TODO add tsdWaterTempFinalQFSciRvw to dataset?
# TODO depth11 doesn't exist yet for Mean/Minimum/Maximum/Variance stats (probably changes once CVAL files change)


# changelog and author contributions / copyrights
#   Guy Litt (2021-03-22)
#     original creation

##############################################################################################
def.file.comb.tsdl.long <- function(file,
                                    nameVarTime, 
                                    mrgeCols = c("startDateTime", "endDateTime"),
                                    locDir = "location",
                                    statDir = "stats",
                                    qmDir = "quality_metrics",
                                    log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # ========================================================================= #
  #  ==================== LOAD Depth location file =========================  #
  #   =====================================================================   #
  locFilePths <- file[base::which(base::basename(base::dirname(file)) == locDir)]
  
  # Turns out it doesn't matter which location file is used - both have same format under $features
  if(base::length(locFilePths) > 1){
    log$info(base::paste0("Multiple location files exist. Using the first location file, ", locFilePths[1]))
  }
 
  locFile <- locFilePths[1]
  
  if(base::length(locFile) != 1){
    log$error(base::paste0("Could not find a location file in provided files: ",
                           base::paste(file, collapse = ", ")))
    stop()
  }
  
  if(NEONprocIS.base::def.validate.json(locFile)){
    
    
    locData <- try(rjson::fromJSON(file = locFile))
    
    if(base::any(base::class(locData) == 'try-error')){
      log$error(base::paste0('File ', locFile,' is unreadable.')) 
      stop()
    } else {
      log$debug(base::paste0('Successfully read in file: ',locFile))
    }
    
    # The features contains the information on depth and horizontal position
    locDataFeat <- locData$features[[1]]
    
    idxsThrm <- base::grep("ThermistorDepth", base::names(locDataFeat))
    
    if(base::length(idxsThrm) == 0){
      log$error(base::paste0("Could not find 'ThermistorDepth' string matches within the 'features' object of ", locFile))
      stop()
    }
    
    subThrmDpth <- locDataFeat[idxsThrm]
    thrmDpthDf <- base::data.frame(depthLocs = base::names(subThrmDpth), thermistorDepth = base::as.numeric(base::unlist(subThrmDpth)), stringsAsFactors = FALSE )
    # Map the 501 - 510 VER locs to readable names used by stats/QM (e.g. depth0...depth10)
    dpthNums <- base::gsub(pattern = "ThermistorDepth",replacement = "depth", thrmDpthDf$depthLocs)
    dpthNums <- base::gsub(pattern = "depth50",replacement = "depth", dpthNums)
    dpthNums <- base::gsub(pattern = "depth51", replacement = "depth1", dpthNums) # For depth10-depth19
    dpthNums <- base::gsub(pattern = "depth52", replacement = "depth2", dpthNums) # Just in case depth20-depth29 exist (not expected)
    thrmDpthDf$depthName <- dpthNums
    thrmDpthDf$horizontalPosition <- base::as.integer(base::gsub("ThermistorDepth","", thrmDpthDf$depthLocs))
    thrmDpthDf <- thrmDpthDf[base::order(thrmDpthDf$horizontalPosition),]
    thrmDpthDf <- dplyr::select(thrmDpthDf,-"depthLocs") # non-publishable term
    # CHECK to make sure VER locs and corresponding depths increase together
    incDiff <- base::which(base::diff(thrmDpthDf$thermistorDepth) < 0 )
    if (base::length(incDiff) >= 1){
      log$error(base::paste0("Unlikely depth for a given Horizontal Position 
                             designation, as 501, 502, 503, etc. should
                             increment with increasing depth. Inspect schemas
                             that lead to creating the location file "),locFile)
      stop()
    }
    log$info(base::paste0("Successfully extracted thermistor depths from location file ", locFile))
  } else {
    log$error(base::paste0("Invalid location .json file ",locFile))
    stop()
  } # End location file prep
  
  # ======================================================================= #
  # ================== STATS AND QUALITY METRICS ORG ====================== #
  #  =====================================================================  #
  lsDataVarTime <- base::list()
  
  for(varTime in nameVarTime){
    filzSameTime <- file[base::grep(base::paste0("_",varTime), file)]
    
    log$info(base::paste0("Beginning to process ", base::paste(filzSameTime, collapse = " and ")))
    
    if(base::length(filzSameTime) != 2 || 
       !base::any(base::grepl(paste0("/",statDir ,"/"),filzSameTime)) || 
       !base::any(base::grepl(paste0("/",qmDir ,"/"),filzSameTime)) ){
       log$error(base::paste0('Expecting just one each of basicStats and 
                             qualityMetrics file for ', varTime,' time index.
                             However the following are present: ',
                             base::paste(filzSameTime, collapse = ", ")))
      stop()
    }
    
    # Read in each file and merge
    lsFnsSame <- base::list()
    for(fnSame in filzSameTime){
      # What format?
      fmt <- utils::tail(base::strsplit(fnSame,'[.]')[[1]],1)
      
      # Load in file 
      if(fmt == 'avro'){
        idxData  <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile=fnSame,NameLib='/ravro.so',log=log),silent=FALSE)
      } else if (fmt == 'parquet'){
        idxData  <- base::try(NEONprocIS.base::def.read.parq(NameFile=fnSame,log=log),silent=FALSE)
      } else {
        log$error(base::paste0('Cannot determine file type for ',fnSame,'. Extension must be .avro or .parquet.'))
        stop()
      }
      if(base::any(base::class(idxData) == 'try-error')){
        log$error(base::paste0('File ', fnSame,' is unreadable.')) 
        stop()
      } else {
        log$debug(base::paste0('Successfully read in file: ',fnSame))
      }
      
      lsFnsSame[[fnSame]] <- idxData
    }
    
    
    if(base::nrow(lsFnsSame[[1]]) != base::nrow(lsFnsSame[[2]])){
      log$error(base::paste0("The basicStats and qualityMetrics datasets have differing number of rows when they should be the same with ", base::paste(filzSameTime, collapse = ", and ") ))
      stop()
    }
    
    # Ensure the merge columns exist in both dataframes to be merged
    if(!NEONprocIS.base::def.validate.dataframe(dfIn=lsFnsSame[[1]],TestNameCol=mrgeCols,log=log)){
      log$error(base::paste0("The merge columns ", base::paste0(mrgeCols,collapse = ", "), " are not fully in ",filzSameTime[1] ))
      stop()
    }
    
    if(!NEONprocIS.base::def.validate.dataframe(dfIn=lsFnsSame[[2]],TestNameCol=mrgeCols,log=log)){
      log$error(base::paste0("The merge columns ", base::paste0(mrgeCols,collapse = ", "), " are not fully in ",filzSameTime[2] ))
      stop()
    }
    
    # Make sure there are no duplicate columns
    if(base::any(!base::intersect(base::names(lsFnsSame[[2]]), base::names(lsFnsSame[[1]]) ) %in% mrgeCols) ){
      log$warn(base::paste0('The non-time column names contained in the files ', base::paste0(filzSameTime,collapse=','), ' overlap. Taking the first instance of duplicate column names.'))
      dupCol <- base::names(lsFnsSame[[1]]) %in% base::setdiff(base::names(lsFnsSame[[2]]),mrgeCols)
      lsFnsSame[[1]] <- lsFnsSame[[1]][!dupCol]
    }
    
    # Ensure merge columns (timeCols) equal    
    sameMrgeColsBool <- base::unlist(base::lapply(1:length(mrgeCols), function(i) base::all.equal(lsFnsSame[1][[mrgeCols[i]]],lsFnsSame[2][[mrgeCols[i]]])) )
    # Issue a warning if the timestamps are not identical
    if(!base::any(sameMrgeColsBool)){
      log$warn(base::paste0('Timestamps in file ',filzSameTime[1], ' are not fully consistent with timestamps in ', filzSameTime[2]))
    }
    
    # Merge by the merge columns (should be start/end times)
    cmboStatQm <- base::merge(lsFnsSame[[1]], lsFnsSame[[2]],by = mrgeCols)
    
    if(base::nrow(lsFnsSame[[1]]) != base::nrow(cmboStatQm) || base::nrow(lsFnsSame[[2]]) != base::nrow(cmboStatQm)){
      log$error(base::paste0("Problem when merging basicStats and qualityMetrics: the number of rows should not have changed with ", base::paste(filzSameTime, collapse = ", and ") ))
      stop()
      
    }
    # ----------------------------------------------------------------------- #
    #             Generate a list of dataframes for each depth 
    # ----------------------------------------------------------------------- #
    # Create indivdual data.frames of each depth
    cols <- base::colnames(cmboStatQm)
    # This assumes the standard format "depth#WaterTemp" or "depth##WaterTemp"
    uniqCols <- base::unique(base::substr(cols, start = 1, stop = 7))
    uniqCols <- uniqCols[base::grep("depth", uniqCols)]
    uniqCols <- base::gsub(uniqCols, pattern = "W", replacement = "")
    
    # separate depths by term "WaterTemp"
    dpthCols <- base::unlist(base::lapply(cols, function(col) base::strsplit(col, split = "WaterTemp", fixed = TRUE)[[1]][1]) )
    nonDpthCols <- base::unlist(base::lapply(cols, function(col) base::strsplit(col, split = "WaterTemp", fixed = TRUE)[[1]][2]) ) 
    
    uniqCols <- base::unique(dpthCols)
    uniqCols <- uniqCols[base::grep("depth", uniqCols)]
    
    lsDpth <- base::list()
    for(dpth in uniqCols){
      idxsColsDpth <- base::which(dpthCols == dpth)
      colsSel <- base::c(mrgeCols,cols[idxsColsDpth] )
      lsDpth[[dpth]] <- cmboStatQm[, colsSel]
      lsDpth[[dpth]]$depthName <- dpth 
      base::colnames(lsDpth[[dpth]]) <- base::gsub(pattern = dpth,
                                                   replacement = "",
                                                   base::colnames(lsDpth[[dpth]]) )
    }
    
    totColsEachDf <- base::lapply(1:base::length(lsDpth), function(i) base::ncol(lsDpth[[i]]) ) 
    
    if(base::length(base::unique(base::unlist(totColsEachDf))) != 1){
      log$info(base::paste0("The combined basicStats and qualityMetrics dataframes exhibit differing numbers of columns for each depth with ",base::paste(filzSameTime, collapse = ", and ") ))
    }
    # ----------------------------------------------------------------------- #
    #                          Convert to long format
    # ----------------------------------------------------------------------- #
    dtLong <- data.table::rbindlist(lsDpth, fill = TRUE)
    # Rename to appropriate dataPub format:
    base::colnames(dtLong) <- base::gsub("WaterTemp", "tsdWaterTemp", base::colnames(dtLong))
    
    # ======================================================================= #
    #  ======================= merge in location info ======================  #
    #   ===================================================================   #
    # Note this step also removes depths that are not present:
    dtLongDpth <- base::merge(x = dtLong, y = thrmDpthDf, by = "depthName")
    dtLongDpth <- dplyr::select(dtLongDpth, -"depthName") # don't pub this col
    
    # Clean up colnames:
    idxsQm <- base::grep("QM",base::colnames(dtLongDpth) ) 
    if(base::length(idxsQm) > 0 ){
      # Remove the spurious QF when it's actually a time-averaged QM
      base::colnames(dtLongDpth)[idxsQm] <- base::gsub(pattern = "QF", replacement = "",  base::colnames(dtLongDpth)[idxsQm])
    }
    
    lsDataVarTime[[varTime]] <- dtLongDpth
    log$info(base::paste0('Successfully merged the contents of ', varTime,' files.'))
  } # End loop around time variables
  
  return(lsDataVarTime)
}

