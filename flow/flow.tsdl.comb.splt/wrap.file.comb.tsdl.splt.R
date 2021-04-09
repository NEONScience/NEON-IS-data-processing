##############################################################################################
#' @title Merge the temp chain stats and quality metrics files with location data contents of multiple files (avro or parquet) that share a common time variable.

#' @author
#' Guy Litt \email{glitt@battelleecology.org}

#' @description
#' Definition function. Merge the contents of multiple tchain parquet data files
#'  that share a common timestamp and time interval. Combine with location information
#'  on depth and convert datasets from wide to individual files by HOR.VER.
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
#' @param filePths Character vector of full or relative file paths. Must be avro or parquet format.
#' @param nameVarTime Character value. The name(s) of the time variable common across QM and stats files. E.g. c("001", "030")
#' @param nameSchmMapDpth The filepath to the schema that maps named location depths to data's depth column naming convention
#' @param nameSchmMapSub The filepath to the schema that maps column name strings to be replaced with other column name strings
#' @param mrgeCols Character vector. The column names for merging stat and QM files.
#'  Default \code{c("startDateTime", "endDateTime")}
#' @param locDir The expected directory name containing the location file(s). Default "location"
#' @param statDir The expected directory name containing the basicStats file. Default "stats"
#' @param qmDir The expected directory name containing the qualityMetrics file. Default "quality_metrics"
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log.
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A list of each time interval containing a list of data frames corresponding to an 
#' individual HOR.VER with the merged file contents.

#' @references Currently none

#' @keywords Currently none

#' @examples
#' # Not run
#' filePths <- c('/path/to/qmDir/qmfile.parquet','/path/to/statDir/statfile.parquet', 'path/to/locDir/locfile.json)
#' def.file.comb.tsdl.splt(file=file,nameVarTime=c('001','030')

# nameSchmMapCols <- "~/pfs/avro_schemas/tsdl_col_term_subs.avsc"
rm(corrColNams)
#@param corrColNams Should column names attempt to be corrected to match pub wb? Default TRUE


#' @export
# TODO add SuspectCal to pub wb?
# TODO remove ConsistencyFail/Pass/NAQM from pub wb?
# TODO add tsdWaterTempFinalQFSciRvw to dataset?
# TODO depth11 doesn't exist yet for Mean/Minimum/Maximum/Variance stats (probably changes once CVAL files change)
# TODO add a colsKeep term??

# nameSchmMapCols <- "~/pfs/avro_schemas/tsdl_col_term_subs.avsc"
# changelog and author contributions / copyrights
#   Guy Litt (2021-04-09)
#     original creation

##############################################################################################
wrap.file.comb.tsdl.splt <- function(filePths,
                                    nameVarTime, 
                                    nameSchmMapDpth,
                                    nameSchmMapCols,
                                    mrgeCols = c("startDateTime", "endDateTime"),
                                    locDir = "location",
                                    statDir = "stats",
                                    qmDir = "quality_metrics",
                                    #corrColNams = TRUE,
                                    log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # ========================================================================= #
  #  ==================== LOAD Depth location file =========================  #
  #   =====================================================================   #
  locFilePths <- filePths[base::which(base::basename(base::dirname(filePths)) == locDir)]
  
  # Turns out it doesn't matter which location file is used - both have same format under $features
  if(base::length(locFilePths) > 1){
    log$info(base::paste0("Multiple location files exist. Using the first location file, ", locFilePths[1]))
  }
 
  locFile <- locFilePths[1]
  
  if(base::length(locFile) != 1){
    log$error(base::paste0("Could not find a location file in provided files: ",
                           base::paste(filePths, collapse = ", ")))
    stop()
  }
  
  
  if(NEONprocIS.base::def.validate.json(locFile) && 
     NEONprocIS.base::def.validate.json(nameSchmMapDpth)){
    
    locData <- try(rjson::fromJSON(file = locFile))
    
    if(base::any(base::class(locData) == 'try-error')){
      log$error(base::paste0('File ', locFile,' is unreadable.')) 
      stop()
    } else {
      log$debug(base::paste0('Successfully read in file: ',locFile))
    }
    
    # Generate the depth-location term mapping:
    rsltParsMap <- def.schm.avro.pars.map(FileSchm = nameSchmMapDpth,log = log)
    
    mapDpth <- rsltParsMap$map
    
    # The features contains the information on depth and horizontal position
    locDataFeat <- locData$features[[1]]
    
    idxsThrm <- base::unlist(base::lapply(mapDpth$term1, function(x) base::grep(x, base::names(locDataFeat)) ) )
    
    if(base::length(idxsThrm) == 0){
      log$error(base::paste0("Could not find string matches within the 'features' object of ", locFile, " when using the first terms mapped in ", nameSchmMapDpth))
      stop()
    }
    
    subThrmDpth <- locDataFeat[idxsThrm]
    thrmDpthDf <- base::data.frame(depthLocs = base::names(subThrmDpth), thermistorDepth = base::as.numeric(base::unlist(subThrmDpth)), stringsAsFactors = FALSE )
    
    # Map the 501 - 511 VER locs to readable names used by stats/QM (e.g. depth0...depth10)
    dpthNameRpt <- wrap.schm.map.char.gsub(obj = thrmDpthDf$depthLocs,FileSchm = nameSchmMapDpth,log = log)
    thrmDpthDf$depthName <- dpthNameRpt$obj
    
    # Extract the HOR & VER locs
    thrmDpthDf$horizontalPosition <- base::as.integer(locDataFeat$HOR)
    thrmDpthDf$verticalPosition <- base::unlist(base::lapply(thrmDpthDf$depthLocs,
                                                             function(x) base::regmatches(x, base::gregexpr("[[:digit:]]+", x)) ))  
    
    thrmDpthDf <- thrmDpthDf[base::order(thrmDpthDf$verticalPosition),]
    thrmDpthDf <- dplyr::select(thrmDpthDf,-"depthLocs") # removes non-published term
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
    badFile <- base::character()
    if(!NEONprocIS.base::def.validate.json(locFile)){
      badFile <- c(badFile,locFile)
    }
    if(!NEONprocIS.base::def.validate.json(locFile)){
      badFile <- c(badFile,nameSchmMapDpth)
    }
    
    log$error(base::paste0("Invalid location .json file ",
                           base::paste0(badFile, collapse = ", ")) )
    stop()
  } # End location file prep
  
  
 
  
  
  # ======================================================================= #
  # ================== STATS AND QUALITY METRICS ORG ====================== #
  #  =====================================================================  #
  lsDataVarTime <- base::list()
  # Loop by time variable for organizing data
  for(varTime in nameVarTime){
    filzSameTime <- filePths[base::grep(base::paste0("_",varTime), filePths)]
    
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
    # Create individual data.frames of each depth
    cols <- base::colnames(cmboStatQm)
    # This assumes the standard format "depth#WaterTemp" or "depth##WaterTemp"
    
    # -----------------
    # Parse columns based on the mapping

    
    testing <- base::lapply(mapDpth$term2, function(x) def.map.char.gsub(pattFind = x, replStr = "", obj = cols) )
    
    
   
    
    
   
   
    
    # Identify the columns without any depth term & columns w/ depth terms:
    idxsMapDpthAll <- base::lapply(mapDpth$term2, function(x) base::grep(x, cols)) 
    colsNonDpth <- cols[-base::unlist(idxsMapDpthAll)]
    colsData <- cols[-base::which(cols %in% colsNonDpth)]
    
    # Note that just colsData and colsMrge will be kept
    
    # TODO add a colsKeep term??
  
    
    # # Identify the columns corresponding to actual depths for this site-loc
    # idxsMapDpthActl <- base::lapply(mapDpth$term2, function(x) base::grepl(x, cols))
    # 
    
    
    # Search for a pattern across all search terms, and choose whichever string match is longest. 
    # e.g. depth1 matches depth10 w/ 6 chars, but depth10 matches depth10 w/ 7 chars.
    matSnglDigMl <- base::sapply(colsData, function(col) base::sapply(mapDpth$term2, 
                                          function(x) base::attributes(base::gregexpr(pattern = x ,text = col)[[1]])$match.length ) ) 
    # The indices corresponding to the maximum match length:
    idxMax <- sapply(1:base::ncol(matSnglDigMl), function(i) base::as.integer(base::which.max(matSnglDigMl[,i]) ) )
    
    # create the full column name - depth mapping df:
    dfMtch <- base::data.frame(colNam = colsData, idxMtch = idxMax, mtchGrp = mapDpth$term2[idxMax], stringsAsFactors = FALSE)
    
  
    
    # TODO adapt test logic to dfMtch...
    # These should all be the same length:
    totlNumCols <- (base::unlist(base::lapply(idxsMapDpthAll, function(x) base::length(x))))
    
    if(base::length(base::unique(totlNumCols)) == 2) {
      # TODO option B - find the indices that have more columns than they should, and determine appropriate substitution
      idxsGood <- base::which(totlNumCols == base::min(totlNumCols))
      # idxsXtra
      
    } else if (base::length(base::unique(totlNumCols)) > 2){
      log$error("Should not expect varying numbers of columns.")
      
    }
    
    
    
   
    # ----------------------------------------------------------------------- #
    #  -------------------------- Clean up colnames ------------------------  #
    #   -------------------------------------------------------------------   #
    # The new column name removes the depth term in the names:
    dfMtch$newColNam <- base::sapply(1:base::nrow(dfMtch), function(i)
      base::gsub(pattern = dfMtch$mtchGrp[i], replacement = "", x = dfMtch$colNam[i]) )

    #  ==================== LOAD Col Substitution schema =====================  #
    if(NEONprocIS.base::def.validate.json(nameSchmMapCols)){
      
      # Generate the depth-location term mapping:
      rsltParsMapCols <- def.schm.avro.pars.map(FileSchm = nameSchmMapCols,log = log)
      
      mapCols <- rsltParsMapCols$map
      
    } else {
      log$warn(base::paste0("Column rename mapping schema ", nameSchmMapCols, 
                            " could not be loaded. No column renaming will occur."))
      
      mapCols <- NULL
    }
    # ----------------------------------------------------------------------- #
    #  ----------------- Perform Col Substitution schema -------------------  #
    if (!base::is.null(mapCols)){
      log$info(paste0("Performing column name substitution using ", nameSchmMapCols))
 
      newColNam <- dfMtch$newColNam
      for(idxColRenm in 1:base::nrow(mapCols)){
        newColNam <- def.map.char.gsub(pattFind=mapCols$term1[idxColRenm],
                                       replStr = mapCols$term2[idxColRenm],
                                       obj = newColNam)
      }
      dfMtch$newColNam <- newColNam
    }
    
    
    
    # ----------------------------------------------------------------------- #
    # Split data into a list of dataframes by the matching group:
    # Loop by depth for merging in location information
    lsDpth <- base::list()
    for(dpth in base::unique(dfMtch$mtchGrp)){
      
      # Subset data.frame by depth:
      idxsColsDpth <- base::which(dfMtch$mtchGrp == dpth)
      colsSel <- base::c(mrgeCols,dfMtch$colNam[idxsColsDpth] )
      lsDpth[[dpth]] <- cmboStatQm[, colsSel]
      
      
      colsRenm <- base::c(mrgeCols, dfMtch$newColNam[idxsColsDpth])
      base::colnames(lsDpth[[dpth]])  <- colsRenm
     
      # 
      # #lsDpth[[dpth]]$depthName <- dpth 
      # base::colnames(lsDpth[[dpth]]) <- base::gsub(pattern = dpth,
      #                                              replacement = "",
      #                                              base::colnames(lsDpth[[dpth]]) )
      # 
      # # -------------- Merge in location information with data ----------------
      # lsDpth[[dpth]] <- base::merge(lsDpth[[dpth]], thrmDpthDf, by = "depthName", all.y = FALSE)
      # lsDpth[[dpth]] <- dplyr::select(lsDpth[[dpth]], -"depthName") # don't pub this col
      # 
      
    }
    

    
    # # --------------------------------------------------------------------------------
    # colsMapDpth <- base::lapply(idxsMapDpthActl, function(x) base::c(colsNonDpth, cols[x]))
    # 
    # # A list of dataframes for each depth
    # lsDpthDat <- base::lapply(1:base::length(colsMapDpth), function(i) cmboStatQm[,colsMapDpth[[i]] ])
    # 
    # # Add the depth name to each data.frame of data
    # lsDpthDat <- base::lapply(1:base::length(lsDpthDat),
    #                        function(i) {
    #                          lsDpthDat[[i]]$depthName <- mapDpth$term2[i];
    #                          return(lsDpthDat[[i]]) } )
    # 
    # # Now merge the data.frame with the location characteristics
    # lsDpth <- base::lapply(1:base::length(lsDpthDat), function(i) {
    #    base::merge(lsDpthDat[[i]], thrmDpthDf, by = "depthName", all.y = FALSE) })
    # 
    # # Standardize column names by removing depth# based on the schema substitution term
    # lsDpth <- base::lapply(1:base::length(lsDpth), function(i) {
    #   base::colnames(lsDpth[[i]]) <- base::gsub(thrmDpthDf$depthName[i],"",base::colnames(lsDpth[[i]]) );
    #   return(lsDpth[[i]])
    # })
    # 
    # 
    # allNams <- base::lapply(1:base::length(lsDpth), function(i) base::colnames(lsDpth[[i]]))
    # if(base::any(base::unlist(base::lapply(1:base::length(thrmDpthDf$depthName),
    #                            function(i) base::grepl(thrmDpthDf$depthName[i],base::unlist(allNams)))))){
    #   # This test identifies any depth map schema names that were not removed from the data column names
    #   idxsFindDpth <- base::lapply(1:base::length(thrmDpthDf$depthName),
    #                          function(i) base::grep(thrmDpthDf$depthName[i],base::names(lsDpth[[1]])))
    #   # the problematic depth strings
    #   dpthProb <- thrmDpthDf$depthName[base::which(base::length(idxsFindDpth)>0)]
    # 
    #   log$error(base::paste0("Error in substituting depth## out of column names. 
    #                          Reconsider the mapping schema 'nameSchmMapDpth' terms: ",
    #                          base::paste0(dpthProb, collapse = ",")))
    #   stop()
    # }
    # 
    # # All the column names (should now be identical)
    # allNams
    # 
    # 
    # # Remove the "depthName" column
    # lsDpthTst  <- base::lapply(1:base::length(lsDpth), function(i) {
    #  lsDpth[[i]][["depthName"]] <- NA})
    # 
    # 
    # -----------------
    uniqCols <- base::unique(base::substr(cols, start = 1, stop = 7))
    uniqCols <- uniqCols[base::grep("depth", uniqCols)]
    uniqCols <- base::gsub(uniqCols, pattern = "W", replacement = "")
    
    # separate depths by term "WaterTemp"
    dpthCols <- base::unlist(base::lapply(cols, function(col) base::strsplit(col, split = "WaterTemp", fixed = TRUE)[[1]][1]) )
    nonDpthCols <- base::unlist(base::lapply(cols, function(col) base::strsplit(col, split = "WaterTemp", fixed = TRUE)[[1]][2]) ) 
    
    uniqCols <- base::unique(dpthCols)
    uniqCols <- uniqCols[base::grep("depth", uniqCols)]
    
    # Loop by depth for merging in location information
    lsDpth <- base::list()
    for(dpth in uniqCols){
      idxsColsDpth <- base::which(dpthCols == dpth)
      colsSel <- base::c(mrgeCols,cols[idxsColsDpth] )
      lsDpth[[dpth]] <- cmboStatQm[, colsSel]
      lsDpth[[dpth]]$depthName <- dpth 
      base::colnames(lsDpth[[dpth]]) <- base::gsub(pattern = dpth,
                                                   replacement = "",
                                                   base::colnames(lsDpth[[dpth]]) )
      
      # -------------- Merge in location information with data ----------------
      lsDpth[[dpth]] <- base::merge(lsDpth[[dpth]], thrmDpthDf, by = "depthName", all.y = FALSE)
      lsDpth[[dpth]] <- dplyr::select(lsDpth[[dpth]], -"depthName") # don't pub this col
      
      # --------------------------------------------------------------------- #
      #  -------------------------- Clean up colnames ----------------------  #
      #   -----------------------------------------------------------------   #
      
      # wrap.schm.map.char.gsub(obj = cols, FileSchm = )
      # 
      if(corrColNams){
        # Remove the spurious QF from column name when it's actually a time-averaged QM
        idxsQm <- base::grep("QM",base::colnames(lsDpth[[dpth]]) ) 
        if(base::length(idxsQm) > 0 ){
          base::colnames(lsDpth[[dpth]])[idxsQm] <- base::gsub(pattern = "QF", replacement = "",  base::colnames(lsDpth[[dpth]])[idxsQm])
        }
        # Change WaterTemp to tsdWaterTemp:
        idxsWatrTemp <- base::grep("WaterTemp", base::colnames(lsDpth[[dpth]]))
        idxsWatrTemp <- idxsWatrTemp[base::which(!base::grepl("tsd",base::colnames(lsDpth[[dpth]])[idxsWatrTemp]))]
        if(base::length(idxsWatrTemp) > 0){
          base::colnames(lsDpth[[dpth]])[idxsWatrTemp] <- base::gsub("WaterTemp","tsdWaterTemp",base::colnames(lsDpth[[dpth]])[idxsWatrTemp])
        }
      }
      #   -----------------------------------------------------------------   #
      #  -------------------------------------------------------------------  #
      # --------------------------------------------------------------------- #
    } # end loop by depth
    
    # Check total column numbers - should be similar
    totColsEachDf <- base::lapply(1:base::length(lsDpth), function(i) base::ncol(lsDpth[[i]]) ) 
    if(base::length(base::unique(base::unlist(totColsEachDf))) != 1){
      log$debug(base::paste0("The combined basicStats and qualityMetrics dataframes exhibit differing numbers of columns for each depth with ",base::paste(filzSameTime, collapse = ", and ") ))
    }
    
    # Remove Depths absent from the location file
    maxVer <- base::max(thrmDpthDf$verticalPosition)
    lsDpthSub <- lsDpth[base::which(base::names(lsDpth) %in% thrmDpthDf$depthName)]
    
    # Rename to HOR.VER
    base::names(lsDpthSub) <- base::unlist(base::lapply(1:base::length(lsDpthSub),
                        function(i) base::unique(base::paste(lsDpthSub[[i]]$horizontalPosition,
                                                             lsDpthSub[[i]]$verticalPosition,
                                                             sep ="."))))
    
    # A list of times and a list of depths containing dataframes of data
    lsDataVarTime[[varTime]] <- lsDpthSub
    log$info(base::paste0('Successfully merged the contents of ', varTime,' files.'))
  } # End loop around time variables
  
  return(lsDataVarTime)
}

