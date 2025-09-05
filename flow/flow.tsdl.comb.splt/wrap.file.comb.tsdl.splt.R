##############################################################################################
#' @title Merge the temp chain stats and quality metrics files with location data contents of multiple files (avro or parquet) that share a common time variable.

#' @author
#' Guy Litt \email{glitt@battelleecology.org}

#' @description
#' Definition function. Merge the contents of multiple tchain parquet data files
#'  that share a common timestamp and time interval. Combine with location information
#'  on depth and convert datasets from wide to individual dataframes of HOR.VER for a 
#'  given time interval (e.g. "001"). Optionally rename column names by matching terms 
#'  and replacing with a substitute term as defined in the \code{nameSchmMapCols} 
#'  avro schema.
#'  
#' @details
#' Steps in this do-all wrapper function:
#' 1. identify location file & load in
#' 2. identify QM and stat files for e/ time interval
#' 3. Combine QM and stat files for e/ time interval
#' 4. Optionally rename column names using column name substitution avro schema.
#'      Renaming considers search pattern strings that end with a number (e.g. depth1),
#'      and makes sure that another number doesn't exist after the last number to yield a match.
#' 5. Assign location to the combined QM & stat dataframe using matched terms from the location mapper avro schema
#' 
#' Example files that exist as \code{basename(filePths)}
#' "CFGLOC110702.json"
#' "tchain_32610_locations.json" 
#' "tchain_CFGLOC110702_2019-01-10_qualityMetrics_001.parquet"
#' "tchain_CFGLOC110702_2019-01-10_qualityMetrics_030.parquet"
#' "tchain_CFGLOC110702_2019-01-10_basicStats_001.parquet" 
#' "tchain_CFGLOC110702_2019-01-10_basicStats_030.parquet"  
#' 
#' Example Files used for matching the '001' time interval:
# "CFGLOC110702.json"
# "tchain_CFGLOC110702_2019-01-10_qualityMetrics_001.parquet"
# "tchain_CFGLOC110702_2019-01-10_basicStats_001.parquet" 
#' 
#' @param filePths Character vector of full or relative file paths. Must be avro or parquet format.
#' @param nameVarTime Character value. The name(s) of the time variable common across QM and stats files. E.g. c("001", "030")
#' @param nameSchmMapDpth The filepath to the avro schema that maps named location depths to data's depth column naming convention
#' @param nameSchmMapCols Optional. The filepath to the avro schema that maps column name strings to be replaced with other column name strings
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

#' @export

# changelog and author contributions / copyrights
#   Guy Litt (2021-04-12)
#     original creation
#   Nora Catolico (2025-08)
#     updates for new structure

##############################################################################################
wrap.file.comb.tsdl.splt <- function(filePths,
                                     fileNames,
                                     InfoDirIn,
                                     DirOut,
                                     NameFileSufxRm,
                                    nameVarTime, 
                                    nameSchmMapDpth,
                                    nameSchmMapCols,
                                    GroupDir,
                                    SplitGroupName,
                                    mrgeCols=c("startDateTime", "endDateTime"),
                                    locDir="location",
                                    statDir="stats",
                                    qmDir="quality_metrics",
                                    log=NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # ========================================================================= #
  #  ====================== Load Mapping schemas ===========================  #
  #   =====================================================================   #
  locFilePths <- filePths[base::which(base::basename(base::dirname(filePths)) == locDir)]
  if(grepl("_locations",locFilePths)){
    locFile <- locFilePths[grepl("_locations",locFilePths)]
    log$debug(base::paste0("location datum(s) found, reading in: ",locFile))
    #first get location history z offset
    LocationHist <- NEONprocIS.base::def.loc.geo.hist(locFile, log = NULL)
  }else{
    locFile <- locFilePths[1]
    log$debug(base::paste0("Not asset location history found, reading in: ",locFile))
    LocationHist <- NULL
  }
  
  
  if(NEONprocIS.base::def.validate.json(locFile,log=log) && 
     NEONprocIS.base::def.validate.json(nameSchmMapDpth,log=log)){
    
    locData <- try(rjson::fromJSON(file=locFile))
    
    if(base::any(base::class(locData) == 'try-error')){
      log$error(base::paste0('File ', locFile,' is unreadable.')) 
      stop()
    } else {
      log$debug(base::paste0('Successfully read in file: ',locFile))
    }
    
    # Generate the depth-location term mapping:
    rsltParsMap <- def.schm.avro.pars.map(FileSchm=nameSchmMapDpth,log=log)
    mapDpth <- rsltParsMap$map
    
    # The features contains the information on depth and horizontal position
    locDataFeat <- locData$features[[1]]
    
    idxsThrm <- base::unlist(base::lapply(mapDpth$term1, function(x) base::grep(x, base::names(locDataFeat)) ) )
    
    if(base::length(idxsThrm) == 0){
      log$error(base::paste0("Could not find string matches within the 'features' object of ", locFile, " when using the first terms mapped in ", nameSchmMapDpth))
      stop()
    }
    
    subThrmDpth <- locDataFeat[idxsThrm]
    thrmDpthDf <- base::data.frame(depthLocs=base::names(subThrmDpth), thermistorDepth=base::as.numeric(base::unlist(subThrmDpth)), stringsAsFactors=FALSE )
    
    # Map the 501 - 511 VER locs to readable names used by stats/QM (e.g. depth0...depth10)
    dpthNameRpt <- wrap.schm.map.char.gsub(obj=thrmDpthDf$depthLocs,FileSchm=nameSchmMapDpth,log=log)
    thrmDpthDf$depthName <- dpthNameRpt$obj
    
    # Extract the HOR & VER locs (these data columns removed at end of this loop)
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
    log$debug(base::paste0("Successfully extracted thermistor depths from location file ", locFile))
  } else {
    badFile <- base::character()
    if(!NEONprocIS.base::def.validate.json(locFile,log=log)){
      badFile <- c(badFile,locFile)
    }
    if(!NEONprocIS.base::def.validate.json(locFile,log=log)){
      badFile <- c(badFile,nameSchmMapDpth)
    }
    
    log$error(base::paste0("Invalid location .json file ",
                           base::paste0(badFile, collapse=", ")) )
    stop()
  } # End location map schema file prep
  
  
  # ========================================================================= #
  #  =================== STATS AND QUALITY METRICS ORG =====================  #
  #   =====================================================================   #
  lsDataVarTime <- base::list()
  # Loop by time variable for organizing data
  for(varTime in nameVarTime){
    filzSameTime <- filePths[base::grep(base::paste0("_",varTime), filePths)]
    
    log$info(base::paste0("Beginning to process ",varTime, " time interval data with ", base::paste(filzSameTime, collapse=" and ")))
    
    if(base::length(filzSameTime) != 2 || 
       !base::any(base::grepl(paste0("/",statDir ,"/"),filzSameTime)) || 
       !base::any(base::grepl(paste0("/",qmDir ,"/"),filzSameTime)) ){
       log$error(base::paste0('Expecting just one each of basicStats and 
                             qualityMetrics file for ', varTime,' time index.
                             However the following are present: ',
                             base::paste(filzSameTime, collapse=", ")))
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
      log$error(base::paste0("The basicStats and qualityMetrics datasets have differing number of rows when they should be the same with ", base::paste(filzSameTime, collapse=", and ") ))
      stop()
    }
    
    # Ensure the merge columns exist in both dataframes to be merged
    if(!NEONprocIS.base::def.validate.dataframe(dfIn=lsFnsSame[[1]],TestNameCol=mrgeCols,log=log)){
      log$error(base::paste0("The merge columns ", base::paste0(mrgeCols,collapse=", "), " are not fully in ",filzSameTime[1] ))
      stop()
    }
    
    if(!NEONprocIS.base::def.validate.dataframe(dfIn=lsFnsSame[[2]],TestNameCol=mrgeCols,log=log)){
      log$error(base::paste0("The merge columns ", base::paste0(mrgeCols,collapse=", "), " are not fully in ",filzSameTime[2] ))
      stop()
    }
    
    # Make sure there are no duplicate columns
    if(base::any(!base::intersect(base::names(lsFnsSame[[2]]), base::names(lsFnsSame[[1]]) ) %in% mrgeCols) ){
      log$warn(base::paste0('The non-time column names contained in the files ', base::paste0(filzSameTime,collapse=','), ' overlap. Taking the first instance of duplicate column names.'))
      dupCol <- base::names(lsFnsSame[[1]]) %in% base::setdiff(base::names(lsFnsSame[[2]]),mrgeCols)
      lsFnsSame[[1]] <- lsFnsSame[[1]][!dupCol]
    }
    
    # Ensure merge columns (timeCols) equal    
    sameMrgeColsBool <- base::unlist(base::lapply(1:base::length(mrgeCols),
                                                  function(i) base::all.equal(lsFnsSame[1][[mrgeCols[i]]],lsFnsSame[2][[mrgeCols[i]]])) )
    # Issue a warning if the timestamps are not identical
    if(!base::any(sameMrgeColsBool)){
      log$warn(base::paste0('Timestamps in file ',filzSameTime[1], ' are not fully consistent with timestamps in ', filzSameTime[2]))
    }
    
    # Merge by the merge columns (should be start/end times)
    cmboStatQm <- base::merge(lsFnsSame[[1]], lsFnsSame[[2]],by=mrgeCols)
    
    if(base::nrow(lsFnsSame[[1]]) != base::nrow(cmboStatQm) || base::nrow(lsFnsSame[[2]]) != base::nrow(cmboStatQm)){
      log$error(base::paste0("Problem when merging basicStats and qualityMetrics: the number of rows should not have changed with ", base::paste(filzSameTime, collapse=", and ") ))
      stop()
    }
    # ----------------------------------------------------------------------- #
    #  ------------ Matching depth string patterns appropriately -----------  #
    #   -------------------------------------------------------------------   #
    cols <- base::colnames(cmboStatQm)

    # Identify the columns without any depth term & columns w/ depth terms:
    idxsMapDpthAll <- base::lapply(thrmDpthDf$depthName, function(x) base::grep(x, cols)) 
    colsNonDpth <- cols[-base::unlist(idxsMapDpthAll)]
    colsData <- cols[-base::which(cols %in% colsNonDpth)]
    # Note that just colsData and colsMrge will be kept, but also removing horizontalPosition/verticalPosition at end of loop
    
    # Search for a pattern across all search terms, and choose whichever string match is best 
    dfMtch <- def.find.mtch.str.best(obj = colsData, subFind = thrmDpthDf$depthName, log=log)
    
    # ----------------------------------------------------------------------- #
    #  ------------------------- Clean up colnames -------------------------  #
    #   -------------------------------------------------------------------   #
    # The new column name removes the depth term in the names:
    dfMtch$newColNam <- base::sapply(1:base::nrow(dfMtch), function(i)
      base::gsub(pattern=dfMtch$mtchGrp[i],replacement="",x=dfMtch$obj[i]) )

    
    # ----------------------------------------------------------------------- #
    #  ---------------- Perform Col Sub with Col Map Schema ----------------  #
    #   ----------------------------- OPTIONAL ----------------------------   #
    #   Load Col Substitution map schema 
    if(NEONprocIS.base::def.validate.json(nameSchmMapCols,log=log)){
      # Generate the depth-location term mapping:
      rsltParsMapCols <- def.schm.avro.pars.map(FileSchm=nameSchmMapCols,log=log)
      mapCols <- rsltParsMapCols$map
    } else {
      log$warn(base::paste0("Column rename mapping schema ", nameSchmMapCols, 
                            " could not be loaded. No column renaming will occur."))
      mapCols <- NULL
    }
    #  Apply column name string matching and substitution:
    if (!base::is.null(mapCols)){
      log$info(paste0("Performing column name substitution using ", nameSchmMapCols))
 
      newColNam <- dfMtch$newColNam
      for(idxColRenm in 1:base::nrow(mapCols)){
        newColNam <- def.map.char.gsub(pattFind=mapCols$term1[idxColRenm],
                                       replStr=mapCols$term2[idxColRenm],
                                       obj=newColNam,
                                       log=log)
      }
      dfMtch$newColNam <- newColNam
    } else {
      log$info(paste0("Not performing column name substitution."))
      dfMtch$newColNam <- dfMtch$obj
    }
    
    # ----------------------------------------------------------------------- #
    #  ---------- Generate a list of dataframes for each depth -------------  #
    #   -------------------------------------------------------------------   #
    # Split data into a list of dataframes by the matching group:
    # Loop by depth for merging in location information
    lsDpth <- base::list()
    for(dpth in thrmDpthDf$depthName){
      
      # Subset data.frame by depth:
      idxsColsDpth <- base::which(dfMtch$mtchGrp == dpth)
      colsSel <- base::c(mrgeCols,dfMtch$obj[idxsColsDpth] )
      lsDpth[[dpth]] <- cmboStatQm[, colsSel]
      
      # Rename columns based on nameSchmMapCols 
      colsRenm <- base::c(mrgeCols, dfMtch$newColNam[idxsColsDpth])
      base::colnames(lsDpth[[dpth]])  <- colsRenm
     
      # -------------- Merge in location information with data ----------------
      lsDpth[[dpth]]$depthName <- dpth 
      lsDpth[[dpth]] <- base::merge(lsDpth[[dpth]], thrmDpthDf, by="depthName", all.y=FALSE)
      lsDpth[[dpth]] <- dplyr::select(lsDpth[[dpth]], -"depthName") # don't pub this col
      
      log$debug(paste0("Merged data and locations at ", dpth, " with the ", varTime, " time interval."))
    }
    # ----------------------------------------------------------------------- #
    # ------------------ Final dim check & naming org ----------------------- #
    #  ---------------------------------------------------------------------  #
    # Check total column numbers - should be similar
    totColsEachDf <- base::lapply(1:base::length(lsDpth), function(i) base::ncol(lsDpth[[i]]) ) 
    if(base::length(base::unique(base::unlist(totColsEachDf))) != 1){
      log$error(base::paste0("The combined basicStats and qualityMetrics dataframes exhibit differing numbers of columns for each depth with ",base::paste(filzSameTime, collapse=", and ") ))
      stop()
    }
    
    # Rename to group and HOR.VER
    dirRepo<-InfoDirIn$dirRepo
    #shorten dirRepo to include everything after the last "_" and before the next number
    NEONsite <- sub(".*_", "", dirRepo)
    NEONsite <- substr(NEONsite,1,4)
    #verify that NEONsite is 4 capital letters
    if (nchar(NEONsite) != 4) {
      log$error(base::paste0("The site name is invalid: ",NEONsite))
      stop()
    }
    
    base::names(lsDpth) <- base::unlist(base::lapply(1:base::length(lsDpth),
                        function(i) base::unique(base::paste0(SplitGroupName,NEONsite,
                                                            lsDpth[[i]]$horizontalPosition,
                                                            lsDpth[[i]]$verticalPosition))))
    
    # Remove horizontalPosition & verticalPosition columns
    lsD <- lapply(1:length(lsDpth), function(i) {x <- lsDpth[[i]] %>% dplyr::select(-base::c("horizontalPosition","verticalPosition")); return(x)})
    base::names(lsD) <- base::names(lsDpth)
    
    # A list of times and a list of depths containing dataframes of data
    lsDataVarTime[[varTime]] <- lsD
    log$debug(base::paste0('Successfully merged the contents of ', varTime,' time interval files.'))
    
    
    dataTime <- lsDataVarTime[[varTime]]
    
    log$debug(base::paste0(
      'HOR.VER locations found in the combined data files: ',
      base::paste0(base::names(dataTime), collapse = ",")
    ))
    
    for(nameLoc in base::names(dataTime)){
      data <- dataTime[[nameLoc]]
      
      #add in z offset
      if(length(data$startDateTime)>0){
        data_all<-NULL
        if(!is.null(LocationHist) & length(LocationHist$CFGLOC)>0){
          for(i in 1:length(LocationHist$CFGLOC)){
            startDate<-LocationHist$CFGLOC[[i]]$start_date
            endDate<-LocationHist$CFGLOC[[i]]$end_date
            data_subset<-data[data$startDateTime>=startDate & data$startDateTime<endDate,]
            if(length(data_subset$startDateTime)>0){
              if(is.null(LocationHist$CFGLOC[[i]]$z_offset)|is.na(LocationHist$CFGLOC[[i]]$z_offset)){
                #do nothing
              }else{
                data_subset$thermistorDepth <- data_subset$thermistorDepth - LocationHist$CFGLOC[[i]]$z_offset
              }
            }
            if(i==1){
              data_all<-data_subset
            }else{
              data_all<-rbind(data_all,data_subset)
            }
          }
          data<-data_all
        }else{
          log$debug(base::paste0('No location history incorporated into files.'))
        }
      }
      
      #split 
      dirRepoParts <- strsplit(dirRepo, "/")[[1]]
      dirRepoParts <- dirRepoParts[dirRepoParts != ""]  # Remove empty strings
      idxDirOut <- base::paste0(DirOut,
                                "/",dirRepoParts[1],
                               "/",dirRepoParts[2],
                               "/",dirRepoParts[3],
                               "/",nameLoc)
      
      NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                    DirSub = "data",
                                    log = log)
      NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                    DirSub = "group",
                                    log = log)
      NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                    DirSub = "location",
                                    log = log)
      
      
      # Take stock of the combined data
      nameCol <- base::names(data)
      log$debug(base::paste0(
        'Columns found in the combined data files: ',
        base::paste0(nameCol, collapse = ',')
      ))
      
      # ----------------------------------------------------------------------- #
      # Remove suffix strings, Take the shortest file name, insert HOR.VER.TMI
      # ----------------------------------------------------------------------- #
      # Subset to dat files that should begin with 'tchain'
      fileDat <- fileNames[base::intersect(base::grep("tchain", fileNames), base::grep(varTime,fileNames))] 
      
      # Remove the NameFileSufx strings to simplify output filename
      fileDats <-  base::lapply(NameFileSufxRm, 
                                function(x) 
                                  base::gsub(pattern="__",replacement="_",
                                             base::gsub(pattern=x,replacement = "",
                                                        fileDat) ) )
      fileDats <- base::unlist(base::lapply(fileDats, 
                                            function(x) x[base::which.min(base::nchar(x))]))
      
      fileBase <-
        fileDats[base::nchar(fileDats) == base::min(base::nchar(fileDats))][1]
      # Insert the HOR.VER into the filename by replacing the varTime with the standard HORVER_TMI
      HORVER <- substr(nameLoc, nchar(nameLoc) - 5, nchar(nameLoc))
      VER <- substr(HORVER, nchar(HORVER) - 2, nchar(HORVER))
      fileBaseLoc <- base::gsub(varTime,
                                base::paste0(HORVER,"_",varTime),fileBase)
      
      fileOut <-
        NEONprocIS.base::def.file.name.out(nameFileIn = fileBaseLoc,
                                           sufx = "",
                                           log = log)
      log$debug(base::paste0(
        "Named output filepath as ", fileOut))
      
      nameFileOut <- base::paste0(idxDirOut, '/data/', fileOut)
      
      # ----------------------------------------------------------------------- #
      # Write out the data file.
      # ----------------------------------------------------------------------- #
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = data,
          NameFile = nameFileOut,
          NameFileSchm = NULL,
          Schm = NULL,
          log=log
        ),
        silent = TRUE)
      if (base::class(rptWrte) == 'try-error') {
        log$error(base::paste0(
          'Cannot write combined file ',
          nameFileOut,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0('Combined data written successfully in file: ',
                              nameFileOut))
      }
      
      
      
      # ----------------------------------------------------------------------- #
      # Update group file
      # ----------------------------------------------------------------------- #
      log$debug(base::paste0("GroupDir: ", GroupDir))
      groupFilePths <- base::list.files(GroupDir, full.names = TRUE)
      log$debug(base::paste0("groupFilePths: ", groupFilePths))
      if(base::length(groupFilePths) > 1){
        log$info(base::paste0("Multiple location files exist. Using the first location file, ", groupFilePths[1]))
      }
      groupFilePth <- groupFilePths[1]
      log$debug(base::paste0("groupFilePth: ", groupFilePth))
      groupData <- try(rjson::fromJSON(file=groupFilePth))
      log$debug(base::paste0("VER: ", VER))
      groupData$features[[1]]$VER<-VER
      groupData$features[[1]]$properties$group<-nameLoc
      log$debug(base::paste0("updated VER: ", groupData$features[[1]]$VER))
      log$debug(base::paste0("updated group: ", groupData$features[[1]]$properties$group))
      #need to keep some features boxed in json formatting
      groupData$features[[1]]$properties$data_product_ID<-list(groupData$features[[1]]$properties$data_product_ID)
      
      # ----------------------------------------------------------------------- #
      # Write out group file
      # ----------------------------------------------------------------------- #
      
      groupFileName <- sub(".*/", "", groupFilePth)
      nameJsonOut <- base::paste0(idxDirOut, '/group/', groupFileName)
      log$debug(base::paste0("groupFileName: ", groupFileName))
      log$debug(base::paste0("nameJsonOut: ", nameJsonOut))
      
      jsonWrte <-
        base::try(jsonlite::write_json(groupData, nameJsonOut, pretty = TRUE, auto_unbox=TRUE),
                  silent = TRUE)
      if (base::class(jsonWrte) == 'try-error') {
        log$error(base::paste0(
          'Cannot write combined file ',
          nameJsonOut,
          '. ',
          attr(jsonWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0('Group JSON written successfully in file: ',
                              nameJsonOut))
      }
      
      
      # ----------------------------------------------------------------------- #
      # Copy over location file
      # ----------------------------------------------------------------------- #
      # Symbolically link each file
      for(idxFileCopy in locFilePths){
        idxFileName <- sub(".*\\/", "", idxFileCopy)
        cmdCopy <- base::paste0('ln -s ',idxFileCopy,' ',base::paste0(idxDirOut,'/',locDir,'/',idxFileName))
        rptCopy <- base::system(cmdCopy)
      }
      
      
      
    } # end loop on HOR.VER
    
  } # End loop around time variables
  
  return()
}

