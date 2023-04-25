##############################################################################################
#' @title Create publication tables and apply relevant science review flags

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Wrapper function. Create publication tables and apply any relevant science review flags.
#' Note that no science review flags need be present in order to create publication tables. 
#' 
#' Publication tables are defined in the publication workbook for a product. 
#' Note that terms in data files should match those in the pub workbook. If a term is present in the
#' data but not in the pub workbook, it will not be output. If a term is present in the pub workbook
#' but not in the data, it will be filled with NA.
#' There should be no ambiguity in term names among files in the input repo and in the pub workbooks
#' at the same timing index. The same term name should refer to the same data in both. If there are 
#' multiple columns in the data files with the same term name at the same aggregation interval in the 
#' input repo, the first instance encountered will be used.  
#' 
#' The science review flag is a manually indicated flag which performs forcing actions a linked
#' final quality flag and terms in the publication workbook. This module performs all relevant 
#' actions for any relevance science review flags that are found in the parent repo for the datum.

#' @param DirIn Character value. The input path to the parent directory of all data for the group. 
#' The repo must be in consolidated group format, meaning that this path is the direct parent of 
#' the 'group' directory and all other directories indicated in \code{DirData}.  
#' The input path is structured as follows: #/pfs/BASE_REPO/##/GROUP_ID, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs'.
#'
#' For example:
#' DirIn = "/scratch/pfs/parQuantumline_level1_consolidate/2020/01/02/par-quantum-line_CPER001000"
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param DirData Character vector. The name(s) of the directories (direct children of \code{DirIn})
#' where the L1+ timeseries data resides (default is c('stats','quality_metrics')). Files in these will be 
#' searched for applicable data applicable to any SRFs and modified accordingly. 
#'
#' @param FilePubWb Character vector. The path(s) (relative or absolute) to the publication workbook(s). 
#' The workbooks are used to apply any redactions relevant to the science review flag(s). 
#' 
#' @param TablPub (optional) Character vector. The table(s) in the pub workbook(s) to produce. By default all of them with a discernible 
#' timing index are produced. Ensure that the column names in the data files match those in the pub workbook. 
#' 
#' @param NameVarTimeBgn Character string. The name of the time variable common across all timeseries files indicating 
#' the start time of the aggregation interval. Default is 'startDateTime'.
#' 
#' @param NameVarTimeEnd Character string. The name of the time variable common across all timeseries files indicating 
#' the end time of the aggregation interval. If the data are instantaneous output, set NameVarTimeEnd to the same variable as 
#' NameVarTimeBgn. If any part of the aggregation interval falls within the time range of the SRF, 
#' the SRF will be applied. Note that the aggregation end time and SRF end time are exclusive, meaning they are not considered 
#' part of the interval. Default is 'endDateTime'. 
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the DirData folder(s) in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is). Note that the 'data' directory is automatically
#' populated in the output. Consider specifying other directories such as 'group','science_review_flags', 'location' etc. 
#' if they are desired in the output.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the data with SRF applied, where DirOutBase replaces BASE_REPO of argument
#' \code{DirIn} but otherwise retains the child directory structure of the input path. All data in the input repository
#' will be included in the output. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.pub]{def.srf.aply}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-02-10)
#     Initial creation
##############################################################################################
wrap.pub.tabl.srf <- function(DirIn,
                          DirOutBase,
                          DirData=c('stats','quality_metrics'),
                          FilePubWb,
                          TablPub=NULL,
                          NameVarTimeBgn='startDateTime',
                          NameVarTimeEnd='endDateTime',
                          DirSubCopy=NULL,
                          log=NULL
){
  
  library(dplyr)
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutData <- fs::path(dirOut,'data')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = 'data',
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    if('data' %in% DirSubCopy){
      LnkSubObj <- TRUE
    } else {
      LnkSubObj <- FALSE
    }
    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=LnkSubObj,
                                       log=log)
  }    
  
  # Load the groups file 
  fileGrp <- base::list.files(fs::path(DirIn, 'group'), 
                              full.names = TRUE)
  if(base::length(fileGrp) == 0){
    log$error(base::paste0(
      'At least 1 groups file is required in directory ',
      fs::path(DirIn, 'group'),
      '. Check input repository.'
    ))
    stop()
  }
  
  # Will use the group properties to restrict SITE, HOR and VER of the SRF records
  metaGrp <- NEONprocIS.base::def.grp.meta(NameFile=fileGrp[1])
  hor <- metaGrp$HOR[1]
  ver <- metaGrp$VER[1]
  site <- metaGrp$site[1]
  domn <- metaGrp$domain[1]
  idDpGrp <- metaGrp$data_product_ID[1]
  
  if(base::is.na(idDpGrp)){
    log$error(base::paste0(
      'No data products indicated for datum ',
      DirIn,
      '. Cannot proceed.'
    ))
    stop()
  }
  idDpGrp <- base::strsplit(idDpGrp,'|',fixed=TRUE)[[1]]
  
  # Get listing of SRF directory. 
  fileSrf <- base::list.files(fs::path(DirIn, 'science_review_flags'), 
                                  full.names = TRUE)
  log$debug(
    base::paste0(
      base::length(fileSrf), ' SRF file(s) found for datum ', DirIn
  ))
  if(base::length(fileSrf) > 0){
    # Load the SRFs (all files in the SRF directory)
    srfList <- base::lapply(fileSrf,NEONprocIS.pub::def.read.srf)
    srf <- base::do.call(base::rbind,srfList)
    log$debug(
      base::paste0(
        base::nrow(srf), ' SRF records found for datum ', DirIn
      ))
    
    # Restrict SRFs to those matching this group
    idDp <- srf$measurement_stream_name
    domnSrf <- base::substr(idDp,start=6,stop=8)
    siteSrf <- base::substr(idDp,start=10,stop=13)
    horSrf <- base::substr(idDp,start=35,stop=37)
    verSrf <- base::substr(idDp,start=39,stop=41)
    srf <- srf[domnSrf==domn & siteSrf==site & horSrf==hor & verSrf==ver,]
    log$debug(
      base::paste0(
        base::nrow(srf), ' SRF records match group metadata for datum ', DirIn
      ))
    
    # Get the final quality flag for each SRF
    # The naming convention for science review flags that modify a final quality flag is <final quality flag term>SciRvw
    srf$qfFinl <- NEONprocIS.pub::def.srf.term.qf.finl(termSrf=srf$srf_term_name,log=log)
    srf$tmi <- base::substr(srf$measurement_stream_name,start=43,stop=45) # Also get timing index

  } else {
    srf <- NULL
  }
  
  # Load the pub WBs 
  pubWb <- NEONprocIS.pub::def.read.pub.wb(NameFile=FilePubWb)
  
  # Constrain to the desired pub tables
  if(base::is.null(TablPub)){
    TablPub <- base::unique(pubWb$table)
  }
  pubWb <- pubWb[pubWb$table %in% TablPub,]
  
  # Issue an error if no matching pub tables
  if(base::nrow(pubWb) == 0){
    log$error(base::paste0('The publication workbook(s) contain no matches to requested pub table(s). Datum:',
                          DirIn,
                          '. Pub Workbook file(s): ',
                          base::paste0(FilePubWb,collapse=','),
                          '. Requested pub table(s): ',
                          base::paste0(TablPub,collapse=',')
    ))
    stop()
  }

  # Constrain pub wb to the data products of this group
  idDpPubWb <- pubWb$dpID
  idDpPubWb <- base::substr(idDpPubWb,start=15,stop=27) # Strip off NEON.DOM.SITE. for matching with DPs of group
  pubWb <- pubWb[idDpPubWb %in% idDpGrp,]
  
  # Issue an error if no matching products
  if(base::nrow(pubWb) == 0){
    log$error(base::paste0('The publication workbook(s) contain no matches to the data product of the group. Datum:',
                           DirIn,
                           '. Pub Workbook file(s): ',
                           base::paste0(FilePubWb,collapse=','),
                           '. Data products in the pub workbook(s): ',
                           base::paste0(base::unique(idDpPubWb),collapse=','),
                           '. Data products of the group: ',
                           base::paste0(idDpGrp,collapse=',')
    ))
    stop()
  } else {
    log$debug(base::paste0('The publication workbook(s) contain the following matches to the data product of the group: ',
                           base::paste0(base::unique(base::substr(pubWb$dpID,start=15,stop=27)),collapse=','),
                           ' for datum: ',
                           DirIn
    ))
  }
  
  # Discern the timing index of each pub table
  tmiTablPub <- base::unlist(
                  base::lapply(TablPub,
                               FUN=function(TablPubIdx){
                               dp <- pubWb$DPNumber[pubWb$table==TablPubIdx]
                               tmi <- base::substr(dp,start=43,stop=45)
                               tmiTabl <- base::setdiff(base::unique(tmi),c(NA,""))
                               if(base::length(tmiTabl)== 0){tmiTabl <- NA}
                               return(tmiTabl)
                               })
  )
  base::names(tmiTablPub) <- TablPub
  
  # Take stock of our data files. File naming convention must be *_TMI.ext
  fileData <- base::list.files(fs::path(DirIn, DirData),full.names=TRUE)
  nameFileData <- base::unlist(
                  base::lapply(strsplit(fileData,'/'),
                    utils::tail,
                    n=1)
                  )
  idxTmi <-  base::regexpr(pattern='_[0-9]{3}\\.',text=nameFileData)
  tmiFile <-  base::substr(nameFileData,start=idxTmi+1,stop=idxTmi+3)
 
  # Create each desired pub wb table and apply SRFs
  for(tmiIdx in base::unique(base::setdiff(tmiTablPub,NA))){
    
    tablPubTmiIdx <- base::names(tmiTablPub[!base::is.na(tmiTablPub) & tmiTablPub==tmiIdx]) # Pub tables with this TMI
    fileDataTmiIdx <- fileData[tmiFile==tmiIdx] # Files with this tmi
    
    # What if no files at this tmi? Issue warning and skip the table.
    if(base::length(fileDataTmiIdx) ==  0){
      log$warn(base::paste0(
        'Pub tables ',
        base::paste0(tablPubTmiIdx,collapse=','),
        ' have a timing index (',
        tmiIdx,
        ') that does not match timing indices of the data files. These tables will not be produced.'
      ))
      next
    }
    
    # Load data into an arrow dataset. Essentially collects metadata about the data files at this tmi without loading
    data <- base::lapply(fileDataTmiIdx,arrow::open_dataset)

    # Create each pub table with this tmi
    for(tablPubIdx in base::names(tmiTablPub[!base::is.na(tmiTablPub) & tmiTablPub==tmiIdx])){
      
      # Constrain pub workbook to table of interest
      pubWbIdx <- pubWb[pubWb$table==tablPubIdx,]
      
      # Remove duplicated field names in the pub table (this can happen when multiple pub workbooks are combined prior to input).
      pubWbIdx <- pubWbIdx[!base::duplicated(pubWbIdx$fieldName),]
      
      # Create the pub table
      rptPub <- NEONprocIS.pub::def.pub.tabl.crea(data=data,
                                                  pubWb=pubWbIdx,
                                                  log=log)
      dataTabl <- rptPub$dataTabl
      nameVarMtch <- rptPub$nameVarMtch
      
      # Check if we only have start and end times. If only those variables, issue a warning and skip.
      if(base::length(nameVarMtch) == 0 || base::all(nameVarMtch %in% c(NameVarTimeBgn,NameVarTimeEnd))){
        log$warn(base::paste0(
          'The field names for pub table ',
          tablPubIdx,
          ' are not found in any data files (other than start/end times). This table will not be produced.'
        ))
        next
      }

      # Apply the science review flags
      if(!base::is.null(srf) && base::nrow(srf) > 0){
        dataTabl <- NEONprocIS.pub::def.srf.aply(srf=srf,
                                                 dataTabl=dataTabl,
                                                 pubWbTabl=pubWbIdx,
                                                 NameVarTimeBgn=NameVarTimeBgn,
                                                 NameVarTimeEnd=NameVarTimeEnd,
                                                 log = log)
      }
      # Create the output schema 
      schmTablPub <- NEONprocIS.pub::def.schm.parq.from.pub.wb(pubWb=pubWbIdx)
    
      # Write out the data for this pub table. File naming convention is GROUPID_YYYY-MM-DD_TABLE_TMI.parquet
      fileOut <- base::paste0(utils::tail(InfoDirIn$dirSplt,1),
                                '_',
                                base::format(InfoDirIn$time,'%Y-%m-%d'),
                                '_',
                                tablPubIdx,
                                '_',
                                tmiIdx,
                                '.parquet')
      pathFileOut <- fs::path(dirOutData,fileOut)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
            data = dataTabl,
            NameFile = pathFileOut,
            NameFileSchm = NULL,
            Schm = schmTablPub[[tablPubIdx]],
            log=log
        ),
        silent = TRUE)
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write pub table ',
          tablPubIdx ,
          'to file ',
          pathFileOut,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Pub table ',
          tablPubIdx ,
          ' written to file ',
          pathFileOut
          ))
      }
      
    } # End loop around pub tables for the tmi
    
  } # End loop around tmi

  
  return()
} # End loop around datum paths
