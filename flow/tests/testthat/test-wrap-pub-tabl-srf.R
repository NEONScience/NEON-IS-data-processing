##############################################################################################
#' @title Unit test of wrap.pub.tabl.srf.R, 
#' Create publication tables and apply relevant science review flags

#' @author
#' Mija Choi \email{choim@battelleecology.org}

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
#' 
# changelog and author contributions / copyrights
#   Mija Choi (2023-03-23)
#      Original Creation
##############################################################################################
test_that("   Testing def.read.srf.R, definition function. Read science review file",{

  source('../../flow.pub.tabl.srf/wrap.pub.tabl.srf.R')
  
  DirOutBase = "pfs/out"
  DirData=c('stats','quality_metrics')
  TimeBgn ='startDateTime'
  TimeEnd ='endDateTime'
  DirSubCopydata = 'data'
  DirSubCopygroup = 'group'
  DirSubCopynull = NULL

  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
 
  #1. Happy path test 
  
  dirInBase = "pfs"
  baseRepo = 'swPhysical_level1_group_consolidate'
  dateDir = '2020/01/02'
  groupRepo = 'sw-physical_ARIK101100'
  DirIn = fs::path(dirInBase,baseRepo,dateDir,groupRepo)
  FilePubWb = 'pfs/pubWb/PublicationWorkbook_elevSurfacewater.txt'
  TablPub = 'EOS_30_min' 
  
  wrap.pub.tabl.srf(DirIn=DirIn,
                DirOutBase=DirOutBase,
                DirData=c('stats','quality_metrics'),
                FilePubWb=FilePubWb,
                TablPub=TablPub,
                NameVarTimeBgn=TimeBgn,
                NameVarTimeEnd=TimeEnd,
                DirSubCopy= DirSubCopydata)
  
  DirOutput = fs::path(DirOutBase,dateDir,groupRepo,'data')
  expect_true(file.exists(DirOutput, recursive = TRUE))
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  wrap.pub.tabl.srf(DirIn=DirIn,
                    DirOutBase=DirOutBase,
                    DirData=c('stats','quality_metrics'),
                    FilePubWb=FilePubWb,
                    TablPub=TablPub,
                    NameVarTimeBgn=TimeBgn,
                    NameVarTimeEnd=TimeEnd,
                    DirSubCopy= DirSubCopygroup)
  
  DirOutput = fs::path(DirOutBase,dateDir,groupRepo,'group')
  expect_true(file.exists(DirOutput, recursive = TRUE))
  
  #2. an error if 
  
  dateDir = '2020/01/02'
  groupRepo = 'sw-physical_PRLA102100'
  DirIn = fs::path(dirInBase,baseRepo,dateDir,groupRepo)
  FilePubWb = 'pfs/pubWb/PublicationWorkbook_parQuantumLine.txt'
  TablPub = 'PARQL_1_min'
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  try(wrap.pub.tabl.srf(DirIn=DirIn,
                        DirOutBase=DirOutBase,
                        DirData=c('stats','quality_metrics'),
                        FilePubWb=FilePubWb,
                        TablPub=TablPub,
                        NameVarTimeBgn=TimeBgn,
                        NameVarTimeEnd=TimeEnd,
                        DirSubCopy=DirSubCopynull), silent=TRUE)
  
  #3. an error if no matching pub tables 
  
  DirIn = "pfs/swPhysical_level1_group_consolidate/2020/01/02/sw-physical_PRLA102100"
  FilePubWb = 'pfs/pubWb/PublicationWorkbook_parQuantumLine.txt'
  TablPub = 'PARQL_1min'
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  try(wrap.pub.tabl.srf(DirIn=DirIn,
                        DirOutBase=DirOutBase,
                        DirData=c('stats','quality_metrics'),
                        FilePubWb=FilePubWb,
                        TablPub=TablPub,
                        NameVarTimeBgn=TimeBgn,
                        NameVarTimeEnd=TimeEnd,
                        DirSubCopy=DirSubCopy), silent=TRUE)
  
  #4. an error if no matching pub tables
  
  DirIn = "pfs/swPhysical_level1_group_consolidate/2020/01/02/sw-physical_ARIK101100"
  TablPub = 'EOS_30_min'
  FilePubWb = 'pfs/pubWb/PublicationWorkbook_elevSurfacewater.txt'
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  try(wrap.pub.tabl.srf(DirIn=DirIn,
                    DirOutBase=DirOutBase,
                    DirData=c('stats','quality_metrics'),
                    FilePubWb=FilePubWb,
                    TablPub=TablPub,
                    NameVarTimeBgn=TimeBgn,
                    NameVarTimeEnd=TimeEnd,
                    DirSubCopy=DirSubCopy), silent=TRUE)
  

})
