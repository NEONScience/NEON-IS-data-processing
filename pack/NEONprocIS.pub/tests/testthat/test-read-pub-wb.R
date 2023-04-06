##############################################################################################
#' @title Unit test of def.read.pub.wb.R, 
#' Read Publication Workbook

#' @author 
#' Mija Choi \email{choim@battelleecology.org}
#' 
#' @description 
#' Definition function. Read in one or more tab-delimited publication workbooks. 

#' @param NameFile Character array. Name(s) (including relative or absolute path) of publication workbook file(s).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A data frame with all input publication workbooks combined.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' pubWb <- NEONprocIS.pub::def.read.pub.wb(NameFile=c('/scratch/mypubWb1.txt','/scratch/mypubWb2.txt'))

# changelog and author contributions / copyrights
#   Mija Choi (2023-04-05)
#      Original Creation
##############################################################################################
test_that("   Testing def.read.srf.R, definition function. Read science review file",{

  wk_dir <- getwd()
  
  #1. Happy path
  file1 = 'pfs/pubWb/PublicationWorkbook_elevSurfacewater.txt'
  file2 = 'pfs/pubWb/PublicationWorkbook_parQuantumLine.txt'
  pubWb_file1 <- NEONprocIS.pub::def.read.pub.wb(NameFile=file1)
  pubWb_file2 <- NEONprocIS.pub::def.read.pub.wb(NameFile=file2)
  pubWb <- NEONprocIS.pub::def.read.pub.wb(NameFile=c(file1,file2))
  expect_true(nrow(pubWb) == (nrow(pubWb_file1) + nrow(pubWb_file2)))
  
  #2.
  pubWb <- try(NEONprocIS.pub::def.read.pub.wb(
    NameFile=c(file1,file2, 'pfs/pubWb/par-quantum-line_CPER001000_2023-02-03_PARQL_1min_001.parquet')), 
    silent=TRUE)
})

