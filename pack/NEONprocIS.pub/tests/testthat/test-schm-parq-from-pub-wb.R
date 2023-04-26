##############################################################################################
#' @title Unit test of def.schm.parq.from.pub.wb.R,
#' Create Parquet schema from publication workbook

#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' Definition function. Create a arrow schema object from a publication workbook. The fields in the
#' schema for each table will be crafted in the order the terms are found in the pub workbook,
#' with any duplicates removed. Note that the rank column in the pub workbook is not heeded in
#' order to avoid issues when multiple pub workbooks are combined.

#' @param NameFile String. Optional (either one of NameFile or PubWb must be entered).
#' Name (including relative or absolute path) of publication workbook file.
#' @param pubWb Data frame. Optional (either one of NameFile or PubWb must be entered). A data frame
#' of the pub workbook, as read in by def.read.pub.wb.
#' @param TablPub Character vector. The table(s) in the pub workbook(s) to produce. By default all of them
#' are produced.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#'
#' @return An Apache Arrow Schema object containing a parquet schema representing the table(s) in the
#' publication workbook.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[arrow]{data-type}
#' @seealso \link[NEONprocIS.pub]{def.read.pub.wb}
# changelog and author contributions / copyrights
#   Mija Choi (2023-04-24)
#      Original Creation
##############################################################################################
test_that("   Testing def.read.srf.R, definition function. Read science review file",
          {
            #1. A relative path of publication workbook file and typeData == unsigned integer
            file1 = 'pfs/pubWb/PublicationWorkbook_parQuantumLine.txt'
            returnedSchm = def.schm.parq.from.pub.wb(NameFile = file1)
            expect_true(!is.null(returnedSchm$PARQL_1min) && !is.null(returnedSchm$PARQL_30min))
            
            #2. typeData == integer
            integerFile1 = 'pfs/pubWb/integerPublicationWorkbook_parQuantumLine.txt'
            returnedSchm = def.schm.parq.from.pub.wb(NameFile = integerFile1)
            expect_true(!is.null(returnedSchm$PARQL_1min) && !is.null(returnedSchm$PARQL_30min))
            
            #3. No Quality flags
            noQfFile1 = 'pfs/pubWb/noQfPublicationWorkbook_parQuantumLine.txt'
            returnedSchm = def.schm.parq.from.pub.wb(NameFile = noQfFile1)
            expect_true(!is.null(returnedSchm$PARQL_1min) && !is.null(returnedSchm$PARQL_30min))
            
            #4. typeData == boolean
            booleanFile1 = 'pfs/pubWb/booleanPublicationWorkbook_parQuantumLine.txt'
            returnedSchm = def.schm.parq.from.pub.wb(NameFile = booleanFile1)
            
            #3. Unsigned and No Quality flags, QF
            unsignedNoQfFile1 = 'pfs/pubWb/unsignedNoQfPb_parQuantumLine.txt'
            returnedSchm = def.schm.parq.from.pub.wb(NameFile = unsignedNoQfFile1)
            
            #4.  typeData == none
            noneFile1 = 'pfs/pubWb/nonePublicationWorkbook_parQuantumLine.txt'
            returnedSchm = def.schm.parq.from.pub.wb(NameFile = noneFile1)
            
            #5. A relative path of publication workbook file with "PARQL_1min" and "PARQL_30min", and no matching TablPub =="EOS_1min" passed in
            
            returnedSchm = def.schm.parq.from.pub.wb(NameFile = file1, TablPub = "EOS_1min")
            expect_true(is.null(returnedSchm$PARQL_1min) && is.null(returnedSchm$PARQL_30min))
            
            #6. A data frame of the pub workbook, as read in by def.read.pub.wb.
            
            pubWb_file1 <- NEONprocIS.pub::def.read.pub.wb(NameFile = file1)
            returnedSchm = def.schm.parq.from.pub.wb(pubWb = pubWb_file1)
            
            #7. No params passed
            returnedSchm = try(def.schm.parq.from.pub.wb(), silent = TRUE)
          })
