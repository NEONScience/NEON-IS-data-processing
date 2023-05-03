##############################################################################################
#' @title Unit test for def.gro.meta.R, get metadata/properties from groups json file

#' @description
#' Definition function. Read group json file and return a data frame of metadata/properties 
#' for the group.

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return 
#' #' A data frame with group metadata.

# changelog and author contributions / copyrights
#   Mija Choi (2023-05-03)
#     Updated the test json due to the group_loader change.
#     After the original test written, the group_loader is modified 
#     to have data_product_ID, site, domain and visibility_code in the output json.
#     The test json is updated accordingly

context("Group metadata tests")

test_that("Valid input file. Return group metadata",
          {
            nameFile <- "def.grp.meta/CFGLOC100245.json"
            metaGrp <- NEONprocIS.base::def.grp.meta(NameFile = nameFile)
            expect_true (is.data.frame(metaGrp))
            expect_true (nrow(metaGrp) == 4)
            expect_true (all.equal(names(metaGrp),c("member",'group','active_periods','HOR','VER','data_product_ID','site','domain','visibility_code')))
          })

test_that("Invalid schema. Return error.",
          {
            nameFile <- "def.grp.meta/CFGLOC100245_1.json"
            metaGrp <- try(NEONprocIS.base::def.grp.meta(NameFile = nameFile),silent=FALSE)
            expect_true ( 'try-error' %in% class(metaGrp))
          })

test_that("test the active dates when not empty",
          {
            nameFile <- "def.grp.meta/CFGLOC100245.json"
            timeExpectBgn <- base::as.POSIXct('2022-10-29',tz='GMT')
            timeExpectEnd <- base::as.POSIXct('2020-05-03 12:16:00',tz='GMT')
            metaGrp <- NEONprocIS.base::def.grp.meta(NameFile = nameFile)
            expect_true (is.data.frame(metaGrp$active_periods[[1]]))
            expect_true (all.equal(names(metaGrp$active_periods[[1]]),c('start_date','end_date')))
            expect_true (metaGrp$active_periods[[1]]$start_date[2] == timeExpectBgn)
            expect_true (is.na(metaGrp$active_periods[[1]]$end_date[2]))
            expect_true (metaGrp$active_periods[[3]]$end_date[1] ==  timeExpectEnd)
          })

test_that("test the active dates when empty",
          {
            nameFile <- "def.grp.meta/CFGLOC100245_2.json"
            metaGrp <- NEONprocIS.base::def.grp.meta(NameFile = nameFile)
            expect_true (is.data.frame(metaGrp$active_periods[[1]]))
            expect_true (all.equal(names(metaGrp$active_periods[[1]]),c('start_date','end_date')))
            expect_true (nrow(metaGrp$active_periods[[1]])==0)
          })
