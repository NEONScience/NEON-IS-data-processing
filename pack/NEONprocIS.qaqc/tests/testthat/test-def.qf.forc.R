#library(testthat)
#source("R/def.qf.forc.R")

test_that("if not valid dataframe, return false",
          {
            qf <- "test"
            returnValue <- try(NEONprocIS.qaqc::def.qf.forc(qf = qf, nameQfDrve="QF1", valuDrve=1, nameQfForc="QF2", valuForc=2), silent = TRUE)
            testthat::expect_true((class(returnValue)[1] == "try-error"))

          })

test_that("when nameQfDrve is not in qf, return qf",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            nameQfDrve <- "QF3"
            valuDrve <- 1
            nameQfForc <- "QF2"
            valuForc <- 5
            returnValue <- try(NEONprocIS.qaqc::def.qf.forc(qf = qf, nameQfDrve=nameQfDrve, valuDrve=1, nameQfForc=nameQfForc, valuForc=valuForc), silent = TRUE)
            testthat::expect_equal(returnValue, qf)
          })

test_that("when nameQfForc is not in qf, return qf",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            nameQfDrve <- "QF1"
            valuDrve <- 1
            nameQfForc <- "QF3"
            valuForc <- 5
            returnValue <- try(NEONprocIS.qaqc::def.qf.forc(qf = qf, nameQfDrve=nameQfDrve, valuDrve=1, nameQfForc=nameQfForc, valuForc=valuForc), silent = TRUE)
            testthat::expect_equal(returnValue, qf)
          })

test_that("when one of the nameQfForc is not in qf",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            nameQfDrve <- "QF1"
            valuDrve <- 1
            nameQfForc <- c("QF3", "QF2")
            valuForc <- 5
            returnValue <- try(NEONprocIS.qaqc::def.qf.forc(qf = qf, nameQfDrve=nameQfDrve, valuDrve=1, nameQfForc=nameQfForc, valuForc=valuForc), silent = TRUE)
            testthat::expect_equal(returnValue$QF2[1], valuForc)
            testthat::expect_equal(returnValue$QF1[3], 1)
            testthat::expect_equal(returnValue$QF1[5], -1)
            testthat::expect_equal(returnValue$QF2[3], valuForc)
           
          })
