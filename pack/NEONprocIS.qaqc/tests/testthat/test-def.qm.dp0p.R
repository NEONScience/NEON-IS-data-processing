#library(testthat)
#source("R/def.qm.dp0p.R")
test_that("calculate the time on dataHeat State is always false",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=c('QF1','QF2'),qfBeta=c('QF1','QF2'),qfBetaIgnr='QF2')
            qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para)

            testthat::expect_true(is.data.frame(qm))
            expect_true(length(qm) == 3)
            testthat::equals(qm[1]$AlphaQF[1],1)
            testthat::equals(qm[1]$AlphaQF[5], 0)
            testthat::equals(qm[2]$BetaQF[3], 0)
            testthat::equals(qm[2]$BetaQF[5], 1)
            testthat::equals(qm[3]$FinalQF[4], 0)
            testthat::equals(qm[3]$FinalQF[5], 1)

          }

)
test_that("when qf dataframe is not valid, stop processing",
          {
            qf <- data.frame(QF1=c("A","B",1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=c('QF1','QF2'),qfBeta=c('QF1','QF2'),qfBetaIgnr='QF2')
            qm <- try(NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para), silent = TRUE)

            testthat::expect_true((class(qm)[1] == "try-error"))
          }

)
test_that("when qfAlph is null in the Para",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=NULL,qfBeta=c('QF1','QF2'),qfBetaIgnr='QF2')
            qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para)

            testthat::expect_true(is.data.frame(qm))
            expect_true(length(qm) == 3)
            testthat::equals(qm[1]$AlphaQF[1],1)
            testthat::equals(qm[1]$AlphaQF[5], 0)
            testthat::equals(qm[2]$BetaQF[3], 0)
            testthat::equals(qm[2]$BetaQF[5], 1)
            testthat::equals(qm[3]$FinalQF[4], 0)
            testthat::equals(qm[3]$FinalQF[5], 1)

          }
)
test_that("when qfBeta is null in the Para",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=c('QF1','QF2'), qfBeta=NULL, qfBetaIgnr='QF2')
            qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para)
            
            testthat::expect_true(is.data.frame(qm))
            expect_true(length(qm) == 3)
            testthat::equals(qm[1]$AlphaQF[1],1)
            testthat::equals(qm[1]$AlphaQF[5], 0)
            testthat::equals(qm[2]$BetaQF[3], 0)
            testthat::equals(qm[2]$BetaQF[5], 1)
            testthat::equals(qm[3]$FinalQF[4], 0)
            testthat::equals(qm[3]$FinalQF[5], 1)
            
          }
)
test_that("when not all parameter in the qfAlph and qfBeta are not in the qf data frame",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=c('QF1','QF2', 'QF3'),qfBeta=c('QF1','QF2', 'QF4'),qfBetaIgnr='QF2')
            qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para)
            
            testthat::expect_true(is.data.frame(qm))
            expect_true(length(qm) == 3)
            testthat::equals(qm[1]$AlphaQF[1],1)
            testthat::equals(qm[1]$AlphaQF[5], 0)
            testthat::equals(qm[2]$BetaQF[3], 0)
            testthat::equals(qm[2]$BetaQF[5], 1)
            testthat::equals(qm[3]$FinalQF[4], 0)
            testthat::equals(qm[3]$FinalQF[5], 1)
            
          }
          
)