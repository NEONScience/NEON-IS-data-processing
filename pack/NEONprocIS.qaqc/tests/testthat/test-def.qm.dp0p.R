##############################################################################################
#' @title Unit test for compute "instantaneous" alpha, beta, and final quality flags

#' @author
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Definition function. Aggregate quality flags to produce alpha, beta, and final quality flags
#' for each L0' (instantaneous) record. The alpha flag is 1 when any of a set of selected
#' flags have a value of 1 (fail). The beta flag is 1 when any of a set of selected
#' flags cannot be evaluated (have a value of -1). If either the alpha flag or beta flag are raised,
#' the final quality is raised (value of 1).

#' @param qf Data frame of named quality flags (values of -1,0,1)
#' @param Para (optional) A named list of:\cr
#' \code{qfAlph} A character vector of the names of quality flags in \code{qf} that are to be used
#' to compute AlphaQF. If any of these flags have a value of 1 for a given record, AlphaQF will be
#' 1 for that record. May be NULL (or not in the list), in which case all flags found in \code{qf}
#' will be used to compute AlphaQF.\cr
#' \code{qfBeta} A character vector of the names of quality flags in \code{qf} that are to be used
#' to compute BetaQF. If any of these flags have a value of -1 for a given record, BetaQF will be
#' 1 for that record. May be NULL (or not in the list), in which case all flags found in \code{qf}
#' will be used to compute BetaQF. Note that this action may be modified by the \code{qfBetaIgnr}
#' list element below\cr
#' \code{qfBetaIgnr} A character vector of the names of quality flags in \code{qf} that, if any of
#' their values equals 1 for a particular record, the betaQF flag for that record is automatically
#' set to 0 (ignores the values of all other flags). May be NULL, (or not in the list), in which
#' case this argument will be ignored.
#' Note that the entire Para argument defaults to NULL, which will follow the default actions
#' described above.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of the alpha, beta, and final quality flags

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
#' Para <- list(qfAlph=c('QF1','QF2'),qfBeta=c('QF1','QF2'),qfBetaIgnr='QF2')
#' def.qm.dp0p(qf=qf,Para=Para)

# changelog and author contributions / copyrights
#   Mija Choi (2022-03-29)
#     added test of Error check - ensure that the contributing flags are included in the data
##############################################################################################
# Define test context
context("\n       | Unit test of compute instantaneous alpha, beta, and final quality flags \n")

test_that("calculate the time on dataHeat State is always false",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=c('QF1','QF2'),qfBeta=c('QF1','QF2'),qfBetaIgnr='QF2')
            qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para)

            testthat::expect_true(is.data.frame(qm))
            expect_true(length(qm) == 3)
            testthat::expect_equal(qm[1]$AlphaQF[1],1)
            testthat::expect_equal(qm[1]$AlphaQF[5], 0)
            testthat::expect_equal(qm[2]$BetaQF[3], 0)
            testthat::expect_equal(qm[2]$BetaQF[5], 1)
            testthat::expect_equal(qm[3]$FinalQF[4], 0)
            testthat::expect_equal(qm[3]$FinalQF[5], 1)

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
            testthat::expect_equal(qm[1]$AlphaQF[1],1)
            testthat::expect_equal(qm[1]$AlphaQF[5], 0)
            testthat::expect_equal(qm[2]$BetaQF[3], 0)
            testthat::expect_equal(qm[2]$BetaQF[5], 1)
            testthat::expect_equal(qm[3]$FinalQF[4], 0)
            testthat::expect_equal(qm[3]$FinalQF[5], 1)

          }
)
test_that("when qfBeta is null in the Para",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=c('QF1','QF2'), qfBeta=NULL, qfBetaIgnr='QF2')
            qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para)
            
            testthat::expect_true(is.data.frame(qm))
            expect_true(length(qm) == 3)
            testthat::expect_equal(qm[1]$AlphaQF[1],1)
            testthat::expect_equal(qm[1]$AlphaQF[5], 0)
            testthat::expect_equal(qm[2]$BetaQF[3], 0)
            testthat::expect_equal(qm[2]$BetaQF[5], 1)
            testthat::expect_equal(qm[3]$FinalQF[4], 0)
            testthat::expect_equal(qm[3]$FinalQF[5], 1)
            
          }
)
test_that("when not all parameter in the qfAlph and qfBeta are not in the qf data frame",
          {
            qf <- data.frame(QF1=c(1,-1,1,0,-1),QF2=c(-1,1,0,0,0),stringsAsFactors=FALSE)
            Para <- list(qfAlph=c('QF1','QF2', 'QF3'),qfBeta=c('QF1','QF2', 'QF4'),qfBetaIgnr='QF2')
            qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf,Para=Para)
            
            testthat::expect_true(is.data.frame(qm))
            testthat::expect_true(length(qm) == 3)
            testthat::expect_equal(qm[1]$AlphaQF[1],1)
            testthat::expect_equal(qm[1]$AlphaQF[5], 0)
            testthat::expect_equal(qm[2]$BetaQF[3], 0)
            testthat::expect_equal(qm[2]$BetaQF[5], 1)
            testthat::expect_equal(qm[3]$FinalQF[4], 0)
            testthat::expect_equal(qm[3]$FinalQF[5], 1)
            
          }
)
 
test_that("Error check - ensure that the contributing flags are included in the data",
          {
          qf3 <- data.frame(QF3=c(1,-1,1,0,-1),QF4=c(-1,1,0,0,0),stringsAsFactors=FALSE)
          Para <- list(qfAlph=c('QF3','QF4'),qfBeta=c('QF3','QF4'),qfBetaIgnr='QF3')
          qm <- NEONprocIS.qaqc::def.qm.dp0p(qf=qf3,Para=Para)
          testthat::expect_equal(qm$AlphaQF[1],1)
          testthat::expect_equal(qm$AlphaQF[1],1)
          testthat::expect_equal(qm$AlphaQF[5],0)
          testthat::expect_equal(qm$BetaQF[5],1)
          testthat::expect_equal(sum(qm$FinalQF),4)
          
          }
)
