##############################################################################################
#' @title Compute alpha & beta quality metrics and final quality flag

#' @author
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Definition function. Aggregate alpha and beta summary flags into alpha & beta quality metrics
#' and the final quality flag.
#' The alpha flag is 1 when any of a set of selected
#' flags have a value of 1 (fail). The beta flag is 1 when any of a set of selected
#' flags cannot be evaluated (have a value of -1). If either the alpha flag or beta flag are raised,
#' the final quality is raised (value of 1).

#' @param qfSmmy Numeric data frame of alpha and beta summary flags for each record, as produced by
#' NEONprocIS.qaqc::def.qm.dp0p, with named columns:\cr
#' \code{AlphaQF} 0 or 1 indicating (1) at least one of the contituent quality flags was raised high (1)\cr
#' \code{BetaQF} 0 or 1 indicating (1) at least one of the contituent quality flags could not be evaluated (-1)\cr
#' All records will be aggregated into a single set of summary metrics
#' @param Thsh Numeric value. The threshold fraction for the sum of the alpha and beta quality
#' metrics multiplied by the respective weights given in argument WghtAlphaBeta (below) at and above
#' which triggers the final quality flag. Default value = 0.2.
#' @param WghtAlphBeta 2-element numeric vector of weights for the alpha and beta quality metrics, respectively.
#' The alpha and beta quality metrics (in fraction form) are multiplied by these respective weights and summed.
#' If the resultant value is greater than the threshold value set in the Thsh argument, the final quality flag is
#' raised. Default is c(2,1).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named numeric vector of:\cr
#' \code{AlphaQM} The alpha quality metric, indicating the percentage of input records in which the
#' alpha quality flag was raised.\cr
#' \code{BetaQM} The beta quality metric, indicating the percentage of input records in which the
#' beta quality flag was raised.\cr
#' \code{FinalQF} The final quality flag, computed via the description for input parameter \code{WghtAlphBeta}.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON Algorithm Theoretical Basis Document: Quality Flags and Quality Metrics for TIS Data Products (NEON.DOC.001113) \cr
#' Smith, D.E., Metzger, S., and Taylor, J.R.: A transparent and transferable framework for tracking quality information in
#' large datasets. PLoS ONE, 9(11), e112249.doi:10.1371/journal.pone.0112249, 2014. \cr

#' @keywords Currently none

#' @examples
#' qfSmmy <- data.frame(AlphaQF=c(1,0,0,1,1),BetaQF=c(0,1,0,0,1),stringsAsFactors=FALSE)
#' def.qm.smmy(qfSmmy=qfSmmy)
#'
# changelog and author contributions / copyrights
#   Mija Choi (2021-12-21)
#     original creation
##############################################################################################
# Define test context
context("\n       | Unit test of Compute alpha & beta quality metrics and final quality flag \n")

test_that("Unit test of def.qm.smmy.R", {
  qfSmmy <-
    data.frame(
      AlphaQF = c(1, 0, 0, 1, 1),
      BetaQF = c(0, 1, 0, 0, 1),
      stringsAsFactors = FALSE
    )
  
  colNames_returned <- c('AlphaQM', 'BetaQM', 'FinalQF')
  
  # Test 1: happy path
  qm_smmy_returned <- NEONprocIS.qaqc::def.qm.smmy(qfSmmy = qfSmmy,
                                                   Thsh = 0.2,
                                                   WghtAlphBeta = c(2, 1))
  
  testthat::expect_true(all(names(qm_smmy_returned) %in% colNames_returned))
  testthat::expect_true(qm_smmy_returned[1] == 60)
  
  # Test 2: Thsh is bigger than ((WghtAlphBeta[1]*qmSmmy['AlphaQF']/100) + (WghtAlphBeta[2]*qmSmmy['BetaQF']/100)
  qm_smmy_returned <- NEONprocIS.qaqc::def.qm.smmy(qfSmmy = qfSmmy,
                                                   Thsh = 2.2, WghtAlphBeta = c(2, 1))
  
  testthat::expect_true(all(names(qm_smmy_returned) %in% colNames_returned))
  testthat::expect_true(qm_smmy_returned[1] == 60)
  
  # Test 3: qfSmmy does not have a column, 'AlphaQM'
  
  qfSmmy_lessCol <- data.frame(BetaQF = c(0, 1, 0, 0, 1), stringsAsFactors = FALSE)
  qm_smmy_returned <- try(NEONprocIS.qaqc::def.qm.smmy(qfSmmy = qfSmmy_lessCol,
                                     Thsh = 0.2, WghtAlphBeta = c(2, 1)),  silent = TRUE)
  
  testthat::expect_true((class(qm_smmy_returned)[1] == "try-error"))
  
  # Test 4: Thsh is not numeric
  
  qm_smmy_returned <- try(NEONprocIS.qaqc::def.qm.smmy(qfSmmy = qfSmmy,
                                     Thsh = "0.2", WghtAlphBeta = c(2, 1)), silent = TRUE)
  
  testthat::expect_true((class(qm_smmy_returned)[1] == "try-error"))
  
  # Test 5: WghtAlphBeta is not numeric
  
  qm_smmy_returned <- try(NEONprocIS.qaqc::def.qm.smmy(qfSmmy = qfSmmy,
                                     Thsh = 0.2, WghtAlphBeta = c("2")),  silent = TRUE)
  
  testthat::expect_true((class(qm_smmy_returned)[1] == "try-error"))
})
