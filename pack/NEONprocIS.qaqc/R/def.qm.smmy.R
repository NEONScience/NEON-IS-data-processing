##############################################################################################
#' @title Compute alpha & beta quality metrics and final quality flag

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

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

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON Algorithm Theoretical Basis Document: Quality Flags and Quality Metrics for TIS Data Products (NEON.DOC.001113) \cr
#' Smith, D.E., Metzger, S., and Taylor, J.R.: A transparent and transferable framework for tracking quality information in 
#' large datasets. PLoS ONE, 9(11), e112249.doi:10.1371/journal.pone.0112249, 2014. \cr 

#' @keywords Currently none

#' @examples
#' qfSmmy <- data.frame(AlphaQF=c(1,0,0,1,1),BetaQF=c(0,1,0,0,1),stringsAsFactors=FALSE)
#' def.qm.smmy(qfSmmy=qfSmmy)

#' @seealso \code{\link[NEONprocIS.qaqc]{def.qm.dp0p}}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-12-07)
#     original creation
##############################################################################################
def.qm.smmy <- function(qfSmmy,
                        Thsh=0.2,
                        WghtAlphBeta=c(2,1),
                        log=NULL){
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Validate the inputs
  chkInp <- NEONprocIS.base::def.validate.dataframe(dfIn=qfSmmy,
                                                    TestNumc=TRUE,
                                                    TestNameCol=c('AlphaQF','BetaQF'),
                                                    log=log)
  if(chkInp == FALSE){
    log$fatal('Input qfSmmy to function NEONprocIS.qaqc::def.qm.smmy must be a numeric data frame with minimum column inputs of AlphaQF and BetaQF.')
    stop()
  }
  if(!base::is.numeric(Thsh)){
    log$fatal('Input Thsh to function NEONprocIS.qaqc::def.qm.smmy must be numeric.')
    stop()  
  }
  if(!base::is.numeric(WghtAlphBeta) && base::length(WghtAlphBeta) != 2){
    log$fatal('Input WghtAlphBeta to function NEONprocIS.qaqc::def.qm.smmy must be a numeric vector of length 2.')
    stop()    
  }
  
  # Compute alpha and beta quality metrics
  numRow <- base::nrow(qfSmmy)
  qmSmmy <- base::colSums(x=qfSmmy==1,na.rm=TRUE)/numRow*100
  
  # Compute final quality flag
  if ((WghtAlphBeta[1]*qmSmmy['AlphaQF']/100) + (WghtAlphBeta[2]*qmSmmy['BetaQF']/100) >= Thsh) {
    qmSmmy['FinalQF'] <- 1
  } else {
    qmSmmy['FinalQF'] <- 0
  }

  qmSmmy <- qmSmmy[c('AlphaQF','BetaQF','FinalQF')]
  names(qmSmmy) <- c('AlphaQM','BetaQM','FinalQF')
  
  return(qmSmmy)
}
