##############################################################################################
#' @title Rename columns of a dataframe based on variable mapping

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Rename the columns of a data frame based on variable name mapping.

#' @param df Data frame.
#' @param mappNameVar A data frame providing the input/output variable names, as output from
#' NEONprocIS.base::mapp.var.in.out. Columns are:\cr
#' \code{nameVarIn} Character. Input column name.\cr
#' \code{nameVarOut} Character. Input column name.\cr
#' Defaults to NULL. In which case the output column names will match the input column names.
#' Any column names in df not found in mappNameVar$nameVarIn will retain their original names.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame with renamed columns.

#' @references Currently none

#' @keywords Currently none

#' @examples
#' NEONprocIS.base::def.df.renm(df=data.frame(X=c(1,2,3),Y=c(4,5,6)),
#'                              mappNameVar=data.frame(
#'                                  nameVarIn=c('X','Y'),
#'                                  nameVarOut=c('A','B'),
#'                                  stringsAsFactors=FALSE))

#' @seealso \link[NEONprocIS.cal]{def.var.mapp.in.out}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-14)
#     original creation
##############################################################################################
def.df.renm <- function(df,
                        mappNameVar = NULL,
                        log = NULL) {
  # initialize logging if necessary
  #browser()
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  varDf <- base::names(df)
  
  varQfOut <-
    base::unlist(base::lapply(
      varDf,
      FUN = function(idxNameIn) {
        idxNameOut <-
          mappNameVar$nameVarOut[mappNameVar$nameVarIn == idxNameIn]
        if (base::is.null(idxNameOut)) {
          idxNameOut <- idxNameIn
        }
        return(idxNameOut)
      }
    ))
  
  base::names(df) <- varQfOut
  
  return(df)
  
}
