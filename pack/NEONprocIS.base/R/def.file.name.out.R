##############################################################################################
#' @title Construct an output file name from an input file name

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Construct an output file name from an input file name, adding a prefix, 
#' appendix, extension, or all of the above.

#' @param nameFileIn Character value. Input file name serving as a basis for the output file name. 
#' @param prfx Character value. String to add to the front of nameFileIn. Defaults to NULL.
#' @param sufx Character value. String to append to the end of nameFileIn. If there are any 
#' periods (.) in the input file name, Sufx will be added before the final period. 
#' Defaults to NULL.
#' @param ext Character value. String indicating the extension of the output file name. If 
#' there are any periods (.) in the input file name, Ext will replace the characters after the 
#' final period. If there are no periods in the file name. A period followed by Ext will be added 
#' to the end of the file name. Defaults to NULL.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A character vector of input directories (datums)  

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' NEONprocIS.base::def.file.name.out(nameFileIn='myFileName.json',prfx='Prefix_',sufx='_Suffix',ext='txt')


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-14)
#     original creation
##############################################################################################
def.file.name.out <- function(nameFileIn,
                              prfx=NULL,
                              sufx=NULL,
                              ext=NULL,
                              log=NULL){
  
  # Initialize log if not input
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Split the file name by periods 
  nameFileSplt <- base::strsplit(nameFileIn,'[.]')[[1]] # Try to grab the file name without extension
  
  # Add suffix & extension
  if(base::length(nameFileSplt) > 1){
    # Replace the extension
    if(!base::is.null(ext)){
      nameFileSplt[base::length(nameFileSplt)] <- ext
    }
    # Add suffix
    nameFileOut <- base::paste0(base::paste0(nameFileSplt[-base::length(nameFileSplt)],collapse='.'),sufx,'.',utils::tail(nameFileSplt,1))
  } else {
    if(!base::is.null(ext)){
      ext <- base::paste0('.',ext)
    }
    nameFileOut <- base::paste0(nameFileSplt,sufx,ext)
  }
  
  # Add prefix
  nameFileOut <- base::paste0(prfx,nameFileOut)
  
  return(nameFileOut)
  
}
