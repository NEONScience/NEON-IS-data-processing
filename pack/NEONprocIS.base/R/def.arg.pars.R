##############################################################################################
#' @title Parse input argument strings into list of named parameters

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Parse the input arguments as read from the command line into a named
#' parameter list. Each input argument must be a character string in the format "Para=value", 
#' where "Para" is the intended parameter name and "value" is the value of the parameter. Options 
#' include: checking for expected parameter names, parsing of each parameter value into a vector, 
#' reading the parameter value(s) from environment variables, conversion of each paramater from the 
#' default class of character to any other recognized class, and logging via the lgr package. 

#' @param arg Character vector of argument strings, each in the format "Para=value", where "Para" 
#' is the intended parameter name and "value" is the value of the parameter. If the value string contains
#' pipes (|), the value string will be split into a character vector with | and/or : used as the delimiter. 
#' If a value string begins with a $ (e.g. "$DIR_IN"), the value of the parameter will be assigned from 
#' the system environment variable matching the value string.  Note that the value string is first 
#' split into a character vector based on the delimiters, then each relevant string of the resultant
#' character vector is evaluated as an environment variable.
#' @param NameParaReqd character vector of required parameter names as a check on the output parameter 
#' list. Defaults to NULL, in which case there are no required parameters.  
#' @param NameParaOptn character vector of optional parameter names as a check on the output parameter 
#' list. Defaults to NULL, in which case there are no optional parameters. If both NameParaReqd and
#' NameParaOptn are NULL, no check is performed on the output parameter list. 
#' @param ValuParaOptn Named list of default values (in desired class) for the optional parameter arguments 
#' in NameParaOptn. Defaults to NULL. Any optional arguments missing from this list will not be included
#' in the output parameter list if they are not provided by the user. 
#' @param TypePara Named list of R classes to convert the corresponding parameter to. For example, 
#' TypePara=list(Para1="numeric") will attempt to convert the value of Para1 to numeric. Defaults to
#' NULL, in which case no type conversion will be attempted.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A named list of parameters.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' arg <- c("DirIn=/scratch/test","DirOut=$DIR_OUT","Freq=10|20")
#' NameParaReqd <- c("DirIn","DirOut")
#' NameParaOptn <- "Freq"
#' TypePara<-list(DirIn="character",DirOut="character",Freq="numeric")
#' Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=NameParaReqd,NameParaOptn=NameParaOptn,TypePara=TypePara)
#' # Result
#' # > Para
#' # $DirIn
#' # [1] "/scratch/test" 
#' 
#' # $DirOut
#' [1] "/scratch/test/out" # Read of environment variable $DIR_OUT
#' 
#' # $Freq
#' # [1] 10 20


#' @seealso \code{\link[NEONprocIS.base]{def.vect.pars.pair}}
#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-09-24)
#     original creation
#   Cove Sturtevant (2019-10-01)
#     added additional parsing of argument string by colon (along with pipe)
#   Cove Sturtevant (2019-10-16)
#     added input of default parameter values
#   Cove Sturtevant (2020-02-11)
#     limit splitting of parameter string to a single colon or pipe (i.e. do not split double colon or double pipe)
##############################################################################################
def.arg.pars <- function(arg,
                     NameParaReqd=NULL,
                     NameParaOptn=NULL,
                     ValuParaOptn=NULL,
                     TypePara=NULL, # Named list of intended class for each parameter. Conversion will be attempted
                     log=NULL){
  browser()
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Error check
  if(base::class(arg) != "character"){
    msg <- "Input argument vector must be of class character."
    print(msg)
    log$fatal(msg)
    stop()
  }
  
  # Error check
  nameValuOptn <- base::names(ValuParaOptn)
  chkOptn <- !(nameValuOptn %in% NameParaOptn)
  if(base::length(chkOptn) > 0 && base::sum(chkOptn) != 0){
    msg <- base::paste0('Default parameter value(s) were provided for: ',base::paste0(nameValuOptn[chkOptn],collapse=','),', but these parameters were not listed in NameParaOptn. Check inputs.')
    log$fatal(msg)
    stop()        
  }
  
  # Intialize list of parameters
  Para <- base::list()
  
  for(idxArg in arg){
    
    # Parse before/after equals sign
    splt <- base::strsplit(idxArg,"=")[[1]]
    
    # Trim whitespace
    splt <- base::trimws(splt)
    
    # Error check
    if(base::length(splt) != 2){
      msg <- base::paste0('There must be one equals sign (=) in each argument. Parameter ',idxArg, ' does not fulfill this requirement.')
      log$fatal(msg)
      stop()        
    }
    
    # Split the parameter value into a character vector delimited by pipes or colons, but not splitting if more than one pipe or colon
    valu <- base::strsplit(splt[2],"(?<![:|])[:|]{1}(?![:|])",perl=TRUE)[[1]]
    
    # Evaluate environment variables
    for(idxValu in base::seq_len(base::length(valu))){
      if(base::substr(valu[idxValu],start=1,stop=1) == "$"){
        # Read the parameter value from system environment variable
        varEnv <- base::substr(valu[idxValu],start=2,stop=base::nchar(valu[idxValu]))
        valu[idxValu] <- base::Sys.getenv(x=varEnv)
        
        # Error check
        if(base::nchar(valu[idxValu]) == 0){
          msg <- base::paste0('Input parameter ',splt[1], ' was indicated to be read from system environment variable ',varEnv, ' but it cannot be found.')
          log$fatal(msg)
          stop()        
        }
      }      
    }
    
    # Assign the parameter
    Para[[splt[1]]] <- valu
    
  }
  
  # Check if the resultant parameter list meets our expectations
  if(!base::is.null(NameParaReqd)){
    inPara <- NameParaReqd %in% base::names(Para)
    if(base::sum(!inPara) > 0){
      msg <- base::paste0('Missing required input parameter(s) ', base::paste0(NameParaReqd[!inPara],collapse=','),
                          '. Check inputs.')
      log$fatal(msg)
      stop()
      
    }
  }
  if(!base::is.null(NameParaReqd) || !base::is.null(NameParaOptn)){
    outPara <- !(base::names(Para) %in% c(NameParaReqd,NameParaOptn))
    if(base::sum(outPara) > 0){
      msg <- base::paste0('Input parameter(s) ', base::paste0(base::names(Para)[outPara],collapse=','),
                          ' are not in the list of acceptable parameters (NameParaReqd & NameParaOptn). Check inputs.')
      log$fatal(msg)
      stop()
      
    }
  }
  
  # Resultant parameter list so far
  nameParaOut <- base::names(Para)
  
  # Assign defaults to optional parameters
  for(idxNamePara in base::setdiff(nameValuOptn,nameParaOut)){
    Para[[idxNamePara]] <- ValuParaOptn[[idxNamePara]]
  }

  # Convert the inputs to the intended types
  if(!base::is.null(TypePara)){
    if(base::class(TypePara) != "list"){
      msg <- base::paste0('TypePara must be a named list of intended classes (as a character string) for each parameter.')
      log$fatal(msg)
      stop()
      
    }
    inPara <- base::names(TypePara) %in% base::names(Para)
    if(base::sum(!inPara) > 0){
      msg <- base::paste0('List name(s): ',base::paste0(base::names(TypePara)[!inPara],collapse=','),
                          ' of TypePara do not match the names of any parameters. Check inputs.')
      log$fatal(msg)
      stop()
      
    }
    
    # Convert to appropriate class
    for(idxNamePara in base::names(TypePara)){
      base::suppressWarnings(base::class(Para[[idxNamePara]]) <- TypePara[[idxNamePara]])
    }
  }
  
  return(Para)
}
