##############################################################################################
#' @title Instantaeous quality metrics module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Aggregate quality flags to produce alpha, beta, and final quality flags
#' for each L0' (instantaneous) record. The alpha flag is 1 when any of a set of selected
#' flags have a value of 1 (fail). The beta flag is 1 when any of a set of selected
#' flags cannot be evaluated (have a value of -1). If either the alpha flag or beta flag are raised,
#' the final quality is raised (value of 1). There are several options to tailor the 
#' computation of the summary flags, including:
#'    1) specifying which flags feed into each the alpha and beta quality flags, noting that multiple 
#'    sets of alpha, beta, and final quality flag may be created, 
#'    2) forcing flags to specific values  based on the value of another flag prior to computation 
#'    of summary flags (and also provided in the output), and 
#'    3) forcing the beta flag to 0 if particular flags are raised (i.e. NULL or Gap flags)
#'
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in and combine all the flags files in the flags directory of each input datum
#'      Change any NA flag values to -1
#'      Force any flags to particular values based on values of other flags (as specified in input arguments)
#'      Create alpha, beta, and final quality flags for each group as specified in the input arguments
#'      Write out the entire flag set, including all input flags and the alpha, beta, and final QFs
#'
#' This script is run at the command line with the following arguments. Each argument must be a string
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#'
#' Nested within this path are (at a minimum) the folder:
#'         /flags
#'
#' The flags folder holds any number of daily files holding quality flags. All files will be combined. Ensure
#' there are no overlapping column names between the files other than "readout_time" and the names of any other 
#' time variables indicated in arguments VarTimeBgn and VarTimeEnd (see below). Note that the
#' "readout_time" variable must exist in all files, as it is used to match up the measurements. 
#' Any non-matching timestamps among files will result in NA values for columns that do not have this timestamp.
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "FileSchmQm=value" (optional), where value is the full path to the avro schema for the output final
#' quality flag file. If this input is not provided, the output schema for the stats will be auto-generated from
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE FLAGS
#' IN THE SORTED FILE LIST. See output information below for details.
#'
#' 5. "VarTimeBgn=value" (optional), where value is the name of the variable in the input files that signifies the
#' start time of the instantaneous measurement. Defaults to 'readout_time'. This variable be reported in the output as 
#' startDateTime and will not be treated as a flag (i.e. no quality metrics will be created for it). If it is not found
#' in any of the input files, startDateTime will be populated with the values in readout_time.
#' 
#' 6. "VarTimeEnd=value" (optional), where value is the name of the variable in the input files that signifies the
#' end time of the instantaneous measurement. Defaults to 'readout_time'. This variable be reported in the output as 
#' endDateTime and will not be treated as a flag (i.e. no quality metrics will be created for it). If it is not found
#' in any of the input files, endDateTime will be populated with the values in readout_time.
#' 
#' 7. "GrpQfAlphX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of
#' the quality flags that should be used in the computation of the alpha quality flag. Begin each argument with
#' the group name (e.g. temp) to be used as a prefix to the output alpha flag, followed by a colon (:), and then
#' the exact names of the quality flags, delimited by pipes (|). For example, if tempRangeQF and tempPersistenceQF
#' feed into the alpha flag (a value of 1 for either of these flags causes the alpha flag to be 1), and you
#' want "temp" to be a prefix for resultant alpha QF, the argument is "GrpQfAlph1=temp:tempRangeQF|tempPersistenceQF".
#' If no prefix to the output name is desired, include the colon as the first character of value
#' (e.g. "GrpQfAlph1=:tempRangeQF|tempPersistenceQF"). If this argument is not included, all
#' quality flags in the file(s) will be used to compute a single alpha flag. There may be
#' multiple assignments of GrpQfAlphX, specified by incrementing the number X by 1 with each additional argument.
#' There is a limit of X=100 for GrpQfAlphX arguments. Note that the group names must be unique among GrpQfAlphX
#' arguments, and must match those found in GrpQfBetaX arguments. The group name will also be applied to the
#' resultant final QF.
#'
#' 8. "GrpQfBetaX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of
#' the quality flags that should be used in the computation of the beta quality flag. Begin each argument with
#' the group name (e.g. temp) to be used as a prefix to the output beta flag, followed by a colon (:), and then
#' the exact names of the quality flags, delimited by pipes (|). For example, if tempRangeQF and tempPersistenceQF
#' feed into the beta flag (a value of -1 for either of these flags causes the beta flag to be 1), and you
#' want "temp" to be a prefix for resultant beta QF, the argument is "GrpQfBeta1=temp:tempRangeQF|tempPersistenceQF".
#' If no prefix to the output name is desired (i.e. to output "betaQF"), include the colon as the first character of
#' value (e.g. "GrpQfBeta1=:tempRangeQF|tempPersistenceQF"). If this argument is not included, all
#' quality flags in the file(s) will be used to compute a single beta flag. There may be
#' multiple assignments of GrpQfBetaX, specified by incrementing the number X by 1 with each additional argument.
#' There is a limit of X=100 for GrpQfBetaX arguments. Note that the group names must be unique among GrpQfBetaX
#' arguments, and must match those found in GrpQfAlphaX arguments. The group name will also be applied to the
#' resultant final QF.
#'
#' 9. "GrpQfBetaIgnrX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names
#' of the quality flags that, if any of their values equals 1, the beta QF flag for the indicated group is
#' set to 0. Begin each argument with the group name (e.g. temp) that corresponds to a group indicated in the
#' GrpQfBetaX argument(s), followed by a colon (:), and then the exact names of the quality flags, delimited by
#' pipes (|), that cause the beta QF for that group to be set to 0 if any of their values equals 1. For example,
#' if the tempNullQF should cause the tempBetaQF to be set to 0 when tempNullQF = 1, the argument is
#' "GrpQfBeta1=temp:tempNullQF". To apply this logic to a group without a prefix (i.e. to betaQF), include the
#' colon as the first character of value (e.g. "GrpQfBetaIgnr1=:nullQF"). Note that the group names must be unique
#' among GrpQfBetaIgnrX arguments, and must be a subset of those found in GrpQfAlphaX arguments.
#'
#' 10. "QfForcX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of
#' the quality flags that should be forced to a particular value if the value of a "driver" flag equals a
#' particular value. Begin each argument with the name of the driver flag (e.g. tempNullQF), followed by a colon (:),
#' then the numeric "driver value" of the driver flag which activates the force, followed by a colon (:),
#' then the exact names of the "forced" flags, delimited by pipes (|), followed by a colon (:),
#' and finally the "forced" value. For example, if tempRangeQF and tempPersistenceQF should be set to -1 when
#' sensorNAQF is 1, then the argument is "QfForc1=sensorNAQf:1:tempRangeQF|tempPersistenceQF:-1". There may
#' be multiple assignments of QfForcX, specified by incrementing the number X by 1 with each additional argument.
#' There is a limit of X=100 for QfForcX arguments.
#' Note that the logic described here occurs before the alpha, beta, and final quality flags are calculated, and
#' forcing actions will occur following increasing value of X in QfForcX.
#'
#' 12. "VarIgnr=value" (optional), where value contains the names of the variables that should be ignored if 
#' found in the input files, separated by pipes (|) (e.g. "VarIgnr=test1QF|test2QF"). Do not include 
#' readout_time here. No quality metrics will be computed for these variables and they will not be included 
#' in the output. Defaults to empty. 
#' 
#' 11. "Tmi=value" (optional), where value is a 3-character index specifying the NEON timing index to include
#' in the output file name. If not input, "000" is used.
#'
#' 12. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the flags folder in the input path that are to be copied with a
#' symbolic link to the output path.
#'
#' Note that all quality flags found in the input files will be output, the GrpQfX arguments
#' simply dictate what QMs feed into a set of alpha/beta/final quality flags.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return Resultant quality flags, including the alpha, beta, and final quality flags output in DirOut, where the
#' terminal directory of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input
#' path. Directory 'quality_metrics' will automatically populated in the output directory with the output file. Any
#' other folders specified in argument DirSubCopy will be copied over unmodified with a symbolic link.
#'
#' If no output schema is provided for the quality flags, the variable names for the alpha, beta, and final QFs will
#' be a combination of the group name and AlphaQF, BetaQF, or FinalQF. The order of the outputs will be all quality flags in
#' the same order they were found in the sorted (increasing order) input file list, followed by the alpha QF, betaQF, and
#' finalQF for each group in the same order as the GrpQfAlphaX inputs. Additionally, the first two columns of the output file
#' will contain the start and end times for the measurement, labeled "startDateTime" and "endDateTime",
#' respectively, and populated with the variables indicated in input arguments VarTimeBgn and varTimeEnd. 
#' Example column ordering: Say there are two input files named outflagsA.parquet and outflagsB.parquet, where
#' outflagsA.parquet contains flag tempValidCalQF, RHValidCalQF and outflagsB.parquet contains flags tempRangeQF, RHRangeQF
#' and the names of the groups in ParaGrp are ordered "temp" and "RH". 
#' The ordering of the output columns will be startDateTime, endDateTime, tempValidCalQF, RHValidCalQF, tempValidCalQF,
#' RHValidCalQF, tempAlphaQF, tempBetaQF, tempFinalQF, RHAlphaQF, RHBetaQF, RHFinalQF. The names (but not the order) of the 
#' output columns may be changed by providing an output schema in argument FileSchmQm. However, ENSURE THAT ANY PROVIDED 
#' OUTPUT SCHEMA MATCHES THIS COLUMN ORDERING. Otherwise, column names will not pertain to the flag in the column. Best practice 
#' is to first run the code without the schema argument so that the default column naming is output. Then craft the schema
#' based on the known column ordering. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON Algorithm Theoretical Basis Document: Quality Flags and Quality Metrics for TIS Data Products (NEON.DOC.001113) \cr
#' Smith, D.E., Metzger, S., and Taylor, J.R.: A transparent and transferable framework for tracking quality information in
#' large datasets. PLoS ONE, 9(11), e112249.doi:10.1371/journal.pone.0112249, 2014. \cr

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.qaqc.qf.finl.dp0p.R "DirIn=/scratch/pfs/waterQuality_exoconductivity_qaqc_flags_group/2019/01/02" "DirOut=/scratch/pfs/out" "FileSchmQm=/outputQfSchema.avsc" "GrpQfAlph1=temp:rangeQF|stepQF" "GrpQfBeta1=temp:rangeQF|stepQF" "QfForc1=nullQF:1:rangeQF|stepQF:-1" "GrpQfBetaIgnr1=temp:nullQF"

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-09)
#     original creation
#   Cove Sturtevant (2020-04-28)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2021-02-04)
#     added option to specify the columns that are reported as the startDateTime and endDateTime in the output
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
#   Cove Sturtevant (2021-11-08)
#     Move main functionality to wrapper function
#     Add error routing
#   Cove Sturtevant (2023-01-13)
#     Add option to ignore variables in the input files
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.qaqc.qm.dp0p.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Use environment variable to specify how many cores to run on
numCoreUse <- base::as.numeric(Sys.getenv('PARALLELIZATION_INTERNAL'))
numCoreAvail <- parallel::detectCores()
if (base::is.na(numCoreUse)){
  numCoreUse <- 1
} 
if(numCoreUse > numCoreAvail){
  numCoreUse <- numCoreAvail
}
log$debug(paste0(numCoreUse, ' of ',numCoreAvail, ' available cores will be used for internal parallelization.'))

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", 
                     "DirOut",
                     "DirErr" 
    ),
    NameParaOptn = c(
      "FileSchmQm",
      "VarTimeBgn",
      "VarTimeEnd",
      base::paste0("GrpQfAlph", 1:100),
      base::paste0("GrpQfBeta", 1:100),
      base::paste0("QfForc", 1:100),
      base::paste0("GrpQfBetaIgnr", 1:100),
      "VarIgnr",
      "Tmi",
      "DirSubCopy"
    ),
    ValuParaOptn=list(
      Tmi="000",
      VarTimeBgn="readout_time",
      VarTimeEnd="readout_time"
      ),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))

# Read in the output schema
log$debug(base::paste0(
  'Output schema for quality flags: ',
  base::paste0(Para$FileSchmQm, collapse = ',')
))
if (base::is.null(Para$FileSchmQm) || Para$FileSchmQm == 'NA') {
  SchmQm <- NULL
} else {
  SchmQm <- base::paste0(base::readLines(Para$FileSchmQm), collapse = '')
}

# Echo arguments
log$debug(base::paste0('Variable to be used for startDateTime: ', Para$VarTimeBgn))
log$debug(base::paste0('Variable to be used for endDateTime: ', Para$VarTimeBgn))

# Parse the groups of flags feeding into alpha quality flags 
nameParaGrpQfAlph <-
  base::names(Para)[names(Para) %in% base::paste0("GrpQfAlph", 1:100)]
if (base::length(nameParaGrpQfAlph) > 0) {
  
  spltGrp <- Para[nameParaGrpQfAlph]
  ParaGrpAlph <- base::lapply(
    spltGrp,
    FUN = function(argSplt) {
      base::list(
        name = argSplt[1],
        qfAlph = utils::tail(x = argSplt, n = -1)
      )
    }
  )
  for (idxGrp in 1:base::length(ParaGrpAlph)) {
    log$debug(
      base::paste0(
        ParaGrpAlph[[idxGrp]]$name,
        'AlphaQF will be computed from the following flags: ',
        base::paste0(ParaGrpAlph[[idxGrp]]$qfAlph, collapse = ',')
      )
    )
  }
  
} else {
  # All flags contribute to alphaQF
  ParaGrpAlph <- base::list(
    base::list(
      name = "",
      qfAlph = NULL
    )
  )
  log$debug('AlphaQF will be computed with all flags found in the input file(s).')
  
}
names(ParaGrpAlph) <- NULL

nameGrpAlph <-
  base::unlist(base::lapply(
    ParaGrpAlph,
    FUN = function(idx) {
      idx$name
    }
  )) 
base::names(ParaGrpAlph) <- nameGrpAlph

# Check that the group names are unique
if (base::length(base::unique(nameGrpAlph)) < base::length(nameGrpAlph)) {
  log$fatal(
    'Group names for GrpQfAlphX arguments are not unique. This is not allowed. Check inputs.'
  )
  stop()
}

# Parse the groups of flags feeding into beta quality flags 
nameParaGrpQfBeta <-
  base::names(Para)[names(Para) %in% base::paste0("GrpQfBeta", 1:100)]
if (base::length(nameParaGrpQfBeta) > 0) {
  
  spltGrp <- Para[nameParaGrpQfBeta]
  ParaGrpBeta <- base::lapply(
    spltGrp,
    FUN = function(argSplt) {
      base::list(
        name = argSplt[1],
        qfBeta = utils::tail(x = argSplt, n = -1)
      )
    }
  )
  for (idxGrp in 1:base::length(ParaGrpBeta)) {
    log$debug(
      base::paste0(
        ParaGrpBeta[[idxGrp]]$name,
        'BetaQF will be computed from the following flags: ',
        base::paste0(ParaGrpBeta[[idxGrp]]$qfBeta, collapse = ',')
      )
    )
  }
  
} else {
  # All flags contribute to betaQF
  ParaGrpBeta <- base::list(
    base::list(
      name = "",
      qfBeta = NULL
    )
  )
  log$debug('BetaQF will be computed with all flags found in the input file(s).')
  
}
names(ParaGrpBeta) <- NULL

nameGrpBeta <-
  base::unlist(base::lapply(
    ParaGrpBeta,
    FUN = function(idx) {
      idx$name
    }
  )) 
base::names(ParaGrpBeta) <- nameGrpBeta

# Check that the group names are unique
if (base::length(base::unique(nameGrpBeta)) < base::length(nameGrpBeta)) {
  log$fatal(
    'Group names for GrpQfBetaX arguments are not unique. This is not allowed. Check inputs.'
  )
  stop()
}

# Check that group names are the same among Alpha and Beta groups
if (!base::all(nameGrpAlph %in% nameGrpBeta) || !base::all(nameGrpBeta %in% nameGrpAlph)){
  log$fatal(
    'Group names are not consistent across input arguments GrpQfAlphX and GrpQfBetaX. Check inputs.'
  )
}
nameGrp <- nameGrpAlph

# Combine the groupings 
ParaGrpAlph <- lapply(ParaGrpAlph,FUN=function(idxGrp){idxGrp <- idxGrp["qfAlph"]})
ParaGrpBeta <- lapply(ParaGrpBeta,FUN=function(idxGrp){idxGrp <- idxGrp["qfBeta"]})
ParaGrp <- base::Map(base::c,ParaGrpAlph, ParaGrpBeta)


# Parse the groups of flags that force other flags to specific values
nameParaQfForc <-
  base::sort(base::names(Para)[names(Para) %in% base::paste0("QfForc", 1:100)])
if (base::length(nameParaQfForc) > 0) {
  spltGrp <- Para[nameParaQfForc]
  ParaForc <- base::lapply(
    spltGrp,
    FUN = function(argSplt) {
      numArgSplt <- base::length(argSplt)
      if (numArgSplt < 4) {
        log$fatal(base::paste0('Malformed ', nameParaQfForc, ' argument. Check inputs.'))
        stop()
      }
      
      rpt <- base::list(
        qfDrve = argSplt[1],
        valuDrve = base::as.numeric(argSplt[2]),
        qfForc = argSplt[3:(numArgSplt - 1)],
        valuForc = base::as.numeric(argSplt[numArgSplt])
      )
      
      if (base::is.na(rpt$valuDrve) || base::is.na(rpt$valuForc)) {
        log$fatal(base::paste0('Malformed ', nameParaQfForc, ' argument. Check inputs.'))
        stop()
      } else {
        log$debug(
          base::paste0(
            'A ',
            rpt$qfDrve,
            ' value of ',
            rpt$valuDrve,
            ' will force the flags ',
            base::paste0(rpt$qfForc, collapse = ','),
            ' to a value of ',
            rpt$valuForc
          )
        )
      }
      return(rpt)
    }
  )
} else {
  ParaForc <- NULL
}

# Parse the groups of flags that force betaQF to 0
nameParaGrpQfBetaIgnr <-
  base::names(Para)[names(Para) %in% base::paste0("GrpQfBetaIgnr", 1:100)]
if (base::length(nameParaGrpQfBetaIgnr) > 0) {
  spltGrp <- Para[nameParaGrpQfBetaIgnr]
  
  for (idxSpltGrp in 1:base::length(spltGrp)) {
    # Pull the group name and flags feeding into this betaQF
    nameIdx <- spltGrp[[idxSpltGrp]][1]
    qfIdx <- utils::tail(x = spltGrp[[idxSpltGrp]], n = -1)
    
    if (!(nameIdx %in% nameGrp)) {
      log$fatal(
        'Group names for input argument GrpQfBetaIgnr are not consistent with those of GrpQfAlphX and GrpQfBetaX. Check inputs.'
      )
      stop()
    }
    idxGrp <- base::which(names(ParaGrp) == nameIdx)
    ParaGrp[[idxGrp]]$qfBetaIgnr <- qfIdx
    
    log$debug(
      base::paste0(
        nameIdx,
        'BetaQF will be forced to 0 if any of the following flags have a value of 1: ',
        base::paste0(ParaGrp[[idxGrp]]$qfBetaIgnr, collapse =
                       ',')
      )
    )
  }
}

# Retrieve variables to ignore in the flags files
VarIgnr <- setdiff(Para$VarIgnr,'readout_time')
log$debug(base::paste0('Variables to ingnore if found in input files: ',base::paste0(VarIgnr,collapse=',')))


# Retrieve optional subdirectories to copy over
DirSubCopy <-
  base::unique(base::setdiff(Para$DirSubCopy, 'quality_metrics'))
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
nameDirSub <- 'flags'
log$debug(base::paste0(
  'Minimum expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = nameDirSub,
                              log = log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.qaqc.qm.dp0p(DirIn=idxDirIn,
                        DirOutBase=Para$DirOut,
                        ParaGrp=ParaGrp,
                        ParaForc=ParaForc,
                        VarTimeBgn=Para$VarTimeBgn,
                        VarTimeEnd=Para$VarTimeEnd,
                        VarIgnr=VarIgnr,
                        Tmi=Para$Tmi,
                        SchmQm=SchmQm,
                        DirSubCopy=DirSubCopy,
                        log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
        
        # Re-route the failed datum
        NEONprocIS.base::def.err.datm(
          err=err,
          call.stack=call.stack,
          DirDatm=idxDirIn,
          DirErrBase=Para$DirErr,
          RmvDatmOut=TRUE,
          DirOutBase=Para$DirOut,
          log=log
        )
      }
    ),
    # This simply to avoid returning the error
    error=function(err) {}
  )
  
  
  return()
} # End loop around datum paths
