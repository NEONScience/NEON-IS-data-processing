##############################################################################################
#' @title Instantaeous quality metrics module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Aggregate quality flags to produce alpha, beta, and final quality flags
#' for each L0' (instantaneous) record. The alpha flag is 1 when any of a set of selected
#' flags have a value of 1 (fail). The beta flag is 1 when any of a set of selected
#' flags cannot be evaluated (have a value of -1). If either the alpha flag or beta flag are raised,
#' the final quality is raised (value of 1). Multiple sets of alpha, beta, and final quality flag
#' may be created, as indicated in the input arguments.
#'
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in and combine all the flags files in the flags directory of each input datum
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
#' 3. "FileSchmQm=value" (optional), where value is the full path to the avro schema for the output final
#' quality flag file. If this input is not provided, the output schema for the stats will be auto-generated from
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE FLAGS
#' IN THE SORTED FILE LIST. See output information below for details.
#'
#' 4. "VarTimeBgn=value" (optional), where value is the name of the variable in the input files that signifies the
#' start time of the instantaneous measurement. Defaults to 'readout_time'. This variable be reported in the output as 
#' startDateTime and will not be treated as a flag (i.e. no quality metrics will be created for it). If it is not found
#' in any of the input files, startDateTime will be populated with the values in readout_time.
#' 
#' 5. "VarTimeEnd=value" (optional), where value is the name of the variable in the input files that signifies the
#' end time of the instantaneous measurement. Defaults to 'readout_time'. This variable be reported in the output as 
#' endDateTime and will not be treated as a flag (i.e. no quality metrics will be created for it). If it is not found
#' in any of the input files, endDateTime will be populated with the values in readout_time.
#' 
#' 6. "GrpQfAlphX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of
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
#' 7. "GrpQfBetaX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of
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
#' 8. "QfForcX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of
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
#' 10. "Tmi=value" (optional), where value is a 3-character index specifying the NEON timing index to include
#' in the output file name. If not input, "000" is used.
#'
#' 11. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
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
#' and the grouping arguments are "GrpQfAlpha1=temp:tempRangeQf|tempValidCalQF", "GrpQfAlpha2=RH:RHRangeQf|RHValidCalQF",
#' "GrpQfBeta1=temp:tempRangeQf", "GrpQfBeta2=RH:tempRangeQf".
#' The ordering of the output columns will be startDateTime, endDateTime, tempValidCalQF, RHValidCalQF, tempValidCalQF,
#' RHValidCalQF, tempAlphaQF, tempBetaQF, tempFinalQF, RHAlphaQF, RHBetaQF, RHFinalQF. The names (but not the order) of the 
#' output columns may be changed by providing an output schema in argument FileSchmQm. However, ENSURE THAT ANY PROVIDED 
#' OUTPUT SCHEMA MATCHES THIS COLUMN ORDERING. Otherwise, column names will not pertain to the flag in the column.
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
##############################################################################################
library(foreach)
library(doParallel)

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

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", "DirOut"),
    NameParaOptn = c(
      "FileSchmQm",
      "VarTimeBgn",
      "VarTimeEnd",
      base::paste0("GrpQfAlph", 1:100),
      base::paste0("GrpQfBeta", 1:100),
      base::paste0("QfForc", 1:100),
      base::paste0("GrpQfBetaIgnr", 1:100),
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
  ParaGrp <- base::lapply(
    spltGrp,
    FUN = function(argSplt) {
      base::list(
        name = argSplt[1],
        qfAlph = utils::tail(x = argSplt, n = -1),
        nameQfAlph = base::paste0(argSplt[1], "AlphaQF"),
        nameQfBeta = base::paste0(argSplt[1], "BetaQF"),
        nameQfFinl = base::paste0(argSplt[1], "FinalQF")
      )
    }
  )
  for (idxGrp in 1:base::length(ParaGrp)) {
    log$debug(
      base::paste0(
        ParaGrp[[idxGrp]]$name,
        'AlphaQF will be computed from the following flags: ',
        base::paste0(ParaGrp[[idxGrp]]$qfAlph, collapse =
                       ',')
      )
    )
  }
  
} else {
  # All flags contribute to alphaQF
  ParaGrp <- base::list(
    base::list(
      name = "",
      qfAlph = NULL,
      nameQfAlph = "AlphaQF",
      nameQfBeta = "BetaQF",
      nameQfFinl = 'FinalQF'
    )
  )
  log$debug('AlphaQF will be computed with all flags found in the input file(s).')
  
}
names(ParaGrp) <- NULL

# Construct the names of the alpha, beta, and final QF columns
nameQfGrp <-
  base::unlist(base::lapply(
    ParaGrp,
    FUN = function(idx) {
      c(idx$nameQfAlph, idx$nameQfBeta, idx$nameQfFinl)
    }
  ))
nameGrp <-
  base::unlist(base::lapply(
    ParaGrp,
    FUN = function(idx) {
      idx$name
    }
  )) 

# Check that the group names are unique
if (base::length(base::unique(nameGrp)) < base::length(nameGrp)) {
  log$fatal(
    'Group names for GrpQfAlphaX arguments are not unique. This is not allowed. Check inputs.'
  )
  stop()
}

# Parse the groups of flags feeding into beta quality flags
nameParaGrpQfBeta <-
  base::names(Para)[names(Para) %in% base::paste0("GrpQfBeta", 1:100)]
if (base::length(nameParaGrpQfBeta) > 0) {
  spltGrp <- Para[nameParaGrpQfBeta]
  
  for (idxSpltGrp in 1:base::length(spltGrp)) {
    # Pull the group name and flags feeding into this betaQF
    nameIdx <- spltGrp[[idxSpltGrp]][1]
    qfIdx <- utils::tail(x = spltGrp[[idxSpltGrp]], n = -1)
    
    idxGrp <- base::which(nameGrp == nameIdx)
    if (base::length(idxGrp) == 0) {
      log$fatal(
        'Group names are not consistent across input arguments GrpQfAlphX and GrpQfBetaX. Check inputs.'
      )
      stop()
    }
    
    ParaGrp[[idxGrp]]$qfBeta <- qfIdx
  }
  
  # Check that all groups have a list of beta flags, and display
  for (idxGrp in 1:base::length(ParaGrp)) {
    if (base::is.null(ParaGrp[[idxGrp]]$qfBeta)) {
      if (ParaGrp[[idxGrp]]$name != "") {
        log$fatal(
          'Group names are not consistent across input arguments GrpQfAlphX and GrpQfBetaX. Check inputs.'
        )
        stop()
      } else {
        log$debug('BetaQF will be computed with all flags found in the input file(s).')
      }
    } else {
      log$debug(
        base::paste0(
          ParaGrp[[idxGrp]]$name,
          'BetaQF will be computed from the following flags: ',
          base::paste0(ParaGrp[[idxGrp]]$qfBeta, collapse =
                         ',')
        )
      )
    }
    
  }
  
} else {
  # All flags contribute to betaQF
  idxGrp <- base::which(nameGrp == "")
  ParaGrp[[idxGrp]]$qfBeta <- NULL
  log$debug('BetaQF will be computed with all flags found in the input file(s).')
  
}



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
    
    idxGrp <- base::which(nameGrp == nameIdx)
    if (base::length(idxGrp) == 0) {
      log$fatal(
        'Group names are not consistent across input arguments GrpQfAlphX and GrpQfBetaIgnr. Check inputs.'
      )
      stop()
    }
    
    ParaGrp[[idxGrp]]$qfBetaIgnr <- qfIdx
    
    log$debug(
      base::paste0(
        ParaGrp[[idxGrp]]$name,
        'BetaQF will be forced to 0 if any of the following flags have a value of 1: ',
        base::paste0(ParaGrp[[idxGrp]]$qfBetaIgnr, collapse =
                       ',')
      )
    )
  }
}


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
  
  # Get directory listing of input directory. Expect subdirectories for flags
  idxDirQf <- base::paste0(idxDirIn, '/flags')
  fileQf <- base::sort(base::dir(idxDirQf))
  log$info(
    base::paste0(
      'Flags from ',
      base::length(fileQf),
      ' files will be combined for computation of alpha, beta, and final quality flags.'
    )
  )
  
  # Gather info about the input directory (including date) and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  idxDirOut <- base::paste0(Para$DirOut, InfoDirIn$dirRepo)
  idxDirOutQf <- base::paste0(idxDirOut, '/quality_metrics')
  NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                DirSub = 'quality_metrics',
                                log = log)
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn, '/', DirSubCopy), idxDirOut, log =
                                         log)
  }
  
  # Combine flags files
  qf <- NULL
  qf <-
    NEONprocIS.base::def.file.comb.ts(
      file = base::paste0(idxDirQf, '/', fileQf),
      nameVarTime = 'readout_time',
      log = log
    )
  
  # Take stock of our quality flags
  numNa <- base::apply(X = base::is.na(qf),
                       MARGIN = 2,
                       FUN = base::sum)
  if (base::sum(numNa) > 0) {
    log$warn(
      base::paste0(
        numNa[numNa > 0],
        " NA values found for variable ",
        base::names(numNa[numNa > 0]),
        " (could be due to regularization if no warning was issued about timestamp consistency). These will be replaced with -1."
      )
    )
    qf[base::is.na(qf)] <- -1
  }
  
  
  # Figure out the names of the output columns
  nameVarIn <- base::names(qf)
  nameQfIn <- base::setdiff(nameVarIn, c("readout_time",Para$VarTimeBgn,Para$VarTimeEnd))
  nameQfOut <- c(nameQfIn, nameQfGrp)
  log$debug(
    base::paste0(
      'The default output column names are as follow (ensure any output schema that changes these names matches this order):',
      base::paste0(c(
        'startDateTime', 'endDateTime', nameQfOut
      ), collapse = ',')
    )
  )
  
  # Are the variables indicated in VarTimeBgn and VarTimeEnd present?
  VarTimeBgnIdx <- Para$VarTimeBgn
  if(!(VarTimeBgnIdx %in% nameVarIn)){
    VarTimeBgnIdx <- 'readout_time'
  }
  VarTimeEndIdx <- Para$VarTimeEnd
  if(!(VarTimeEndIdx %in% nameVarIn)){
    VarTimeEndIdx <- 'readout_time'
  }
  
  # Initialize the output
  qf <-
    qf[c(VarTimeBgnIdx,
         VarTimeEndIdx,
         nameQfIn,
         base::rep(nameQfIn[1], base::length(nameQfGrp)))]
  base::names(qf) <- c('startDateTime', 'endDateTime', nameQfOut)
  qf[nameQfGrp] <-
    0 # intialize alpha, beta, and final QF to 0 (good)
  
  # Force any flags that were indicated in the input arguments
  if (!base::is.null(ParaForc)) {
    for (idxForc in 1:length(ParaForc)) {
      qf <-
        NEONprocIS.qaqc::def.qf.forc(
          qf = qf,
          nameQfDrve = ParaForc[[idxForc]]$qfDrve,
          valuDrve = ParaForc[[idxForc]]$valuDrve,
          nameQfForc = ParaForc[[idxForc]]$qfForc,
          valuForc = ParaForc[[idxForc]]$valuForc,
          log = log
        )
    }
  }
  
  # Go through each group, creating the alpha, beta, and final quality flags
  for (idxGrp in 1:length(ParaGrp)) {
    qmIdx <- NULL
    
    log$debug(
      base::paste0(
        'Computing ',
        ParaGrp[[idxGrp]]$nameQfAlph,
        ',',
        ParaGrp[[idxGrp]]$nameQfBeta,
        ', and ',
        ParaGrp[[idxGrp]]$nameQfFinl,
        '...'
      )
    )
    
    # Compute the flags
    qmIdx <-
      NEONprocIS.qaqc::def.qm.dp0p(qf = qf[nameQfIn],
                                   Para = ParaGrp[[idxGrp]],
                                   log = log)
    
    # Put in the output
    qf[c(ParaGrp[[idxGrp]]$nameQfAlph,
         ParaGrp[[idxGrp]]$nameQfBeta,
         ParaGrp[[idxGrp]]$nameQfFinl)] <-
      qmIdx[c('AlphaQF', 'BetaQF', 'FinalQF')]
    
  } # End loops around alpha/beta/final QF groups
  
  # Write out the file. Replace the final underscore-delimited component of one of the input
  # filenames, assuming this final component denotes the type of flags that are in the file
  # Note that we still use 'quality metrics' here, even though the output consists of quality flags,
  # since this output is analagous to the quality metrics from the data product/publication standpoint.
  qf[nameQfOut] <- lapply(qf[nameQfOut],base::as.integer) # Turn flags to integer
  fileQmOutSplt <-
    base::strsplit(fileQf[1], '[_]')[[1]] # Separate underscore-delimited components of the file name
  fileQmOutSplt[base::length(fileQmOutSplt)] <-
    base::paste(
      base::paste('qualityMetrics', Para$Tmi, sep = '_'),
      utils::tail(
        x = base::strsplit(utils::tail(x = fileQmOutSplt, n = 1), '[.]')[[1]],
        n = -1
      ),
      sep = '.'
    ) # Replace last component, but try to keep the extension
  fileQmOut <- base::paste(fileQmOutSplt, collapse = '_')
  NameFileOutQm <- base::paste0(idxDirOutQf, '/', fileQmOut)
  
  rptWrte <-
    base::try(NEONprocIS.base::def.wrte.parq(
      data = qf,
      NameFile = NameFileOutQm,
      NameFileSchm = NULL,
      Schm = SchmQm
    ),
    silent = TRUE)
  if (base::class(rptWrte) == 'try-error') {
    log$error(base::paste0(
      'Cannot write quality metrics file ',
      NameFileOutQm,
      '. ',
      attr(rptWrte, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0(
      'Quality metrics written successfully in file: ',
      NameFileOutQm
    ))
  }
  
  return()
} # End loop around datum paths
