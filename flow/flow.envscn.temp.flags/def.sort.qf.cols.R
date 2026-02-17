##############################################################################################
#' @title Sort quality flag columns in standard order

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Sort QF columns by depth, then test type, then variable type.
#' Order: readout_time, then for each depth (01-08):
#' VSIC/VSWCfactory/VSWCsoilSpecific x Null/Gap/Range/Step/Persistence/Spike, then tempTest

#' @param cols Character vector of column names to sort

#' @return Character vector of column names in sorted order

#' @references
#' License: GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @seealso Currently none

#' @ export

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-02-17)
#'     original creation
##############################################################################################
def.sort.qf.cols <- function(cols) {
  
  # Keep readout_time first
  readoutCol <- cols[cols == "readout_time"]
  
  # Get all QF columns
  qfCols <- cols[base::grepl("QF$", cols)]
  
  # Extract components for sorting
  sortDf <- data.frame(
    col = qfCols,
    stringsAsFactors = FALSE
  )
  
  # Classify variable type
  sortDf$varType <- base::ifelse(
    base::grepl("^VSIC", sortDf$col), "1_VSIC",
    base::ifelse(
      base::grepl("^VSWCfactory", sortDf$col), "2_VSWCfactory",
      base::ifelse(
        base::grepl("^VSWCsoilSpecific", sortDf$col), "3_VSWCsoilSpecific",
        "4_temp"
      )
    )
  )
  
  # Extract depth number
  sortDf$depth <- base::as.numeric(
    base::gsub(".*Depth(\\d+).*|.*depth(\\d+).*", "\\1\\2", sortDf$col)
  )
  
  # Classify QF test type
  sortDf$qfType <- base::ifelse(
    base::grepl("NullQF$", sortDf$col), "1_Null",
    base::ifelse(
      base::grepl("GapQF$", sortDf$col), "2_Gap",
      base::ifelse(
        base::grepl("RangeQF$", sortDf$col), "3_Range",
        base::ifelse(
          base::grepl("StepQF$", sortDf$col), "4_Step",
          base::ifelse(
            base::grepl("PersistenceQF$", sortDf$col), "5_Persistence",
            base::ifelse(
              base::grepl("SpikeQF$", sortDf$col), "6_Spike",
              "7_tempTest"
            )
          )
        )
      )
    )
  )
  
  # Sort by depth, then test type, then variable type
  sortDf <- sortDf[base::order(sortDf$depth, sortDf$qfType, sortDf$varType), ]
  
  # Combine readout_time first, then sorted QF columns
  return(base::c(readoutCol, sortDf$col))
}
