##############################################################################################
#' @title Workflow for correcting fDOM for temperature and absorbance

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Workflow. Apply temperature and absorbance corrections to fDOM data for the
#' water quality transition.
#'
#' Inputs:
#' (1) fdomDataRegularized
#' (2) prtDataL1_5min
#' (3) sunaDataL0
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Directories for corrected fDOM data, temperature correction flag, absorbance
#' correction flag, temperature correction uncertatinty factors, absorbance correction
#' uncertainty factors.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' #TBD

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

#Set some directory information similar to all the other scripts to access the pachyderm repo(s)

#Read in the L0, regularized fDOM data

#Apply temperature corrections (equation 7 in the ATBD)
#rho_fDOM comes from CVAL
#temp data comes from PRT


#Apply absorbance corrections (equation 6 in the ATBD)
#pathlength comes from CVAL
#Absorbance comes from SUNA cal table and SUNA L0 data

#Maybe we should have two functions and one flow that calls them rather than a big old flow
#What is the overhead associated with multiple functions versus one larger function?
#It is harder for other to read/work with larger functions?