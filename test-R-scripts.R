# Run unit tests.

# Load libraries
library(rlang)
library(testthat)
library(devtools)
library(usethis)
library(covr)
library(dplyr)
library(arrow)
library(here)

# Get the full path to the project root
project_root <- here()
print(project_root)

# base test
base_dir <- paste0(project_root, "/pack/NEONprocIS.base/tests/testthat")
devtools::test(pkg=base_dir)

# qaqc test
qaqc_dir <- paste0(project_root, "/pack/NEONprocIS.qaqc/tests/testthat")
devtools::test(pkg=qaqc_dir)

# report
cov <- covr::package_coverage()
report(cov)
