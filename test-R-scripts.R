# R script file
source('neon-package-loader.R')
load_test_dependencies()

library(rlang)
library(stringr)
library(devtools)
library(testthat)
library(usethis)
library(covr)
library(dplyr)
library(arrow)
library(here)

# base test

# Get the full path to the project root
project_root <- here()
print(project_root)

# qaqc test
project_root <- here()
test_dir <- paste0(project_root, "/pack/NEONprocIS.qaqc/tests/testthat")
devtools::test(pkg=test_dir)

# report
cov <- covr::package_coverage()
report(cov)

############

