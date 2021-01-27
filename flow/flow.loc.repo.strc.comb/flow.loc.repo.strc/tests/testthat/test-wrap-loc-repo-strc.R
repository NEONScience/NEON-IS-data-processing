#
# unit test Flow scripts
# changelog and author contributions / copyrights
#   Mija Choi (2021-01-11)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.loc.repo.strc.R\n")

# Unit test of wrap.loc.repo.strc.R
test_that("Unit test of wrap.loc.repo.strc.R", {
  source('../../wrap.loc.repo.strc.R')
  library(stringr)
  
  wk_dir <- getwd()
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/prt/2019/01/01/3119')
  
  dirInLoc <- base::paste0(testInputDir, '/location')
  fileLoc <- base::dir(dirInLoc)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc, '/', fileLoc))
  nameLoc <- loc$name
  sourceId <- loc$source_id
  install_date <- loc$install_date
  
  testOutputDir = "pfs/out"
  installdate <- str_replace_all(install_date, "-", "/")
  testOutputDirPath <- base::paste0(testOutputDir, "/", installdate, collapse = '/')
  
  #
  # Test scenario 1::
  # if Comb = FALSE, default when Comb is not passed in to wrap.loc.repo.strc,
  # then pfs/prt/2019/01/01/3119 copied to pfs/out/2019/01/01/CFGLOC100241/3119/location/
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir)
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath, "/", nameLoc, "/", sourceId, "/location")
  expect_true (dir.exists(testOutputDirnamedLoc))
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  # Test scenario 2::
  # if Comb = TRUE then 3119 is replaced with the NAME, CFGLOC100241.
  # pfs/prt/2019/01/01/3119/location/ copied to pfs/out/2019/01/01/CFGLOC100241/location/
  #                   =====                                       ==============
  
  wrap.loc.repo.strc(DirIn = testInputDir,DirOutBase = testOutputDir,Comb = TRUE)
  
  testOutputDirSourceIdLoc <- base::paste0(testOutputDirPath, "/", nameLoc, "/location")
  expect_true (dir.exists(testOutputDirSourceIdLoc))
  # clean out the test output dirs and file recursively
  #
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  # Test scenario 3::
  # If there is no location file, skip
  #  testInputDir = "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt_noFiles/2019/01/01/3119"
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/prt_noFiles/2019/01/01/3119')
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  
  expect_true (!dir.exists(testOutputDir))
  
  # Test scenario 4::
  # If there is more than one location file, use the first
  testInputDir <-
    base::paste0(wk_dir, '/', 'pfs/prt_moreThanOneFile/2019/01/01/3119')
  dirInLoc <- base::paste0(testInputDir, '/location')
  fileLoc <- base::dir(dirInLoc)
  # numFileLoc <- base::length(fileLoc)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc, '/', fileLoc[1]))
  nameLoc <- loc$name
  sourceId <- loc$source_id
  
  testOutputDir = "pfs/out"
  testOutputDirPath <- base::paste0(testOutputDir, "/", installdate, collapse = '/')
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  testOutputDirSourceIdLoc <- base::paste0(testOutputDirPath, "/", nameLoc, "/location")
  expect_true (dir.exists(testOutputDirSourceIdLoc))
  
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
})
