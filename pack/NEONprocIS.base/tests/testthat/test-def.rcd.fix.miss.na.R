library(testthat)
source("R/def.rcd.fix.miss.na.R")

test_that("fixing the badtime values",
          {
            data <- NEONprocIS.base::def.read.parq(NameFile ="~/git/NEON-IS-data-processing/pack/NEONprocIS.base/tests/testthat/def.rcd.fix.miss.na/testdata.parquet")
            inputfilepaths <- c('~/git/NEON-IS-data-processing/pack/NEONprocIS.base/tests/testthat/def.rcd.fix.miss.na/testdata.parquet', '~/git/NEON-IS-data-processing/pack/NEONprocIS.base/tests/testthat/def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
            output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(data=data, timeBad=returnList$timeBad,valuBad='test')
            expect_true (length(output_filename$readout_time) == 6)
            testthat::equals(length(output_filename$suspectCalQF[6]), 'test')
          }

)
# test_that("Output when there is no extension in the nameFileIn",
#           {
# 
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName',prfx='Prefix_',sufx='_Suffix',ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName_Suffix.txt')
#           }
# 
# )
# 
# test_that("Output when there is no suffix",
#           {
# 
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName',prfx='Prefix_',ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName.txt')
#           }
# 
# )
# 
# test_that("Output when there is no prefix and suffix",
#           {
# 
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName',ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName.txt')
#           }
# 
# )
# 
# test_that("Output when there are multiple periods in the filenameIn",
#           {
# 
#             output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(nameFileIn='myFileName.ext1.ext2.ext3', prfx='Prefix_',sufx='_Suffix', ext='txt')
#             testthat::equals(output_filename, 'Prefix_myFileName.ext1.ext2_Suffix.txt')
#           }
# 
# )
# 
