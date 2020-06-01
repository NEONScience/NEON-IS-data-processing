#library(testthat)
#source("R/def.file.name.out.R")

test_that("Output path with prefix filename suffix and extension",
          {

            output_filename <- NEONprocIS.base::def.file.name.out(nameFileIn='myFileName.json',prfx='Prefix_',sufx='_Suffix',ext='txt')
            testthat::equals(output_filename, 'Prefix_myFileName_Suffix.txt')
          }

)

test_that("Output when there is no extension in the nameFileIn",
          {

            output_filename <- NEONprocIS.base::def.file.name.out(nameFileIn='myFileName',prfx='Prefix_',sufx='_Suffix',ext='txt')
            testthat::equals(output_filename, 'Prefix_myFileName_Suffix.txt')
          }

)

test_that("Output when there is no suffix",
          {

            output_filename <- NEONprocIS.base::def.file.name.out(nameFileIn='myFileName',prfx='Prefix_',ext='txt')
            testthat::equals(output_filename, 'Prefix_myFileName.txt')
          }

)

test_that("Output when there is no prefix and suffix",
          {

            output_filename <- NEONprocIS.base::def.file.name.out(nameFileIn='myFileName',ext='txt')
            testthat::equals(output_filename, 'myFileName.txt')
          }

)

test_that("Output when there are multiple periods in the filenameIn",
          {

            output_filename <- NEONprocIS.base::def.file.name.out(nameFileIn='myFileName.ext1.ext2.ext3', prfx='Prefix_',sufx='_Suffix', ext='txt')
            testthat::equals(output_filename, 'Prefix_myFileName.ext1.ext2_Suffix.txt')
          }

)

