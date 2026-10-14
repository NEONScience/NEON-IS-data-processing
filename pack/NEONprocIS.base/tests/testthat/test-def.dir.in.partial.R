test_that("Input directory is returned when all partial child names match",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27134"
            subDirPartial <- c("dat", "calib")
            dirIn <- NEONprocIS.base::def.dir.in.partial(
              DirBgn = dirBgn,
              nameDirSubPartial = subDirPartial
            )
            dirOut <- "def.dir.in/test_input/pfs/27134/2019/01/01"

            testthat::expect_equal(dirOut, dirIn)
          })

test_that("Partial child names must match under the same input directory",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27135"
            subDirPartial <- c("data", "calib")
            dirIn <- NEONprocIS.base::def.dir.in.partial(
              DirBgn = dirBgn,
              nameDirSubPartial = subDirPartial
            )

            testthat::expect_equal(character(0), dirIn)
          })

test_that("When no partial names are provided, terminal directories are datums",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27135"
            dirIn <- NEONprocIS.base::def.dir.in.partial(
              DirBgn = dirBgn,
              nameDirSubPartial = NULL
            )
            dirOut1 <- "def.dir.in/test_input/pfs/27135/outer/data"
            dirOut2 <- "def.dir.in/test_input/pfs/27135/outer/inner/calibration"

            testthat::expect_equal(c(dirOut1, dirOut2), dirIn)
          })