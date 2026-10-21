test_that("Directories with matching partial names are returned",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27134"
            subDirPartial <- "dat"
            dirIn <- NEONprocIS.base::def.dir.in.partial(
              DirBgn = dirBgn,
              nameDirSubPartial = subDirPartial
            )
            dirOut <- "def.dir.in/test_input/pfs/27134/2019/01/01"

            testthat::expect_equal(dirOut, dirIn)
          })

test_that("Matching directories are returned at their own nesting level",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27135"
            subDirPartial <- "calib"
            dirIn <- NEONprocIS.base::def.dir.in.partial(
              DirBgn = dirBgn,
              nameDirSubPartial = subDirPartial
            )
            dirOut <- "def.dir.in/test_input/pfs/27135/outer/inner"

            testthat::expect_equal(dirOut, dirIn)
          })

test_that("When no partial names are provided, terminal directories are returned",
          {
            dirBgn <- "def.dir.in/test_input/pfs/27135"
            dirIn <- NEONprocIS.base::def.dir.in.partial(
              DirBgn = dirBgn,
              nameDirSubPartial = NULL
            )
            dirOut <- c(
              "def.dir.in/test_input/pfs/27135/outer/data",
              "def.dir.in/test_input/pfs/27135/outer/inner/calibration"
            )

            testthat::expect_equal(dirOut, dirIn)
          })