###############################################################################################

source('../../../../neon-package-loader.R')
load_neon_base()

test_that("multiplication works", {
  expect_equal(2 * 2, 4)
})
