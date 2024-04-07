dat_pat <- '/scratch/PrimaryPrecipAlgo/'
site <- 'konz'
prism <- read.csv(paste0(date_pat, site, '_prism.csv'))
sec <-  read.csv(paste0(dat_pat, site, '_sec.csv'))

sum(sec$value)
sum(prism$value)
