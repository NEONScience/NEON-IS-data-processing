pat <- '/scratch/prism/raw_data'

filez <- list.files(path = pat, full.names = T)

for (file in filez){
  
df <- read.csv(filez)
site <- stringr::str_extract(file, pattern = '[A-Z]{4}')

df$datetime <- as.POSIXct(df$date/1000, origin = '1970-01-01')
df$startDate <- as.Date(df$datetime)

df <- df[,c('startDate','ppt')]

write.csv( df, file = paste0('/scratch/prism/current_2016_2024_', site, '.csv'), row.names = F)

}
