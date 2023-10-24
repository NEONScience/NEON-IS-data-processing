#
# get data paths for all days, for example, ECTE planar fit processing
# requests data files from 4 days before and 4 days after current day,
# total 9 days' data were needed.
# this function will return path for all (9) days
#
get.all.days.path <- function(original_directory, currdate, offDays, log=NULL) {
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Convert the path into a character vector by splitting it using '/'
  path_elements <- unlist(strsplit(original_directory, '/'))
  # Find the date element in the path
  date_index <- which(path_elements %in% substr(currdate, 1, 4))
  
  if (length(date_index) < 1) {
    log$error("input directory does not contain the provided date.")
    return(NULL)
  }
  
  # Extract the date from the path
  date <- as.Date(paste(path_elements[date_index], path_elements[date_index + 1], path_elements[date_index + 2], sep = '/'))
    
  # Calculate and format the new dates as 'YYYY/MM/DD'
  new_dates <- seq(date-offDays, date+offDays, by='days')
  new_date_strings <- format(new_dates, format='%Y/%m/%d')
  
  # Create the new paths by replacing the original date with the new dates
  new_paths <- lapply(new_date_strings, function(date) {
    path_elements[date_index:(date_index + 2)] <- unlist(strsplit(date, '/'))
    paste(path_elements, collapse = '/')
  })
  base::names(new_paths) <- new_dates
  
  return(new_paths)
}


###############################
get.all.days.tmp.path <- function(original_name, currdate, planar_window) {
  
  new_dates <- seq(date-planar_window+1, as.Date(currdate), by='days')

  idx <- unlist(gregexpr(pattern=currdate, original_name))
  
  # Create the new paths by replacing the original date with the new dates
  new_paths <- lapply(new_dates, function(date) {
    paste0(substr(original_name,1,idx-1),date,".h5")
  })

  return(unlist(new_paths))
}
