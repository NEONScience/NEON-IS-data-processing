# Specify directory paths
dirIn <- Sys.getenv("DIR_IN") # Input path placed into the container for each datum. DIR_IN is the input repo name.
dirOutBase <- '/pfs/out' # Base output path - MUST be /pfs/out for Pachyderm to recognize it as output
dirOut <- paste0(dirOutBase,'/',dirIn) # Specific output path for each datum

# Create some text for each datum path, and print it to screen
text <- paste('Hello. The datum path is',dirIn) 
print(text)

# Write an output file in the output path
dir.create(dirOut,recursive = TRUE) # Create the output directory in the container
nameFile <- paste0(dirOut,'/output.txt') # Name of the output file (full path)
write(text,nameFile)  # Write the output file
