# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

print(arg)
print(length(arg))