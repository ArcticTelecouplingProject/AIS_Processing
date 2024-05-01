
# Load libraries 
library(tidyverse)
library(sf)

# Start timer
start <- proc.time()

# Import year from sb file 
year <- commandArgs(trailingOnly = TRUE)

# Load in files 
if(year %in% c(2021:2022)){
  # dir <- "D:/NSF_AIS_2021-2022/2021/"
  dir <- pase0("../Data_Raw/", year)
  files <- paste0(dir, list.files(dir, pattern='.csv'))
  # Separate file names into monthly lists
  jan <- files[grepl(paste0("exactEarth_", year, "01"), files)]
  feb <- files[grepl(paste0("exactEarth_", year, "02"), files)]
  mar <- files[grepl(paste0("exactEarth_", year, "03"), files)]
  apr <- files[grepl(paste0("exactEarth_", year, "04"), files)]
  may <- files[grepl(paste0("exactEarth_", year, "05"), files)]
  jun <- files[grepl(paste0("exactEarth_", year, "06"), files)]
  jul <- files[grepl(paste0("exactEarth_", year, "07"), files)]
  aug <- files[grepl(paste0("exactEarth_", year, "08"), files)]
  sep <- files[grepl(paste0("exactEarth_", year, "09"), files)]
  oct <- files[grepl(paste0("exactEarth_", year, "10"), files)]
  nov <- files[grepl(paste0("exactEarth_", year, "11"), files)]
  dec <- files[grepl(paste0("exactEarth_", year, "12"), files)]
}
if(year %in% 2015:2020){
  # dir <- paste0("D:/AlaskaConservation_AIS_20210225/Data_Raw/", year, "/")
  dir <- pase0("../Data_Raw/", year)
  files <- paste0(dir, list.files(dir, pattern='.csv'))
  # Iterate through and create list of lists of file names 
  jan <- files[grepl("-01-", files)]
  feb <- files[grepl("-02-", files)]
  mar <- files[grepl("-03-", files)]
  apr <- files[grepl("-04-", files)]
  may <- files[grepl("-05-", files)]
  jun <- files[grepl("-06-", files)]
  jul <- files[grepl("-07-", files)]
  aug <- files[grepl("-08-", files)]
  sep <- files[grepl("-09-", files)]
  oct <- files[grepl("-10-", files)]
  nov <- files[grepl("-11-", files)]
  dec <- files[grepl("-12-", files)]
}

# Create a list of lists of all csv file names grouped by month
csvsByMonth <- list(jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec)

flags <- read.csv("../Data_Raw/FlagCodes.csv")

scrambleids <- read.csv("../Data_Raw/ScrambledMMSI_Keys_2015-2022.csv") %>%
  dplyr::select(MMSI, scramblemmsi)

dest <- read.csv("../Data_Raw/Destination_Recoding.csv")

daynight <- FALSE

# Load functions
source("1-Functions.R")

## MSU HPCC: https://wiki.hpcc.msu.edu/display/ITH/R+workshop+tutorial#Rworkshoptutorial-Submittingparalleljobstotheclusterusing{doParallel}:singlenode,multiplecores
# Request a single node (this uses the "multicore" functionality)
registerDoParallel(cores=as.numeric(Sys.getenv("SLURM_CPUS_ON_NODE")[1]))

# create a blank list to store the results (I truncated the code before the ship-type coding, and just returned the sf of all that day's tracks so I didn't 
#       have to debug the raster part. If we're writing all results within the function - as written here and as I think we should do - the format of the blank list won't really matter.)
res=list()

# foreach and %dopar% work together to implement the parallelization
# note that you have to tell each core what packages you need (another reason to minimize library use), so it can pull those over
# I'm using tidyverse since it combines dplyr and tidyr into one library (I think)
res=foreach(i=1:12,.packages=c("tidyverse", "sf", "doParallel"),
            .errorhandling='pass',.verbose=T,.multicombine=TRUE) %dopar% 
  clean_and_vectorize(csvList=csvsByMonth[[i]], 
                      flags=flags, 
                      scrambleids=scrambleids,
                      dest=dest, 
                      daynight=FALSE)
# lapply(csvsByMonth, FWS.AIS)

# Elapsed time and running information
tottime <- proc.time() - start
tottime_min <- tottime[[3]]/60

cat("Time elapsed:", tottime_min, "\n")
cat("Currently registered backend:", getDoParName(), "\n")
cat("Number of workers used:", getDoParWorkers(), "\n")

