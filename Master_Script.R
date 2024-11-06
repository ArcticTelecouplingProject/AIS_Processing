
# Load libraries 
library(tidyverse)
library(sf)
library(stringi)
library(purrr)
library(foreach)
library(doParallel)
library(zoo)

# Start timer
start <- proc.time()

# Import year from sb file 
# year <- 2015
year <- commandArgs(trailingOnly = TRUE)

# Load in files 
if(year %in% c(2021:2022)){
  # dir <- paste0("D:/NSF_AIS_2021-2022/", year, "/")
  dir <- paste0("../Data_Raw/", year, "/")
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
  dir <- paste0("../Data_Raw/", year, "/")
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

hexgrid <- st_read("../Data_Raw/BlankHexes.shp")


# Load functions
# Get a list of all R script files in the R folder 
r_files <- list.files(path = "./R", pattern = "\\.R$", full.names = TRUE)
# Source each file to load the functions into memory
lapply(r_files[2:length(r_files)], source)


######################## TESTING ########################  
# csvsByMonth <- list(jul[1:2])
# csvList <- csvsByMonth[[1]]
# output <- "vector"
# daynight <- TRUE

# Example usage
# process_ais_data(csvList, year, flags, dest,
#                              daynight = TRUE,
#                              output = "vector",
#                              hexgrid = hexgrid,
#                              scrambleids = scrambleids)
#########################################################

## MSU HPCC: https://wiki.hpcc.msu.edu/display/ITH/R+workshop+tutorial#Rworkshoptutorial-Submittingparalleljobstotheclusterusing{doParallel}:singlenode,multiplecores
# Request a single node (this uses the "multicore" functionality)
registerDoParallel(cores=as.numeric(Sys.getenv("SLURM_CPUS_ON_NODE")[1]))

# create a blank list to store the results (I truncated the code before the ship-type coding, and just returned the sf of all that day's tracks so I didn't 
#       have to debug the raster part. If we're writing all results within the function - as written here and as I think we should do - the format of the blank list won't really matter.)
res=list()

# foreach and %dopar% work together to implement the parallelization
# note that you have to tell each core what packages you need (another reason to minimize library use), so it can pull those over
# I'm using tidyverse since it combines dplyr and tidyr into one library (I think)
res=foreach(i=1:length(csvsByMonth),.packages=c("tidyverse", "sf", "doParallel", "stringi"),
            .errorhandling='pass',.verbose=T,.multicombine=TRUE) %dopar% 
            process_ais_data(csvList=csvsByMonth[[i]], 
                             year = year, 
                             flags = flags, 
                             dest = dest,
                             daynight = FALSE,
                             output = "vector",
                             hexgrid = hexgrid,
                             scrambleids = scrambleids)

# Elapsed time and running information
tottime <- proc.time() - start
tottime_min <- tottime[[3]]/60

cat("Time elapsed:", tottime_min, "\n")
cat("Currently registered backend:", getDoParName(), "\n")
cat("Number of workers used:", getDoParWorkers(), "\n")

