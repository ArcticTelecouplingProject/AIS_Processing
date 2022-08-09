################################################################################
# TITLE: AIS Vectorization Script
# PURPOSE: This script takes individual daily csv files from exactEarth, cleans
  # data to remove erroneous points, and linearly interpolates to create daily
  # transit segments for individual vessels in vector format. 
# AUTHOR: Ben Sullender & Kelly Kapsar
# CREATED: 2021
# LAST UPDATED ON: -- 
# 
# NOTE: Known issue with speed filter -- it does not remove first points if they 
  # are errneous. 
# NOTE 2: CODE DESIGNED TO RUN ON HPCC.
################################################################################

# Start timer
start <- proc.time()

# Load libraries 
library(maptools)
library(rgdal)
library(dplyr)
library(tidyr)
library(tibble)
library(sf)
library(foreach)
library(doParallel)

####################################################################
##################### AIS PROCESSING FUNCTION ######################
####################################################################

# INPUTS: A list of lists containing all daily csv file names for one year of AIS data.  
## Inner list = file paths/names for daily AIS csvs
## Outer list = a list of months 

# OUTPUTS: 
## Monthly shapefiles for each ship type containing vectorized daily ship transit segments
## Text file with output information (number of unique ships, rows excluded, etc.)

FWS.AIS <- function(csvList){
  # start timer 
  starttime <- proc.time()
  
  # clear variables
  AIScsv <- NA
  AIScsvDF <- NA
  AISlookup <- NA
  temp <- NA
  
  start <- proc.time()
  # read in csv (specifying only columns that we want based on position in dataframe)
  temp <-  lapply(csvList, read.csv, header=TRUE, na.strings=c("","NA"),
                  colClasses = c(rep("character", 2), "NULL", "character", "NULL", "NULL", 
                                 "character", rep("NULL", 6), "character", "NULL", rep("character",8),
                                 "NULL", "character", "NULL", "character", "NULL", rep("character", 2), rep("NULL", 109)
                  ))
  
  
  AIScsv <- do.call(rbind , temp)
  
  importtime <- (proc.time() - start)[[3]]/60
  start <- proc.time()
  
  # Create AIS_ID field
  AIScsv <- AIScsv %>% add_column(AIS_ID = paste0(AIScsv$MMSI,"-",substr(AIScsv$Time,1,8)))
  
  orig_aisids <- length(unique(AIScsv$AIS_ID))
  orig_MMSIs <- length(unique(AIScsv$MMSI))
  
  
  # Convert character columns to numeric as needed
  numcols <- c(1:2, 6:12, 14:17)
  AIScsv[,numcols] <- lapply(AIScsv[,numcols], as.numeric)
  
  # this will come in handy later. chars 28 to 34 = "yyyy-mm"
  MoName <- substr(csvList[[1]][1],45, 51)
  yr <- substr(MoName, 1, 4) 
  mnth <- substr(MoName, 6, 7)
  print(paste0("Processing",yr, mnth))
  
  # create df
  # we only care about lookup col, time, lat + long, and non-static messages
  AIScsvDF <- AIScsv %>%
    dplyr::select(MMSI,Latitude,Longitude,Time,Message_ID,AIS_ID,SOG) %>%
    filter(Message_ID!=c(5,24)) %>%
    filter(!is.na(Latitude)) %>%
    filter(!is.na(Longitude)) %>%
    filter(nchar(trunc(abs(MMSI))) > 8)
  
  filt_aisids <- length(unique(AIScsvDF$AIS_ID))
  filt_mmsis <- length(unique(AIScsvDF$MMSI))

  dftime <- (proc.time() - start)[[3]]/60
  start <- proc.time()
  
  # Filter out points > 100 km/hr 
  AISspeed <- AIScsvDF %>%
    mutate(Time = as.POSIXct(Time, format="%Y%m%d_%H%M%OS")) %>% 
    st_as_sf(coords=c("Longitude","Latitude"),crs=4326) %>%
    # project into Alaska Albers (or other CRS that doesn't create huge gap in mid-Bering with -180W and 180E)
    st_transform(crs=3338) %>%
    mutate(speed = NA) %>% arrange(Time)
  
  # Calculate the euclidean speed between points
  euclidean_speed <- function(lat2, lat1, long2, long1, time2, time1) {
    latdiff <- lat2 - lat1
    longdiff <- long2 - long1
    distance <- sqrt(latdiff^2 + longdiff^2)/1000
    timediff <- as.numeric(difftime(time2,time1,units=c("hours")))
    return(distance / timediff)
  }
  
  # Calculate 
  AISspeed[, c("long", "lat")] <- st_coordinates(AISspeed)
  AISspeed <- AISspeed %>% 
    group_by(AIS_ID) %>%
    arrange(AIS_ID, Time) %>% 
    mutate(speed = euclidean_speed(lat, lag(lat), long, lag(long), Time, lag(Time)))
  
  toofast_pts <- length(which(AISspeed$speed >= 100))
  AISspeed <- AISspeed %>% filter(speed < 100 | is.na(speed))
  
  # Remove AIS_IDs with only one point 
  SingleAISid <- AISspeed %>% st_drop_geometry() %>% group_by(AIS_ID) %>% summarize(n=n()) %>% filter(n <= 3)
  Short_aisids <- length(SingleAISid$AIS_ID)
  AISspeed <- AISspeed[!(AISspeed$AIS_ID %in% SingleAISid$AIS_ID),] 
  
  speedtime <- (proc.time() - start)[[3]]/60
  start <- proc.time()
  
  # create sf lines by sorted / grouped points
  AISsf <- AISspeed %>%
    arrange(Time) %>%
    # create 1 line per AIS ID
    group_by(AIS_ID) %>%
    # keep MMSI for lookup / just in case; do_union is necessary for some reason, otherwise it throws an error
    summarize(MMSI=first(MMSI),do_union=FALSE, npoints=n()) %>%
    st_cast("LINESTRING") %>% 
    st_make_valid()

  # Figure out which rows aren't LINESTRINGS and remove from data 
  notlines <- AISsf[which(st_geometry_type(AISsf) != "LINESTRING"),]
  AISsf <- AISsf[-which(st_geometry_type(AISsf) != "LINESTRING"),]
  NotLine_aisids <- length(notlines$AIS_ID)
  
  linetime <- (proc.time() - start)[[3]]/60
  start <- proc.time()
  
  # create lookup table
  # we only care about 7 columns in total: lookup col (MMSI + date), name + IMO (in case we have duplicates / want to do an IMO-based lookup in the future),
  #     ship type, and size (in 3 cols: width, length, Draught)
  #     I'm including Destination and Country because that would be dope! We could do stuff with innocent passage if that's well populated.
  AISlookup <- AIScsv %>%
    add_column(DimLength = AIScsv$Dimension_to_Bow+AIScsv$Dimension_to_stern, DimWidth = AIScsv$Dimension_to_port+AIScsv$Dimension_to_starboard) %>%
    dplyr::select(-Dimension_to_Bow,-Dimension_to_stern,-Dimension_to_port,-Dimension_to_starboard) %>%
    filter(Message_ID==c(5,24)) %>%
    filter(nchar(trunc(abs(MMSI))) > 8) %>% 
    distinct(AIS_ID, .keep_all=TRUE)
  
  InSfNotLookup_aisids <- length(AISsf$AIS_ID[!(AISsf$AIS_ID %in% AISlookup$AIS_ID)])
  InLookupNotSf_aisids <- length(AISlookup$AIS_ID[!(AISlookup$AIS_ID %in% AISsf$AIS_ID)])
  
  # step 2: join lookup table to the lines 
  AISjoined <- AISsf %>%
    left_join(AISlookup,by="AIS_ID")
  
  # step 3: split lines by ship type
  # link to ship type/numbers table: 
  # https://help.marinetraffic.com/hc/en-us/articles/205579997-What-is-the-significance-of-the-AIS-Shiptype-number-
    AISjoined <- AISjoined %>%
      mutate(AIS_Type = case_when(
      is.na(AISjoined$Ship_Type) ~ "Other",
      substr(AISjoined$Ship_Type,1,2)==30 ~ "Fishing",
      substr(AISjoined$Ship_Type,1,1)==7 ~ "Cargo",
      substr(AISjoined$Ship_Type,1,1)==8 ~ "Tanker",
      # from trial and error, I think that this last "TRUE" serves as a catch-all, but I can't logically figure out why it works. -\__(%)__/-
      TRUE ~ "Other"
    ))  
  
  ntank_aisids <- length(which(AISjoined$AIS_Type == "Tanker"))
  nfish_aisids <- length(which(AISjoined$AIS_Type == "Fishing"))
  ncargo_aisids <- length(which(AISjoined$AIS_Type == "Cargo"))
  nother_aisids <- length(which(AISjoined$AIS_Type == "Other"))
  ntotal_aisids <- length(AISjoined$AIS_Type)
  nmmsi <- length(unique(AISjoined$MMSI.x))
  
  jointime <- (proc.time() - start)[[3]]/60
  start <- proc.time()
  
  # Loop through each ship type, rasterize, and save shp file as well 
  allTypes <- unique(AISjoined$AIS_Type)
  
  for (k in 1:length(allTypes)){

          AISfilteredType <- AISjoined %>%
                filter(AIS_Type==allTypes[k])

          # Save data in vector format
          write_sf(AISfilteredType,paste0("../Data_Processed/Vector/Tracks_",MoName,"-",allTypes[k],".shp"))
  }
  
# Save processing info to text file 
vectortime <- (proc.time() - start)[[3]]/60

runtime <- proc.time() - starttime 
runtime_min <- runtime[[3]]/60 
summarystats <- data.frame(cbind(yr, mnth, runtime_min, orig_aisids, orig_MMSIs, 
                                 filt_aisids,filt_mmsis,Short_aisids, toofast_pts, NotLine_aisids, InSfNotLookup_aisids, InLookupNotSf_aisids,
                   ntank_aisids, ncargo_aisids, nfish_aisids, nother_aisids, ntotal_aisids, nmmsi))
write.csv(summarystats, paste0("../Data_Processed/Metadata/Metadata_",MoName,".csv"))


runtimes <- data.frame(cbind(yr, mnth, runtime_min, importtime, dftime, speedtime, linetime, jointime, vectortime)) 
write.csv(runtimes, paste0("../Data_Processed/Metadata/Runtimes_",MoName,".csv"))
print(runtimes)
return(runtimes)
}

####################################################################
####################### PARALLELIZATION CODE ####################### 
####################################################################

# Pull up list of AIS files
files <- paste0("../Data_Raw/2020/", list.files("../Data_Raw/2020", pattern='.csv'))

# Separate file names into monthly lists
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

# Create a list of lists of all csv file names grouped by month
csvsByMonth <- list(jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec)


## MSU HPCC: https://wiki.hpcc.msu.edu/display/ITH/R+workshop+tutorial#Rworkshoptutorial-Submittingparalleljobstotheclusterusing{doParallel}:singlenode,multiplecores
# Request a single node (this uses the "multicore" functionality)
registerDoParallel(cores=as.numeric(Sys.getenv("SLURM_CPUS_ON_NODE")[1]))

# create a blank list to store the results (I truncated the code before the ship-type coding, and just returned the sf of all that day's tracks so I didn't 
#       have to debug the raster part. If we're writing all results within the function - as written here and as I think we should do - the format of the blank list won't really matter.)
res=list()

# foreach and %dopar% work together to implement the parallelization
# note that you have to tell each core what packages you need (another reason to minimize library use), so it can pull those over
# I'm using tidyverse since it combines dplyr and tidyr into one library (I think)
res=foreach(i=1:12,.packages=c("maptools", "rgdal", "dplyr", "tidyr", "tibble", "stars", "raster", "foreach", "doParallel"),
            .errorhandling='pass',.verbose=T,.multicombine=TRUE) %dopar% FWS.AIS(csvList=csvsByMonth[[i]])
# lapply(csvsByMonth, FWS.AIS)

# Elapsed time and running information
tottime <- proc.time() - start
tottime_min <- tottime[[3]]/60

cat("Time elapsed:", tottime_min, "\n")
cat("Currently registered backend:", getDoParName(), "\n")
cat("Number of workers used:", getDoParWorkers(), "\n")

