################################################################################
# TITLE: Destination coding addition to vector data
# PURPOSE: This script takes monthly vector files by ship type and adds manually
# interpreted revisions of destination codes to individual track lines. 
# AUTHOR: Kelly Kapsar & Bella Block
# CREATED: 2023-04-27
# LAST UPDATED ON: 2023-04-27
# 
# NOTE: 
################################################################################

# Load libraries
library(sf)
library(dplyr)

# Outpath 
outpath <- "D:/AIS_V2_DayNight_60km6hrgap/Vector_Destinations/"
# outpath <- "D:/AIS_V2_DayNight_60km6hrgap/Vector_Destinations/"

# Create list of files 
vpath <- "D:/AIS_V2_DayNight_60km6hrgap/Vector/"

vecs <- list.files(vpath, pattern='.shp', full.names = F)
vecsfull <- list.files(vpath, pattern='.shp', full.names = T)

# Read in codes. 
destpath <- "../Data_Processed/Destination_Recoding.csv"
dest <- read.csv(destpath)

miss <- c()

# for(i in 1:3){
for(i in 1:length(vecsfull)){
  if(i %% 10 == 0){print(paste0(i, " of ", length(vecsfull), " files recoded."))}
  # read in one month of data
  vec <- st_read(vecsfull[[i]], quiet = TRUE)
  # unique(vec$Destntn)
  
  # determine which cells have matching destination codes in destination database
  # !vec$Destntn %in% dest$Destntn
  
  miss <- c(miss, vec$Destntn[which(!vec$Destntn %in% dest$Destntn)])
  
  # Determine percentage of cells that have matching destination codes in destination database
  # sum(unique(vec$Destntn) %in% unique(dest$Destntn))/length(unique(vec$Destntn))
  
  # length(unique(dest$Destntn))
  
  # Remove duplicated values for destntn code
  # destuniq <- dest[!duplicated(dest$Destntn),]
  
  vecnew <- vec %>% left_join(dest, by="Destntn")
  
  st_write(vecnew, paste0(outpath, vecs[[i]]), quiet = TRUE)
  rm(vecsnew)
  rm(vec)
}

write.csv(unique(miss), "../Data_Processed/missing_destination_codes.csv")

browseURL("https://www.youtube.com/watch?v=K1b8AhIsSYQ&pp=ygUSd2UgYnVpbHQgdGhpcyBjaXR5")
