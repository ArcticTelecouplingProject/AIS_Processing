################################################################################
# TITLE: Reformat output raster and vector data
# PURPOSE: This script takes monthly vector and raster files by ship type and 
# resaves them as raster stacks by ship type and annual vector files. 
# AUTHOR: Kelly Kapsar
# CREATED: 2022-10-11
# LAST UPDATED ON: 2022-10-11 

# NOTE: 
################################################################################
library(sf)
library(dplyr)
library(terra)

# Setup to see syntax - yours will vary based on input/output file structure
raspath <- "D:/AIS_V2_DayNight_60km6hrgap/Raster/"
vpath <- "D:/AIS_V2_DayNight_60km6hrgap/VectorOld/"

vecs <- list.files(vpath, pattern='.shp', full.names = F)
vecsfull <- list.files(vpath, pattern='.shp', full.names = T)

# Remove identifying information from vector lines 
for(i in 1:length(vecsfull)){
  if(i %/% 10 == 0){
    print(i)
  }
  temp <- st_read(vecsfull[i])
  tempnew <- temp %>% dplyr::select(-IMO, -Vssl_Nm)
  st_write(tempnew, paste0("D:/AIS_V2_DayNight_60km6hrgap/Vector/", vecs[[i]]))
}

# Aggregate vector lines into annual summaries
for(i in 2015:2020){
  print(i)
  yrvecs <- vecsfull[grep(i, vecsfull)]
  yrvecs <- lapply(yrvecs, st_read)
  yr <- do.call(rbind, yrvecs)
  yrnew <- yr %>% dplyr::select(-IMO, -Vssl_Nm)
  st_write(yr, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/Vector_", i, ".shp"))
}


# Reformat individual rasters into raster stacks by vessel type
AA <- "+proj=aea +lat_0=50 +lon_0=-154 +lat_1=55 +lat_2=65 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs"

saverasters <- list.files(raspath, pattern='.tif.aux')
saverasters <- substr(saverasters, 11,nchar(saverasters)-17)

# Vessel types
shiptypes <- c("Cargo", "Fishing", "Other", "Passenger", "Pleasure", "Sailing", "Tanker", "Tug")

# Get layer names together 
seqdates <- seq.Date(as.Date("2015-01-01", format="%Y-%m-%d"), as.Date("2020-12-31", format="%Y-%m-%d"), by="month")
mons <- substr(seqdates, 1,nchar(as.character(seqdates))-3)

for(i in 1:length(shiptypes)){
  print(i)
  temp <- saverasters[grep(shiptypes[i], saverasters)]
  temp <- substr(temp, 1, nchar(temp)-8)
  temp <- paste0(raspath,temp)
  tempnew <- lapply(temp, terra::rast)
  rstack <- terra::rast(tempnew)
  names(rstack) <- mons
  terra::crs(rstack) <- AA 
  values(rstack) <- values(rstack)/1000 # convert to km 
  terra::writeRaster(rstack, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/AISRaster_",shiptypes[i],"_25kmRes_kmUnits.tif"), overwrite=T)
  rm(temp)
  rm(tempnew)
}





