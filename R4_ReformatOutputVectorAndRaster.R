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
vpath <- "D:/AIS_V2_DayNight_60km6hrgap/Vector/"

vecs <- list.files(vpath, pattern='.shp', full.names = F)
vecsfull <- list.files(vpath, pattern='.shp', full.names = T)


# Remove identifying information from vector lines 
vecs21 <- vecsfull[grep(2021, vecsfull)]
vecs22 <- vecsfull[grep(2022, vecsfull)]

for(i in vecs21){
  print(i)
  temp <- st_read(i, quiet=T)
  if("Cntry_C" %in% colnames(temp)){
    temp <- temp %>% dplyr::rename(CntryCd = Cntry_C)
    st_write(temp, i,append = FALSE)
  }
}

for(i in vecs22){
  print(i)
  temp <- st_read(i, quiet=T)
  if("Cntry_C" %in% colnames(temp)){
    temp <- temp %>% dplyr::rename(CntryCd = Cntry_C)
    st_write(temp, i,append = FALSE)
  }
}

# Aggregate vector lines into annual summaries
for(i in 2015:2022){
  print(i)
  yrvecs <- vecsfull[grep(i, vecsfull)]
  yrvecs <- lapply(yrvecs, st_read)
  yr <- do.call(rbind, yrvecs)
  yrnew <- yr %>% dplyr::select(-IMO, -Vssl_Nm)
  st_write(yr, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/Vector_", i, ".shp"))
}

# Aggregate vector lines by vessel type
types <- c("Cargo", "Tanker", "Fishing", "Tug", "Pleasure", "Passenger", "Sailing", "Other")

for(i in 4:length(types)){
  temp <- types[[i]]
  print(temp)
  typevecs <- vecsfull[grep(temp, vecsfull)]
  typevecs <- lapply(typevecs, st_read)
  typ <- dplyr::bind_rows(typevecs)
  typ$yr <- lubridate::year(typ$Tm_Strt)
  typ$mnth <- lubridate::month(typ$Tm_Strt)
  typ$day <- lubridate::day(typ$Tm_Strt)
  
  st_write(typ, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/Vector_", temp, ".shp"))
}

browseURL("https://www.youtube.com/watch?v=K1b8AhIsSYQ")

# Have to save fishing data in two separate files because it's too big for one shapefile
typ15 <- typ[typ$yr %in% 2015:2018,]
st_write(typ15, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/Vector_", temp, "_a.shp"))

typ19 <- typ[typ$yr %in% 2019:2020,]
st_write(typ19, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/Vector_", temp, "_b.shp"))
 
typ21 <- typ[typ$yr %in% 2021:2022,]
st_write(typ21, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/Vector_", temp, "_c.shp"))

###################################################################
# RASTER
###################################################################
raspath <- "D:/AIS_V2_DayNight_60km6hrgap/Raster/"

# Reformat individual rasters into raster stacks by vessel type
AA <- "+proj=aea +lat_0=50 +lon_0=-154 +lat_1=55 +lat_2=65 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs"

saverasters <- list.files(raspath, pattern='.tif')
# saverasters <- substr(saverasters, 11,nchar(saverasters)-17)

# Vessel types
shiptypes <- c("Cargo", "Fishing", "Other", "Passenger", "Pleasure", "Sailing", "Tanker", "Tug")

# Get layer names together 
seqdates <- seq.Date(as.Date("2015-01-01", format="%Y-%m-%d"), as.Date("2022-12-31", format="%Y-%m-%d"), by="month")
mons <- substr(seqdates, 1,nchar(as.character(seqdates))-3)

# Summer (JJA)
for(i in 1:length(shiptypes)){
  print(i)
  temp <- saverasters[grep(shiptypes[i], saverasters)]
  temp <- temp[grep("-06-|-07-|-08-", temp)]
  temp <- paste0(raspath,temp)
  tempnew <- lapply(temp, terra::rast)
  rstack <- terra::rast(tempnew)
  sumras <- sum(rstack)
  terra::crs(sumras) <- AA 
  values(sumras) <- values(sumras)/1000 # convert to km 
  terra::writeRaster(sumras, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/AISRaster_",shiptypes[i],"_Summer_4kmRes_kmUnits.tif"), overwrite=T)
  rm(temp)
  rm(tempnew)
}

# Fall (SON)
for(i in 1:length(shiptypes)){
  print(i)
  temp <- saverasters[grep(shiptypes[i], saverasters)]
  temp <- temp[grep("-09-|-10-|-11-", temp)]
  temp <- paste0(raspath,temp)
  tempnew <- lapply(temp, terra::rast)
  rstack <- terra::rast(tempnew)
  sumras <- sum(rstack)
  terra::crs(sumras) <- AA 
  values(sumras) <- values(sumras)/1000 # convert to km 
  terra::writeRaster(sumras, paste0("D:/AIS_V2_DayNight_60km6hrgap/Reformatted/AISRaster_",shiptypes[i],"_Fall_4kmRes_kmUnits.tif"), overwrite=T)
  rm(temp)
  rm(tempnew)
}




