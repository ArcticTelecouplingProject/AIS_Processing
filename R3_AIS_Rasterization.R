################################################################################
# TITLE: AIS Rasterization Script
# PURPOSE: This script takes AIS vector daily transit segments and transforms
  # them into raster pixels of a specified resolution containing total length of 
  # vessel traffic.
# AUTHOR: Ben Sullender (modified by Kelly Kapsar)
# CREATED: 2021 
# LAST UPDATED ON: ??
# 
# NOTE: 
################################################################################

# Load Libraries 
library(spatstat)
library(rgdal)
library(raster)
library(maptools)
library(sf)


AIS.Rasta <- function(filename, vectorName, savedsn, cellsize=25000, nightonly=FALSE){
  # start timer 
  starttime <- proc.time()
  
  #read line shapefile
  # moSHP <- readOGR(dsn, vectorName)
  temp <- st_read(filename)
  
  if(nightonly == TRUE){
    temp <- temp[temp$timefdy == "night",]
  }
  # test <- readOGR(paste0(path, files[[1]]))
  moSHP <- as(temp, "Spatial")
  
  
  #convert to spatial lines format - this step takes the longest
  moPSP <- as.psp(moSHP)
  
  #create bounding extent for all area
  extentAOI <- as.owin(list(xrange=c(-2550000,550000),yrange=c(235000,2720000)))
                  
  #create mask with pixel size
  allMask <- as.mask(extentAOI,eps=cellsize)
  
  #run pixellate with mask
  moPXL <- pixellate.psp(moPSP,W=allMask)
  
  #render as raster
  moRAST <- raster(moPXL)
  
  cellsize_km <- cellsize/1000
  
  #write output
  if(nightonly==FALSE){
    writeRaster(moRAST,paste0(savedsn,"AISRaster",substr(vectorName,7,nchar(vectorName)),"_",cellsize_km,"km",".tif"))
  }
  if(nightonly==TRUE){
    writeRaster(moRAST,paste0(savedsn,"AISRaster",substr(vectorName,7,nchar(vectorName)),"_",cellsize_km,"km_NightOnly",".tif")) 
  }
  
  runtime <- proc.time() - starttime 
  print(runtime)
}

# Setup to see syntax - yours will vary based on input/output file structure
path <- "D:/AIS_V2_DayNight_60km6hrgap/Vector/"

files <- list.files(path, pattern='.shp')

savedsn <- "D:/AIS_V2_DayNight_60km6hrgap/Raster/"

# the "-4" part gets rid of ".shp"
inS <- substr(files,1,nchar(files)-4)


start <- proc.time()
lapply(inS, function(x){AIS.Rasta(filename= paste0(path, x, ".shp"), vectorName=x, savedsn=savedsn, cellsize=25000, nightonly=FALSE)}) 
tottime <- proc.time()-start
browseURL("https://www.youtube.com/watch?v=AZQxH_8raCI&ab_channel=worldslover234")

# wd <- "D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2020/Raster"
# files <- paste0(wd,"/",list.files(wd, pattern='.tif'))
# file.rename(files, gsub(".tif","_25km.tif",files))

