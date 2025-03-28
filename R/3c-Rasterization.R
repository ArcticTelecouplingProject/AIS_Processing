################################################################################
# TITLE: AIS Rasterization Script
# PURPOSE: Converts AIS vector daily transit segments into raster pixels 
#          of a specified resolution, representing the total length of 
#          vessel traffic.
# AUTHOR: Ben Sullender (modified by Kelly Kapsar)
# CREATED: 2021 
# LAST UPDATED ON: 2025-03-18
################################################################################

# Load required libraries 
# library(rgdal)   # For spatial data operations
# library(raster)  # For raster manipulation
# library(maptools) # For spatial tools
# library(sf)      # For reading vector data
# library(spatstat.geom) # For spatial statistics

#' AIS Rasterization Function
#'
#' This function reads a vector shapefile of vessel transit segments, 
#' converts it into a raster grid, and saves the output as a GeoTIFF file.
#'
#' @param filename Character. The full path to the input shapefile.
#' @param vectorName Character. The base name of the vector file.
#' @param savedsn Character. The directory path where output raster should be saved.
#' @param cellsize Numeric. The resolution of the output raster in meters (default: 4000m).
#' @param nightonly Logical. If TRUE, only nighttime data will be processed (default: FALSE).
#'
#' @return Saves a raster file and prints processing time.
#' @export
AIS.Rasta <- function(filename, vectorName, savedsn, cellsize=4000, nightonly=FALSE) {
  
  print(filename)  # Print the filename for tracking progress
  starttime <- proc.time()  # Start timer for execution time tracking
  
  # Read vector shapefile
  temp <- st_read(filename, quiet = TRUE)
  
  # Convert to Spatial format
  moSHP <- as(temp, "Spatial")
  
  # Convert to spatial lines format (this step can be time-consuming)
  moPSP <- as.psp(moSHP)
  
  # Define the bounding extent for the area of interest (AOI)
  extentAOI <- as.owin(list(xrange=c(-2550000,550000), yrange=c(235000,2720000)))
  
  # Create a raster mask with the specified pixel size
  allMask <- as.mask(extentAOI, eps=cellsize)
  
  # Rasterize the spatial line segments
  moPXL <- pixellate.psp(moPSP, W=allMask)
  
  # Convert to raster format
  moRAST <- raster(moPXL)
  
  # Convert cell size to kilometers for output file naming
  cellsize_km <- cellsize / 1000
  
  # Construct output filename and save raster
  output_filename <- paste0(savedsn, "AISRaster", substr(vectorName, 7, nchar(vectorName)), "_", cellsize_km, "km", 
                            ifelse(nightonly, "_NightOnly", ""), ".tif")
  writeRaster(moRAST, output_filename)
  
  # Calculate and print runtime
  runtime <- proc.time() - starttime 
  print(runtime)
}

################################################################################
# Example Usage (Uncomment and Modify as Needed)
################################################################################

# Define file paths and parameters
# path <- "D:/AIS_V2_DayNight_60km6hrgap/Vector/"
# files <- list.files(path, pattern='.shp')
# savedsn <- "D:/AIS_V2_DayNight_60km6hrgap/Raster/"

# Get base names of shapefiles and filter out already processed ones
# saverasters <- list.files(savedsn, pattern='.tif.aux')
# saverasters <- substr(saverasters, 11, nchar(saverasters) - 17)
# inS <- substr(files, 1, nchar(files) - 4)
# inSnew <- inS[!substr(inS, 8, nchar(inS)) %in% saverasters]

# Process each new shapefile using lapply
# start <- proc.time()
# lapply(inSnew, function(x) {
#   AIS.Rasta(filename = paste0(path, x, ".shp"), vectorName = x, 
#             savedsn = savedsn, cellsize = 4000, nightonly = FALSE)
# })
# total_time <- proc.time() - start

# Rename output raster files (Example Usage)
# wd <- "D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2020/Raster"
# files <- paste0(wd, "/", list.files(wd, pattern='.tif'))
# file.rename(files, gsub(".tif", "_25km.tif", files))
