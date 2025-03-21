################################################################################
# TITLE: Hex Grid Creation Script
# PURPOSE:  Script that creates a hex grid for the Alaska Conservation Fund 
  # AIS study area 
# AUTHOR: Ben Sullender, Kelly Kapsar
# CREATED: 2021-08-09
# LAST UPDATED ON: 2024-10-22
# 
# NOTE: This script does not contain relative file paths. Running it will require
  # adjusting file paths to point to appropriate inputs. 
################################################################################

#' Create Hex Grid for Alaska Conservation Fund AIS Study Area
#'
#' This function creates a hexagonal grid for the Alaska Conservation Fund AIS study area and performs spatial operations to refine the grid.
#'
#' @param cell_size Numeric. The cell size for hexagons in meters (default is 26864.24, which creates hexagons of area ~625 km^2).
#' @param hex_shapefile_output_path Character. File path to save the output shapefile of hexagons (default is './Data_Processed/HexGrid_625km2_KEK.shp').
#' @param hexbounds_path Character. File path to the shapefile defining the study area boundaries for AIS data collection.
#' @param ak_can_rus_path Character. File path to the shapefile representing Alaska, Canada, and Russia boundaries used to create the ocean layer.
#'
#' @return Saves the resulting hex grid shapefile to the specified output path.
#' @import sf
#' @import dplyr
#' @import tidyr
#' @examples
#' create_hex_grid(
#'   cell_size = 26864.24,
#'   hex_shapefile_output_path = './Data_Processed/HexGrid_625km2_KEK.shp',
#'   hexbounds_path = './Data_Raw/ais_reshape/ais_reshape.shp',
#'   ak_can_rus_path = './Data_RAW/AK_CAN_RUS/AK_CAN_RUS.shp'
#' )
create_hex_grid <- function(cell_size = 26864.24,
                            hex_shapefile_output_path = './Data_Processed/HexGrid_625km2_KEK.shp',
                            hexbounds_path,
                            ak_can_rus_path) {
  # Import libraries
  library(sf)
  library(dplyr)
  library(tidyr)
  
  # Create a bounding box
  AOIbbox <- st_polygon(list(matrix(c(-2550000, 200000, 550000, 200000, 550000, 2720000, -2550000, 2720000, -2550000, 200000), ncol = 2, byrow = TRUE)))
  
  # Create hexagonal grid
  hexes <- st_make_grid(AOIbbox, cellsize = cell_size, square = FALSE, flat_topped = TRUE)
  
  hexesSF <- st_as_sf(hexes) %>%
    st_set_crs(3338) %>%
    mutate(hexID = 1:length(hexes))
  
  # Read in the boundaries of the study area used to collect AIS data
  hexbounds <- st_read(hexbounds_path)
  
  # Spatial intersection of AIS data collection boundaries and hex grid
  hexgrid <- st_intersection(hexesSF, hexbounds$geometry)
  
  # # Save output
  # st_write(hexesSF, './hex_test/Hexes_625km2.shp')
  
  # Read in AK_CAN_RUS ocean boundaries
  AK_sea <- st_read(ak_can_rus_path) %>%
    st_crop(hexbounds) %>%
    st_simplify(dTolerance = 50)
  
  t <- st_intersection(AK_sea, hexbounds)
  AK_sea_new <- st_difference(hexbounds, st_union(t))
  
  # Identify hexagons intersecting the ocean
  test <- st_intersects(hexesSF, AK_sea_new, sparse = FALSE)
  saveRDS(test, 'tempHex_intersects.rds')
  
  hexv2 <- hexesSF %>% rename(geometry = x)
  hexv2$inocean <- test
  
  # Drop hexagons that do not touch the ocean baselayer
  hexv3 <- hexv2 %>%
    filter(inocean == TRUE)
  
  # Merge results with original AOI and drop hexagons that do not touch the AOI
  hexv4 <- st_join(hexv3, hexbounds) %>%
    filter(!is.na(VISIBLE))
  
  # Clean out attributes and re-populate hex ID sequentially
  hexRevised <- hexv4 %>%
    select(hexID) %>%
    mutate(hexID = 1:nrow(hexv4))
  
  # Save output
  write_sf(hexRevised, hex_shapefile_output_path)
  
  # Save revised hexes
  st_write(hexRevised, hex_shapefile_output_path)
}

