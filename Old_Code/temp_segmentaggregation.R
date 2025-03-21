# Load required libraries
library(sf)
library(dplyr)

# Create extended example data frame with more locations
ship_data <- data.frame(
  scramblemmsi = c(111, 111, 111, 111, 111, 111, 222, 222, 222, 222, 222, 222),
  timestamp = as.POSIXct(c('2023-10-10 12:00', '2023-10-10 12:05', '2023-10-10 12:10', 
                           '2023-10-10 12:15', '2023-10-10 12:20', '2023-10-10 12:25',
                           '2023-10-10 12:00', '2023-10-10 12:05', '2023-10-10 12:10', 
                           '2023-10-10 12:15', '2023-10-10 12:20', '2023-10-10 12:25')),
  status_change = c(FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
  lon = c(10, 11, 12, 13, 14, 15, 20, 21, 22, 23, 24, 25),
  lat = c(50, 51, 52, 53, 54, 55, 60, 61, 62, 63, 64, 65)
)

# Convert the data to an sf object
# ship_data <- st_as_sf(ship_data, coords = c("lon", "lat"), crs = 4326)

# Sort by scramblemmsi and timestamp
ship_data <- ship_data %>%
  arrange(scramblemmsi, timestamp)

# Create a group ID for each set of consecutive points based on scramblemmsi and status_change
ship_data <- ship_data %>%
  group_by(scramblemmsi) %>%
  mutate(segment_id = cumsum(lag(status_change, default = TRUE))) %>%
  ungroup()

# Duplicate the last point in each segment if the next segment_id exists
ship_data_expanded <- ship_data %>%
  group_by(scramblemmsi) %>%
  do({
    segment <- .
    expanded_segment <- segment
    
    # Loop through each segment and check if the next segment_id exists
    if(length(unique(segment$segment_id)) > 1){
      for (i in 2:length(unique(segment$segment_id))) {
        new_segment_id <- i - 1
        
        # Check if the next segment_id exists
        if (new_segment_id %in% segment$segment_id) {
          # Duplicate the last point and modify its segment_id
          first_point <- segment[segment$segment_id == i,] %>% filter(row_number() == 1)
          
          # Create a new row with the duplicated point and new segment_id
          expanded_segment <- rbind(expanded_segment, data.frame(
            scramblemmsi = first_point$scramblemmsi,
            timestamp = first_point$timestamp,  # Copy the timestamp from the last point
            status_change = FALSE,  # Set status_change as FALSE for the duplicated point
            lon = first_point$lon,
            lat = first_point$lat,
            segment_id = new_segment_id
          ))
        }
      }
    }
    expanded_segment
  }) %>%
  ungroup()

# Now convert to LINESTRING geometries
line_segments <- ship_data_expanded %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326) %>% 
  group_by(scramblemmsi, segment_id) %>%
  arrange(timestamp) %>% 
  summarise(geometry = st_combine(geometry), .groups = 'drop') %>%
  mutate(geometry = st_cast(geometry, "LINESTRING")) %>%
  ungroup()

# Create the final sf object
# line_segments_sf <- st_as_sf(line_segments)

# View the line segments
# print(line_segments_sf)

# Optional: Plot the result
plot(st_geometry(line_segments), col = c("red", "blue"), main = "Ship Line Segments")

