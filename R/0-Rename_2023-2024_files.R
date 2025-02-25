
# Rename files for 2023-2024 AIS data so that it can be processed in monthly batches. 

library(data.table)

# Define the directory where your CSV files are stored
input_dir <- "/mnt/home/kapsarke/Documents/AIS/Data_Raw/2023-2024/orig_file_names"
output_dir <- "/mnt/home/kapsarke/Documents/AIS/Data_Raw/2023-2024/"

# Create output directory if it doesn't exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}

# Get a list of all CSV files in the directory
csv_files <- list.files(input_dir, pattern = "\\.csv$", full.names = TRUE)

# Function to process each CSV file
process_csv <- function(file_path) {
  # Read the CSV file
  df <- fread(file_path)
  
  # Ensure the timestamp column exists
  if (!"position_timestamp" %in% colnames(df)) {
    message("Skipping file (no timestamp column): ", file_path)
    return(NULL)
  }
  
  # Convert timestamp column to datetime
  df[, position_timestamp := as.POSIXct(position_timestamp, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")]
  
  # Extract Year-Month
  df[, year_month := format(position_timestamp, "%Y-%m")]
  
  # Get unique year-month combinations
  unique_months <- unique(df$year_month)
  
  # Process each month separately
  for (ym in unique_months) {
    subset_df <- df[year_month == ym, ]
    
    # Define output filename with correct format
    file_base <- paste0("Historical_AOI_", ym, ".csv")
    file_path_out <- file.path(output_dir, file_base)
    
    # Ensure unique filename if duplicates exist
    counter <- 1
    while (file.exists(file_path_out)) {
      file_path_out <- file.path(output_dir, paste0("Historical_AOI_", ym, "_", counter, ".csv"))
      counter <- counter + 1
    }
    
    # Save the new CSV file without the extra column
    fwrite(subset_df[, !"year_month"], file_path_out)
    
    message("Saved: ", file_path_out)
  }
}

# Process all CSV files
lapply(csv_files, process_csv)

message("Processing complete.")


################################################################################
# Check for number of signals per day 


# Define the directory where the new CSV files are saved
dir <- "/mnt/home/kapsarke/Documents/AIS/Data_Raw/2023-2024/"

# Get a list of all the processed CSV files
csv_files <- list.files(dir, pattern = "Historical_AOI", full.names = TRUE)

# Initialize an empty data frame
summary_df <- data.frame(Year = integer(), Month = integer(), Date = character(), Total_Rows = integer(), stringsAsFactors = FALSE)

# Loop through each file and extract information
for (file in csv_files) {
  # Read the CSV file
  df <- fread(file)
  print(file)
  
  # Ensure the timestamp column exists
  if (!"position_timestamp" %in% colnames(df)) {
    message("Skipping file (no timestamp column): ", file)
    next
  }
  
  # Convert position_timestamp to Date format (removing time)
  df[, Date := as.Date(position_timestamp, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")]
  
  # Aggregate counts by Date (ignoring time)
  date_summary <- df[, .(Total_Rows = .N), by = .(Date)]
  
  # Append to summary dataframe
  summary_df <- rbind(summary_df, date_summary, use.names = TRUE, fill = TRUE)
}

# Print or save the summary
print(summary_df)
# Optionally, save it as a CSV for future reference
write.csv(summary_df, file.path(output_dir, "file_summary_by_date.csv"), row.names = FALSE)
