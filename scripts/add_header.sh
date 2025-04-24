#!/bin/bash
# Create a header file with renamed columns
echo "division_record_number,date_reported,date_of_occurence,time_of_occurrence,AREA,reporting_district_number,Part_1_2,crime_code,Mocodes,Vict_Age,Vict_Sex,Vict_Descent,Premis_Cd,Weapon_Used_Cd,Status,LOCATION,Cross_Street,Latitude,Longitude" > header.csv

# Make sure the output directory exists
hdfs dfs -mkdir -p /crime_data_analysis/output/final_crimes_data/

# Get the data (excluding any header that might be present)
hdfs dfs -cat /crime_data_analysis/output/cleaned_crimes_data/part-* > data_only.csv

# Combine header with data
cat header.csv data_only.csv > combined_data.csv

# Upload the combined file back to HDFS
hdfs dfs -put -f combined_data.csv /crime_data_analysis/output/final_crimes_data/crimes_with_header.csv

# Clean up local files
rm header.csv data_only.csv combined_data.csv

echo "Processing complete. Final data is available at: /crime_data_analysis/output/final_crimes_data/crimes_with_header.csv"