#!/bin/bash

# Script to run the Pig jobs in sequence
# Usage: ./run_pig_jobs.sh
# Permission to make it executable as shell script:
# chmod +x run_pig_jobs.sh

# Delete data from HDFS: 
hadoop fs -rm -r -f /crime_data_analysis

# Set permission to write to logs
LOG_DIR="$(pwd)/logs"
mkdir -p $LOG_DIR

# Create HDFS directories if they don't exist
hadoop fs -mkdir -p /crime_data_analysis/raw
hadoop fs -mkdir -p /crime_data_analysis/output
hadoop fs -mkdir -p /crime_data_analysis/processed
hadoop fs -mkdir -p /crime_data_analysis/analyzed

# Copy the data file to HDFS if not already there
echo "Copying data to HDFS if needed..."
hadoop fs -test -e /crime_data_analysis/raw/crime_data.csv
if [ $? -ne 0 ]; then
    echo "Data file not found in HDFS, copying from local..."
    hadoop fs -copyFromLocal /Users/ingale.r/Downloads/Crime_Data_from_2020_to_Present.csv /crime_data_analysis/raw/crime_data.csv
fi

# Run the load job
echo "Running data load job..."
pig -x mapreduce -f $(pwd)/pig-scripts/load/load_crime_data.pig > $LOG_DIR/load_$(date +%Y%m%d_%H%M%S).log 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: Data load job failed!"
    exit 1
fi

# Run the cleanup job
echo "Running data cleanup job..."
pig -x mapreduce -f $(pwd)/pig-scripts/utils/cleanup.pig > $LOG_DIR/cleanup_$(date +%Y%m%d_%H%M%S).log 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: Data cleanup job failed!"
    exit 1
fi

# Run the transform job
# echo "Running data transform job..."
# pig -x mapreduce -f pig-scripts/transform/group_by_area.pig > $LOG_DIR/transform_$(date +%Y%m%d_%H%M%S).log 2>&1
# if [ $? -ne 0 ]; then
#     echo "ERROR: Data transform job failed!"
#     exit 1
# fi