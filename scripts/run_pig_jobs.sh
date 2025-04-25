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
pig -x mapreduce -f /Users/ingale.r/code/spatial-crime-analysis-LA/pig-scripts/load/load_crime_data.pig > $LOG_DIR/load_$(date +%Y%m%d_%H%M%S).log 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: Data load job failed!"
    exit 1
fi
# Run the cleanup job
echo "Running data cleanup job..."
pig -x mapreduce -f /Users/ingale.r/code/spatial-crime-analysis-LA/pig-scripts/utils/cleanup.pig > $LOG_DIR/cleanup_$(date +%Y%m%d_%H%M%S).log 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: Data cleanup job failed!"
    exit 1
fi

# Run the lookup cleanup job
echo "Running lookup cleanup job..."
pig -x mapreduce -f /Users/ingale.r/code/spatial-crime-analysis-LA/pig-scripts/utils/clean_lookups.pig > $LOG_DIR/clean_lookups_$(date +%Y%m%d_%H%M%S).log 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: Lookup cleanup job failed!"
    exit 1
fi

# Replace original lookups with cleaned versions
echo "Replacing lookup files with cleaned versions..."
./scripts/replace_lookups.sh > $LOG_DIR/replace_lookups_$(date +%Y%m%d_%H%M%S).log 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: Replacing lookup files failed!"
    exit 1
fi