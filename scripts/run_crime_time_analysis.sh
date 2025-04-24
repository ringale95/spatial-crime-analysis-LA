#!/bin/bash

# Set variables and before make sure you have a crime-app.jar file available by running mvn clean package
JAR_PATH="/Users/ingale.r/code/spatial-crime-analysis-LA/target/crime-app.jar"
MAIN_CLASS="edu.neu.crimeanalysis.CrimeTimeDriver"
INPUT_PATH="/crime_data_analysis/output/final_crimes_data/crimes_with_header.csv"
OUTPUT_PATH="/crime_data_analysis/analyzed/crime_time_analysis"

# Optional: Clean previous output
hdfs dfs -rm -r -f $OUTPUT_PATH

# Run the Hadoop MapReduce job
hadoop jar "$JAR_PATH" "$MAIN_CLASS" "$INPUT_PATH" "$OUTPUT_PATH"

# Check if job ran successfully
if [ $? -eq 0 ]; then
  echo "Crime time analysis completed successfully!"
  echo "Output available at: $OUTPUT_PATH"
else
  echo "Job failed! Check your logs."
fi
