# spatial-crime-analysis-LA
End-to-end Big Data pipeline for crime analysis in LA using Hadoop ecosystem tools: Hive, Pig Latin, and MapReduce.

## Crime Data Analysis with Apache Pig and Hadoop

This repository contains scripts and tools for analyzing the Los Angeles Crime Incident dataset using Apache Pig and Hadoop MapReduce.

## Project Overview

This project aims to analyze Los Angeles Crime Incident data from 2020 to present using Big Data technologies. The analysis will uncover patterns and insights related to temporal data, geographic coordinates, crime classifications, and victim demographics that could potentially assist law enforcement agencies.

### Key Analysis Areas:

1. **Time-based crime distribution**: Identifying high-crime periods (hour of day, day of week, month) to reveal when criminal activity peaks across different regions.
2. **Geographic crime density**: Processing latitude and longitude coordinates to identify crime hotspots across the city.
3. **Crime type correlation**: Analyzing relationships between crime types and location types to reveal which locations are most vulnerable to specific criminal activities.
4. **Case resolution efficiency**: Calculating average time-to-resolution metrics by crime type and area to highlight efficiency of response systems.

## Dataset Structure

The dataset contains numerous fields including but not limited to:
- Unique identifiers (DR_NO)
- Dates and times (Date Rptd, DATE OCC, TIME OCC)
- Location information (AREA, AREA NAME, LAT, LON)
- Crime classification (Crm Cd, Crm Cd Desc)
- Victim demographics (Vict Age, Vict Sex, Vict Descent)
- Case status information (Status, Status Desc)

## Prerequisites

- Apache Hadoop 3.4.1+
- Apache Pig 0.17.0+ (https://pig.apache.org/releases.html)
- Java 8+
- HDFS running on localhost:9000

## Hadoop Setup

1. **Format the HDFS NameNode**:
   ```bash
   cd /usr/local/bin/hadoop-tmp/hadoop-3.4.1/bin
   ./hadoop namenode -format
   ```

2. **Start Hadoop services**:
   ```bash
   cd /usr/local/bin/hadoop-tmp/hadoop-3.4.1/sbin
   ./start-all.sh
   ```

3. **Start Job History Server**:
   ```bash
   cd /usr/local/bin/hadoop-tmp/hadoop-3.4.1/bin
   ./mapred --daemon start historyserver
   ```

4. **Verify HDFS is accessible**:
   ```bash
   ./hadoop fs -ls /
   ```

5. **Create required HDFS directories**:
   ```bash
   ./hadoop fs -mkdir -p /user
   ./hadoop fs -mkdir -p /rawdata/crime_data
   ```

6. **Check Hadoop status**:
   ```bash
   ./hdfs dfsadmin -report
   ```

## Important Hadoop URLs

- **NameNode UI**: http://localhost:9870
- **Resource Manager**: http://localhost:8088/cluster
- **Job History**: http://localhost:19888/jobhistory

## Pig Setup and Usage

**Download pig from : https://pig.apache.org/releases.html**

1. **Start Pig in MapReduce mode**:
   ```bash
   pig -x mapreduce
   ```

2. **Use HDFS commands within Pig**:
   ```
   grunt> fs -ls /                              # List files in HDFS
   grunt> fs -mkdir /rawdata/crime_data        # Create directory
   grunt> fs -copyFromLocal file.csv /rawdata/crime_data/  # Copy local file to HDFS
   ```

## Data Loading

To load the crime dataset into HDFS:

```bash
hadoop fs -copyFromLocal /path/to/Crime_Data_from_2020_to_Present.csv /rawdata/crime_data/
```

## Running MapReduce Jobs

To deploy a MapReduce job:

```bash
./hadoop jar YourAnalysis.jar /rawdata/crime_data/input /output
```

Check job status in the Resource Manager UI: http://localhost:8088/cluster
View completed jobs in the Job History UI: http://localhost:19888/jobhistory

## Running Pig Scripts

From within the Pig grunt shell:
```
grunt> run /path/to/analysis_script.pig
```

## Example Pig Latin Scripts

### Basic Crime Data Loading and Filtering

```pig
-- Load the crime data
raw_crimes = LOAD '/rawdata/crime_data/Crime_Data_from_2020_to_Present.csv' 
             USING PigStorage(',') 
             AS (DR_NO:long, DateRptd:chararray, DateOcc:chararray, TimeOcc:int, 
                 Area:int, AreaName:chararray, RptDistNo:int, Part12:int, 
                 CrmCd:int, CrmCdDesc:chararray, Mocodes:chararray, VictAge:int, 
                 VictSex:chararray, VictDescent:chararray, PremisCd:int, 
                 PremisDesc:chararray, WeaponUsedCd:int, WeaponDesc:chararray,
                 Status:chararray, StatusDesc:chararray, CrmCd1:int, CrmCd2:int,
                 CrmCd3:int, CrmCd4:int, Location:chararray, CrossStreet:chararray,
                 LAT:double, LON:double);

-- Filter for specific crime types
theft_crimes = FILTER raw_crimes BY CrmCdDesc MATCHES '.*THEFT.*';

-- Group by area and count
area_counts = GROUP theft_crimes BY AreaName;
theft_by_area = FOREACH area_counts GENERATE 
                group AS area, 
                COUNT(theft_crimes) AS crime_count;

-- Order by count descending
ordered_results = ORDER theft_by_area BY crime_count DESC;

-- Store results
STORE ordered_results INTO '/output/theft_by_area' USING PigStorage(',');
```

## Project Structure

- `scripts/` - Contains Pig Latin scripts for data processing
- `mapreduce/` - Contains MapReduce Java code
- `data/` - Sample data files (not stored in git, use the loading instructions above)
- `results/` - Output from analysis
- `docs/` - Additional documentation

## Troubleshooting

- **HDFS Access Issues**: Ensure Hadoop is running with `jps` command
- **Pig Script Errors**: Check the log file mentioned in error messages
- **Data Loading Problems**: Verify CSV format and schema definitions
- **MapReduce Job Failures**: Check logs in the Resource Manager and Job History UI

## Contributors

- Raveena Ingale
