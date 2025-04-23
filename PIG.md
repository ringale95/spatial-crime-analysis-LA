## PIG

## Pig Setup and Usage

### Steps to configure pig
1. **Download pig from : https://pig.apache.org/releases.html**
2. Setup bashrc or zshrc 

3. **Start Pig in MapReduce or local mode**:
   
   ```bash
   pig -x mapreduce

   Local mode:
   pig
   ```
   In the Pig Grunt shell, fs is a command that provides access to the underlying Hadoop Distributed File System (HDFS) operations.

4. **LOAD Data into pig engine**
   Load data from HDFS to pig temporary storage for processing. Pig storage is temporary location like cache which is used for processing period.

5. **Use HDFS commands within Pig**:
   
   ```
   grunt> fs -ls /                              # List files in HDFS
   grunt> fs -mkdir /rawdata/crime_data        # Create directory
   grunt> fs -copyFromLocal file.csv /rawdata/crime_data/  # Copy local file to HDFS
   ```
