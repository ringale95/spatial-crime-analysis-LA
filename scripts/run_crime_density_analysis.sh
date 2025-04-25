#!/bin/bash

export HIVE_HOME="/usr/local/hive"
export HADOOP_HOME="/usr/local/bin/hadoop-tmp/hadoop-3.4.1"
export PATH=$PATH:$HIVE_HOME/bin:$HADOOP_HOME/bin

echo "Cleaning up Derby metastore DB..."
rm -rf metastore_db derby.log
hdfs dfs -rm -r -f /user/hive/warehouse/test_db.db

# Clean stale Derby lock files
LOCK_DIR="/Users/ingale.r/code/spatial-crime-analysis-LA/metastore_db"
rm -f "$LOCK_DIR/db.lck" "$LOCK_DIR/dbex.lck"

echo "Clean slate."

# === Step 1: Initialize Hive Metastore Schema ===
echo "Checking and initializing Hive schema (Derby)..."
$HIVE_HOME/bin/schematool -dbType derby -initSchema > init_schema.log 2>&1
if grep -q "Schema already exists" init_schema.log; then
  echo "ℹ️  Schema already initialized. Skipping."
else
  echo "Hive metastore schema initialized."
fi

# === Step 2: Run Hive Test Query via Beeline ===
echo "Running Hive test query via Beeline..."
$HIVE_HOME/bin/beeline -u jdbc:hive2:// -e "
CREATE DATABASE IF NOT EXISTS test_db;
USE test_db;

DROP TABLE IF EXISTS test_table PURGE;
CREATE TABLE test_table (id INT, name STRING);
INSERT INTO test_table VALUES (1, 'Newyork'), (2, 'Hawai');
SELECT * FROM test_table;
DESCRIBE FORMATTED test_table;
"

echo "Hive test completed successfully!"
