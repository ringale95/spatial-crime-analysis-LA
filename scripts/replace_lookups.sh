#!/bin/bash
# Script to replace original lookup files with cleaned versions

echo "Replacing lookup files with cleaned versions..."

# Create backup directory
hadoop fs -mkdir -p /crime_data_analysis/output/lookups/backup

# Crime Code Lookup
hadoop fs -mv /crime_data_analysis/output/lookups/crime_code_lookup /crime_data_analysis/output/lookups/backup/crime_code_lookup_orig
hadoop fs -mv /crime_data_analysis/output/lookups/clean_crime_code_lookup /crime_data_analysis/output/lookups/crime_code_lookup

# Area Code Lookup
hadoop fs -mv /crime_data_analysis/output/lookups/area_code_lookup /crime_data_analysis/output/lookups/backup/area_code_lookup_orig
hadoop fs -mv /crime_data_analysis/output/lookups/clean_area_code_lookup /crime_data_analysis/output/lookups/area_code_lookup

# Premise Code Lookup
hadoop fs -mv /crime_data_analysis/output/lookups/premise_code_lookup /crime_data_analysis/output/lookups/backup/premise_code_lookup_orig
hadoop fs -mv /crime_data_analysis/output/lookups/clean_premise_code_lookup /crime_data_analysis/output/lookups/premise_code_lookup

# Weapon Code Lookup
hadoop fs -mv /crime_data_analysis/output/lookups/weapon_code_lookup /crime_data_analysis/output/lookups/backup/weapon_code_lookup_orig
hadoop fs -mv /crime_data_analysis/output/lookups/clean_weapon_code_lookup /crime_data_analysis/output/lookups/weapon_code_lookup

# Status Code Lookup
hadoop fs -mv /crime_data_analysis/output/lookups/status_code_lookup /crime_data_analysis/output/lookups/backup/status_code_lookup_orig
hadoop fs -mv /crime_data_analysis/output/lookups/clean_status_code_lookup /crime_data_analysis/output/lookups/status_code_lookup

echo "Replacement complete. Original files backed up to /crime_data_analysis/output/lookups/backup/"