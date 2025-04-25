USE crime_analysis;

DROP TABLE IF EXISTS crime_code_lookup;
DROP TABLE IF EXISTS area_code_lookup;

CREATE EXTERNAL TABLE IF NOT EXISTS crime_code_lookup (
  Crm_Cd STRING,
  Crm_Cd_Desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/crime_data_analysis/output/lookups/backup/crime_code_lookup_orig/crime_code_lookup/';

CREATE EXTERNAL TABLE IF NOT EXISTS area_code_lookup (
  AREA STRING,
  AREA_NAME STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/crime_data_analysis/output/lookups/backup/area_code_lookup_orig/area_code_lookup/';


DROP TABLE IF EXISTS resolution_efficiency_by_crime_and_area;

-- Step 3: Create new analysis table with joins to lookup values
CREATE TABLE resolution_efficiency_by_crime_and_area AS
SELECT
  TRIM(a.AREA_NAME) AS area_name,
  TRIM(cr.Crm_Cd_Desc) AS crime_description,
  ROUND(AVG(
    DATEDIFF(
      from_unixtime(unix_timestamp(c.date_rptd, 'MM/dd/yyyy')),
      from_unixtime(unix_timestamp(c.date_occ, 'MM/dd/yyyy'))
    )
  ), 2) AS avg_days_to_resolution
FROM crimes_cleaned_resolution_ready c
LEFT JOIN area_code_lookup a 
  ON TRIM(c.area) = TRIM(a.AREA)
LEFT JOIN crime_code_lookup cr 
  ON TRIM(c.crm_cd) = TRIM(cr.Crm_Cd)
WHERE 
  c.date_rptd IS NOT NULL AND 
  c.date_occ IS NOT NULL AND 
  c.date_rptd != '' AND 
  c.date_occ != ''
GROUP BY 
  TRIM(a.AREA_NAME), 
  TRIM(cr.Crm_Cd_Desc);

-- Step 4: Preview top 10 crimes that take the longest to resolve
SELECT * 
FROM resolution_efficiency_by_crime_and_area
ORDER BY avg_days_to_resolution DESC
LIMIT 10;
