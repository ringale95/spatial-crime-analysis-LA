
CREATE DATABASE IF NOT EXISTS crime_analysis
LOCATION '/hive/warehouse/crime_analysis.db';

USE crime_analysis;

DROP TABLE IF EXISTS crime_analysis.crimes_with_location;

CREATE EXTERNAL TABLE IF NOT EXISTS crime_analysis.crimes_with_location (
     dr_no STRING,
  date_rptd STRING,
  date_occ STRING,
  time_occ STRING,
  area STRING,
  rpt_dist_no STRING,
  part_1_2 INT,
  crm_cd STRING,
  mocodes STRING,
  vict_age INT,
  vict_sex STRING,
  vict_descent STRING,
  premis_cd STRING,
  weapon_used_cd STRING,
  status STRING,
  location STRING,
  cross_street STRING,
  lat DOUBLE,
  lon DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/crime_data_analysis/output/final_crimes_data/'
TBLPROPERTIES ('skip.header.line.count'='1');


SELECT * FROM crimes_with_location LIMIT 10;


CREATE TABLE IF NOT EXISTS crime_density_by_grid AS
SELECT 
  ROUND(lat, 2) AS lat_grid,
  ROUND(lon, 2) AS lon_grid,
  COUNT(*) AS crime_count
FROM crimes_with_location
WHERE lat IS NOT NULL AND lon IS NOT NULL
GROUP BY ROUND(lat, 2), ROUND(lon, 2);


--  top 10 crime hotspots
CREATE OR REPLACE VIEW top_crime_hotspots AS
SELECT 
  lat_grid,
  lon_grid,
  crime_count
FROM crime_density_by_grid
ORDER BY crime_count DESC
LIMIT 10;

SELECT * FROM top_crime_hotspots;


